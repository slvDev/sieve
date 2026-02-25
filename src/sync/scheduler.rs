//! Peer-driven scheduler for historical sync.
//!
//! Manages block ranges to fetch, assigns work to peers, and uses
//! AIMD batch sizing (additive increase, multiplicative decrease).

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::sync::{FetchBatch, FetchMode};
use reth_network_api::PeerId;
use tokio::sync::Mutex;

/// Default number of attempts before a block is promoted to escalation queue.
const DEFAULT_ESCALATION_THRESHOLD: u32 = 5;

/// Initial backoff duration after a peer failure.
const BACKOFF_INITIAL: Duration = Duration::from_millis(500);

/// Maximum backoff duration for a peer.
const BACKOFF_MAX: Duration = Duration::from_secs(128);

// ── EscalationState ──────────────────────────────────────────────────

/// Shard-aware escalation state for priority retry of difficult blocks.
///
/// Blocks that fail N attempts in the normal queue are promoted here and get
/// priority handling (checked before normal queue). The shard grouping enables
/// prioritizing blocks from nearly-complete shards.
#[derive(Debug, Default)]
struct EscalationState {
    /// Blocks awaiting retry, grouped by shard for prioritization.
    shards: HashMap<u64, HashSet<u64>>,
    /// Total count for quick access.
    total_count: usize,
    /// Track which peers tried each block and when (for cooldown).
    peer_attempts: HashMap<u64, HashMap<PeerId, Instant>>,
}

impl EscalationState {
    fn add_block(&mut self, block: u64, shard_size: u64) {
        let shard_start = (block / shard_size) * shard_size;
        if self.shards.entry(shard_start).or_default().insert(block) {
            self.total_count += 1;
        }
    }

    fn remove_block(&mut self, block: u64, shard_size: u64) {
        let shard_start = (block / shard_size) * shard_size;
        if let Some(shard_blocks) = self.shards.get_mut(&shard_start) {
            if shard_blocks.remove(&block) {
                self.total_count = self.total_count.saturating_sub(1);
                if shard_blocks.is_empty() {
                    self.shards.remove(&shard_start);
                }
            }
        }
        self.peer_attempts.remove(&block);
    }

    const fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    const fn len(&self) -> usize {
        self.total_count
    }
}

// ── SchedulerConfig ──────────────────────────────────────────────────

/// Scheduler configuration.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Hard cap for blocks assigned in a single peer batch.
    pub blocks_per_assignment: usize,
    /// Initial blocks per peer batch (before AIMD adjusts).
    pub initial_blocks_per_assignment: usize,
    /// Number of attempts before a block is promoted to escalation queue.
    pub max_attempts_per_block: u32,
    /// Shard size for shard-aware escalation prioritization.
    pub shard_size: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            blocks_per_assignment: 32,
            initial_blocks_per_assignment: 32,
            max_attempts_per_block: DEFAULT_ESCALATION_THRESHOLD,
            shard_size: 10_000,
        }
    }
}

// ── PeerHealthConfig ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PeerHealthConfig {
    aimd_increase_after: u32,
    aimd_partial_decrease: f64,
    aimd_failure_decrease: f64,
    aimd_min_batch: usize,
    aimd_initial_batch: usize,
    aimd_max_batch: usize,
    quality_partial_weight: f64,
}

impl PeerHealthConfig {
    #[must_use]
    pub fn from_scheduler_config(config: &SchedulerConfig) -> Self {
        let max_batch = config.blocks_per_assignment.max(1);
        let initial_batch = config.initial_blocks_per_assignment.max(1).min(max_batch);
        Self {
            aimd_increase_after: 5,
            aimd_partial_decrease: 0.7,
            aimd_failure_decrease: 0.5,
            aimd_min_batch: 1,
            aimd_initial_batch: initial_batch,
            aimd_max_batch: max_batch,
            quality_partial_weight: 1.0,
        }
    }
}

// ── PeerHealth ───────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct PeerHealth {
    backoff_until: Option<Instant>,
    backoff_duration: Duration,
    /// Separate cooldown for stale-head peers. Not cleared by `record_success`,
    /// unlike `backoff_until` which resets on any successful fetch. This prevents
    /// a stale peer from being prematurely un-cooled by an unrelated success.
    stale_head_until: Option<Instant>,
    successes: u64,
    failures: u64,
    partials: u64,
    assignments: u64,
    assigned_blocks: u64,
    inflight_blocks: u64,
    batch_limit: usize,
    batch_limit_max: usize,
    success_streak: u32,
    last_error: Option<String>,
    last_error_count: u64,
}

impl PeerHealth {
    fn is_cooling_down(&self) -> bool {
        let now = Instant::now();
        self.backoff_until.is_some_and(|until| now < until)
            || self.stale_head_until.is_some_and(|until| now < until)
    }
}

// ── PeerQuality ──────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct PeerQuality {
    pub score: f64,
    pub samples: u64,
}

// ── PeerHealthTracker ────────────────────────────────────────────────

#[derive(Debug)]
pub struct PeerHealthTracker {
    config: PeerHealthConfig,
    health: Mutex<HashMap<PeerId, PeerHealth>>,
}

impl PeerHealthTracker {
    pub(crate) fn new(config: PeerHealthConfig) -> Self {
        Self {
            config,
            health: Mutex::new(HashMap::new()),
        }
    }

    fn ensure_batch_limit(&self, entry: &mut PeerHealth) {
        if entry.batch_limit == 0 {
            entry.batch_limit = self.config.aimd_initial_batch.max(1);
        }
        if entry.batch_limit_max == 0 || entry.batch_limit > entry.batch_limit_max {
            entry.batch_limit_max = entry.batch_limit;
        }
    }

    fn clamp_batch_limit(&self, value: usize) -> usize {
        value
            .max(self.config.aimd_min_batch)
            .min(self.config.aimd_max_batch.max(1))
    }

    pub(crate) async fn record_success(&self, peer_id: PeerId) {
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        self.ensure_batch_limit(entry);
        entry.successes = entry.successes.saturating_add(1);
        entry.backoff_until = None;
        entry.backoff_duration = Duration::ZERO;
        entry.success_streak = entry.success_streak.saturating_add(1);
        if entry.success_streak >= self.config.aimd_increase_after {
            entry.batch_limit = self.clamp_batch_limit(entry.batch_limit.saturating_add(1));
            entry.success_streak = 0;
        }
        entry.batch_limit_max = entry.batch_limit_max.max(entry.batch_limit);
    }

    pub(crate) async fn record_partial(&self, peer_id: PeerId) {
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        self.ensure_batch_limit(entry);
        entry.partials = entry.partials.saturating_add(1);
        entry.success_streak = 0;
        let reduced =
            (entry.batch_limit as f64 * self.config.aimd_partial_decrease).floor() as usize;
        entry.batch_limit = self.clamp_batch_limit(reduced);
    }

    pub(crate) async fn record_failure(&self, peer_id: PeerId) {
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        self.ensure_batch_limit(entry);
        entry.failures = entry.failures.saturating_add(1);
        entry.success_streak = 0;
        let reduced =
            (entry.batch_limit as f64 * self.config.aimd_failure_decrease).floor() as usize;
        entry.batch_limit = self.clamp_batch_limit(reduced);
        let next_backoff = if entry.backoff_duration.is_zero() {
            BACKOFF_INITIAL
        } else {
            (entry.backoff_duration * 2).min(BACKOFF_MAX)
        };
        entry.backoff_duration = next_backoff;
        entry.backoff_until = Some(Instant::now() + next_backoff);
        tracing::debug!(
            peer_id = ?peer_id,
            backoff_ms = next_backoff.as_millis() as u64,
            failures = entry.failures,
            "peer backing off after failure"
        );
    }

    /// Set a stale-head cooldown on a peer.
    ///
    /// Unlike failure backoff (`backoff_until`), this is NOT cleared by
    /// `record_success`. The peer must wait for the full cooldown even if
    /// it succeeds on other work.
    pub(crate) async fn set_stale_head_cooldown(&self, peer_id: PeerId, duration: Duration) {
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        self.ensure_batch_limit(entry);
        entry.stale_head_until = Some(Instant::now() + duration);
    }

    pub(crate) async fn is_peer_cooling_down(&self, peer_id: PeerId) -> bool {
        let health = self.health.lock().await;
        health
            .get(&peer_id)
            .is_some_and(PeerHealth::is_cooling_down)
    }

    pub(crate) async fn note_error(&self, peer_id: PeerId, error: String) {
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        if entry.last_error.as_deref() == Some(error.as_str()) {
            entry.last_error_count = entry.last_error_count.saturating_add(1);
        } else {
            entry.last_error = Some(error);
            entry.last_error_count = 1;
        }
    }

    pub(crate) async fn record_assignment(&self, peer_id: PeerId, blocks: usize) {
        if blocks == 0 {
            return;
        }
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        self.ensure_batch_limit(entry);
        entry.assignments = entry.assignments.saturating_add(1);
        entry.assigned_blocks = entry.assigned_blocks.saturating_add(blocks as u64);
        let clamped = self.clamp_batch_limit(entry.batch_limit);
        entry.batch_limit = clamped;
        entry.batch_limit_max = entry.batch_limit_max.max(clamped);
        entry.inflight_blocks = entry.inflight_blocks.saturating_add(blocks as u64);
    }

    pub(crate) async fn finish_assignment(&self, peer_id: PeerId, blocks: usize) {
        if blocks == 0 {
            return;
        }
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        self.ensure_batch_limit(entry);
        entry.inflight_blocks = entry.inflight_blocks.saturating_sub(blocks as u64);
    }

    pub(crate) async fn batch_limit(&self, peer_id: PeerId) -> usize {
        let mut health = self.health.lock().await;
        let entry = health.entry(peer_id).or_default();
        self.ensure_batch_limit(entry);
        self.clamp_batch_limit(entry.batch_limit)
    }

    pub(crate) async fn quality(&self, peer_id: PeerId) -> PeerQuality {
        let health = self.health.lock().await;
        let Some(entry) = health.get(&peer_id) else {
            return PeerQuality {
                score: 1.0,
                samples: 0,
            };
        };
        let total = entry.successes + entry.failures + entry.partials;
        if total == 0 {
            return PeerQuality {
                score: 1.0,
                samples: 0,
            };
        }
        let weighted_success = (entry.partials as f64).mul_add(
            self.config.quality_partial_weight,
            entry.successes as f64,
        );
        PeerQuality {
            score: (weighted_success / total as f64).clamp(0.0, 1.0),
            samples: total,
        }
    }

    pub(crate) async fn peer_count(&self) -> usize {
        self.health.lock().await.len()
    }
}

// ── BlockPeerBackoff ─────────────────────────────────────────────────

/// Per-peer-per-block backoff to prevent a single peer from spinning on a block
/// it cannot serve. Tracks cooldown per (block, peer_id) pair.
#[derive(Debug, Default)]
struct BlockPeerBackoff {
    entries: HashMap<(u64, PeerId), BlockPeerCooldown>,
}

#[derive(Debug)]
struct BlockPeerCooldown {
    fail_count: u32,
    cooldown_until: Instant,
}

impl BlockPeerBackoff {
    fn record_failure(&mut self, block: u64, peer_id: PeerId) {
        let key = (block, peer_id);
        let entry = self.entries.entry(key).or_insert_with(|| BlockPeerCooldown {
            fail_count: 0,
            cooldown_until: Instant::now(),
        });
        entry.fail_count = entry.fail_count.saturating_add(1);
        let backoff = if entry.fail_count <= 1 {
            BACKOFF_INITIAL
        } else {
            let factor = 2u32.saturating_pow(entry.fail_count.saturating_sub(1));
            (BACKOFF_INITIAL * factor).min(BACKOFF_MAX)
        };
        entry.cooldown_until = Instant::now() + backoff;
    }

    fn is_cooling_down(&self, block: u64, peer_id: PeerId) -> bool {
        self.entries
            .get(&(block, peer_id))
            .is_some_and(|e| Instant::now() < e.cooldown_until)
    }

    fn remove_block(&mut self, block: u64) {
        self.entries.retain(|&(b, _), _| b != block);
    }
}

// ── PeerWorkScheduler ────────────────────────────────────────────────

/// Peer-driven scheduler state.
///
/// # Lock ordering (acquire in this order to prevent deadlocks):
///   pending -> queued -> attempts -> escalation -> completed -> in_flight -> block_peer_backoff
#[derive(Debug)]
pub struct PeerWorkScheduler {
    config: SchedulerConfig,
    pending: Mutex<BinaryHeap<Reverse<u64>>>,
    queued: Mutex<HashSet<u64>>,
    attempts: Mutex<HashMap<u64, u32>>,
    peer_health: Arc<PeerHealthTracker>,
    escalation: Mutex<EscalationState>,
    completed: Mutex<HashSet<u64>>,
    in_flight: Mutex<HashSet<u64>>,
    block_peer_backoff: Mutex<BlockPeerBackoff>,
}

impl PeerWorkScheduler {
    /// Create a scheduler with shared peer health tracking.
    pub fn new_with_health(
        config: SchedulerConfig,
        blocks: Vec<u64>,
        peer_health: Arc<PeerHealthTracker>,
    ) -> Self {
        let queued: HashSet<u64> = blocks.iter().copied().collect();
        let pending = blocks.into_iter().map(Reverse).collect::<BinaryHeap<_>>();
        Self {
            config,
            pending: Mutex::new(pending),
            queued: Mutex::new(queued),
            attempts: Mutex::new(HashMap::new()),
            peer_health,
            escalation: Mutex::new(EscalationState::default()),
            completed: Mutex::new(HashSet::new()),
            in_flight: Mutex::new(HashSet::new()),
            block_peer_backoff: Mutex::new(BlockPeerBackoff::default()),
        }
    }

    /// Returns the next batch for a peer (escalation first, then normal).
    pub async fn next_batch_for_peer(&self, peer_id: PeerId, peer_head: u64) -> FetchBatch {
        if self.is_peer_cooling_down(peer_id).await {
            tracing::trace!(
                peer_id = ?peer_id,
                "scheduler: peer cooling down"
            );
            return FetchBatch {
                blocks: Vec::new(),
                mode: FetchMode::Normal,
            };
        }

        // ESCALATION FIRST (priority queue)
        if let Some(block) = self.pop_escalation_for_peer(peer_id).await {
            return FetchBatch {
                blocks: vec![block],
                mode: FetchMode::Escalation,
            };
        }

        // Normal queue second
        let max_blocks = self.peer_health.batch_limit(peer_id).await;
        let normal = self
            .pop_next_batch_for_head(peer_id, peer_head, max_blocks)
            .await;
        if !normal.is_empty() {
            return FetchBatch {
                blocks: normal,
                mode: FetchMode::Normal,
            };
        }

        FetchBatch {
            blocks: Vec::new(),
            mode: FetchMode::Normal,
        }
    }

    /// Append a contiguous range of blocks to the pending queue.
    pub async fn enqueue_range(&self, range: std::ops::RangeInclusive<u64>) -> usize {
        let start = *range.start();
        let end = *range.end();
        if end < start {
            return 0;
        }
        let mut pending = self.pending.lock().await;
        let mut queued = self.queued.lock().await;
        let mut added = 0usize;
        for block in start..=end {
            if queued.insert(block) {
                pending.push(Reverse(block));
                added += 1;
            }
        }
        added
    }

    async fn pop_next_batch_for_head(
        &self,
        peer_id: PeerId,
        peer_head: u64,
        max_blocks: usize,
    ) -> Vec<u64> {
        let mut pending = self.pending.lock().await;
        let mut queued = self.queued.lock().await;
        let mut in_flight = self.in_flight.lock().await;
        let backoff = self.block_peer_backoff.lock().await;

        let limit = max_blocks
            .max(1)
            .min(self.config.blocks_per_assignment.max(1));
        let mut batch = Vec::with_capacity(limit);
        let mut last: Option<u64> = None;

        while batch.len() < limit {
            let Some(Reverse(next)) = pending.peek().copied() else {
                break;
            };

            if next > peer_head {
                tracing::trace!(
                    peer_id = ?peer_id,
                    block = next,
                    peer_head,
                    "scheduler: pending block above head cap"
                );
                break;
            }
            if let Some(prev) = last {
                if next != prev.saturating_add(1) {
                    break;
                }
            }

            if backoff.is_cooling_down(next, peer_id) {
                tracing::trace!(
                    peer_id = ?peer_id,
                    block = next,
                    "scheduler: pending block on cooldown, stopping batch"
                );
                break;
            }

            pending.pop();
            queued.remove(&next);
            in_flight.insert(next);
            batch.push(next);
            last = Some(next);
        }

        batch
    }

    async fn pop_escalation_for_peer(&self, peer_id: PeerId) -> Option<u64> {
        let mut escalation = self.escalation.lock().await;
        let completed = self.completed.lock().await;
        let mut in_flight = self.in_flight.lock().await;
        let backoff = self.block_peer_backoff.lock().await;

        if escalation.is_empty() {
            return None;
        }

        // Get shard priorities sorted by "closest to complete" (fewest blocks = highest priority)
        let mut shard_priorities: Vec<_> = escalation
            .shards
            .iter()
            .map(|(shard_start, blocks)| (*shard_start, blocks.len()))
            .collect();
        shard_priorities.sort_by_key(|(_, missing)| *missing);

        for (shard_start, _) in shard_priorities {
            let Some(shard_blocks) = escalation.shards.get(&shard_start) else {
                continue;
            };

            let mut best_block: Option<u64> = None;
            for &block in shard_blocks {
                if completed.contains(&block) {
                    continue;
                }
                if backoff.is_cooling_down(block, peer_id) {
                    continue;
                }
                best_block = Some(block);
                break;
            }

            if let Some(block) = best_block {
                if let Some(shard_set) = escalation.shards.get_mut(&shard_start) {
                    shard_set.remove(&block);
                }
                escalation.total_count = escalation.total_count.saturating_sub(1);
                if escalation
                    .shards
                    .get(&shard_start)
                    .is_some_and(HashSet::is_empty)
                {
                    escalation.shards.remove(&shard_start);
                }

                escalation
                    .peer_attempts
                    .entry(block)
                    .or_default()
                    .insert(peer_id, Instant::now());

                in_flight.insert(block);
                return Some(block);
            }
        }

        if !escalation.is_empty() {
            tracing::debug!(
                peer_id = ?peer_id,
                escalation_total = escalation.total_count,
                "scheduler: all escalation blocks on cooldown for peer"
            );
        }
        None
    }

    /// Mark blocks as completed and remove from in-flight and escalation.
    pub async fn mark_completed(&self, blocks: &[u64]) -> u64 {
        let mut recovered = 0u64;
        let mut escalation = self.escalation.lock().await;
        let mut completed = self.completed.lock().await;
        let mut in_flight = self.in_flight.lock().await;
        let shard_size = self.config.shard_size;

        for block in blocks {
            in_flight.remove(block);
            completed.insert(*block);

            if escalation.peer_attempts.contains_key(block) {
                recovered = recovered.saturating_add(1);
            }

            escalation.remove_block(*block, shard_size);
        }

        // Clean up per-peer-per-block backoff entries
        drop(escalation);
        drop(completed);
        drop(in_flight);
        let mut backoff = self.block_peer_backoff.lock().await;
        for block in blocks {
            backoff.remove_block(*block);
        }

        recovered
    }

    /// Requeue failed blocks or promote to escalation after max attempts.
    /// Returns list of blocks newly promoted to escalation.
    pub async fn requeue_failed(&self, blocks: &[u64]) -> Vec<u64> {
        let mut pending = self.pending.lock().await;
        let mut queued = self.queued.lock().await;
        let mut attempts = self.attempts.lock().await;
        let mut escalation = self.escalation.lock().await;
        let mut in_flight = self.in_flight.lock().await;
        let mut newly_escalated = Vec::new();
        let shard_size = self.config.shard_size;

        for block in blocks {
            in_flight.remove(block);
            let count = attempts.entry(*block).or_insert(0);
            *count = count.saturating_add(1);
            if *count <= self.config.max_attempts_per_block {
                if !queued.contains(block) {
                    queued.insert(*block);
                    pending.push(Reverse(*block));
                }
            } else {
                escalation.add_block(*block, shard_size);
                tracing::debug!(
                    block = *block,
                    attempts = *count,
                    threshold = self.config.max_attempts_per_block,
                    escalation_total = escalation.total_count,
                    "scheduler: block escalated"
                );
                newly_escalated.push(*block);
            }
        }
        newly_escalated
    }

    /// Requeue a failed escalation block for indefinite retry.
    pub async fn requeue_escalation_block(&self, block: u64) {
        let mut escalation = self.escalation.lock().await;
        let completed = self.completed.lock().await;
        let mut in_flight = self.in_flight.lock().await;
        let shard_size = self.config.shard_size;

        in_flight.remove(&block);
        if completed.contains(&block) {
            escalation.remove_block(block, shard_size);
            return;
        }

        escalation.add_block(block, shard_size);
    }

    /// Record that a specific peer failed a specific block (per-peer-per-block backoff).
    pub async fn record_block_peer_failure(&self, block: u64, peer_id: PeerId) {
        let mut backoff = self.block_peer_backoff.lock().await;
        backoff.record_failure(block, peer_id);
    }

    /// Record a successful peer batch.
    pub async fn record_peer_success(&self, peer_id: PeerId) {
        self.peer_health.record_success(peer_id).await;
    }

    /// Record a partial response (some blocks returned, some missing).
    pub async fn record_peer_partial(&self, peer_id: PeerId) {
        self.peer_health.record_partial(peer_id).await;
    }

    /// Record a peer failure.
    pub async fn record_peer_failure(&self, peer_id: PeerId) {
        self.peer_health.record_failure(peer_id).await;
    }

    /// Check if peer is currently cooling down.
    pub async fn is_peer_cooling_down(&self, peer_id: PeerId) -> bool {
        self.peer_health.is_peer_cooling_down(peer_id).await
    }

    /// Check if all work is complete.
    pub async fn is_done(&self) -> bool {
        let pending_empty = self.pending.lock().await.is_empty();
        let inflight_empty = self.in_flight.lock().await.is_empty();
        let escalation_empty = self.escalation.lock().await.is_empty();
        pending_empty && inflight_empty && escalation_empty
    }

    /// Number of completed blocks.
    pub async fn completed_count(&self) -> usize {
        self.completed.lock().await.len()
    }

    /// Number of pending blocks (normal queue + escalation).
    pub async fn pending_count(&self) -> usize {
        let pending = self.pending.lock().await;
        let escalation = self.escalation.lock().await;
        pending.len() + escalation.len()
    }

    /// Number of in-flight blocks.
    pub async fn inflight_count(&self) -> usize {
        self.in_flight.lock().await.len()
    }

    /// Number of blocks in the escalation queue.
    pub async fn escalation_len(&self) -> usize {
        self.escalation.lock().await.len()
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler_with_blocks(config: SchedulerConfig, start: u64, end: u64) -> PeerWorkScheduler {
        let blocks = (start..=end).collect::<Vec<_>>();
        let peer_health = Arc::new(PeerHealthTracker::new(
            PeerHealthConfig::from_scheduler_config(&config),
        ));
        PeerWorkScheduler::new_with_health(config, blocks, peer_health)
    }

    #[tokio::test]
    async fn next_batch_respects_head_and_consecutive() {
        let config = SchedulerConfig {
            blocks_per_assignment: 3,
            ..Default::default()
        };
        let scheduler = scheduler_with_blocks(config, 0, 9);
        let peer_id = PeerId::random();

        let batch = scheduler.next_batch_for_peer(peer_id, 4).await;
        assert_eq!(batch.blocks, vec![0, 1, 2]);
        assert_eq!(batch.mode, FetchMode::Normal);

        let batch = scheduler.next_batch_for_peer(peer_id, 4).await;
        assert_eq!(batch.blocks, vec![3, 4]);
        assert_eq!(batch.mode, FetchMode::Normal);

        let batch = scheduler.next_batch_for_peer(peer_id, 4).await;
        assert!(batch.blocks.is_empty());
    }

    #[tokio::test]
    async fn requeue_promotes_to_escalation_after_max_attempts() {
        let config = SchedulerConfig {
            max_attempts_per_block: 2,
            ..Default::default()
        };
        let scheduler = scheduler_with_blocks(config, 1, 1);
        let block = 1;

        scheduler.requeue_failed(&[block]).await;
        assert_eq!(scheduler.escalation_len().await, 0);

        scheduler.requeue_failed(&[block]).await;
        assert_eq!(scheduler.escalation_len().await, 0);

        let escalated = scheduler.requeue_failed(&[block]).await;
        assert_eq!(escalated, vec![1]);
        assert_eq!(scheduler.escalation_len().await, 1);
    }

    #[tokio::test]
    async fn escalation_retries_failed_blocks() {
        let config = SchedulerConfig {
            max_attempts_per_block: 1,
            ..Default::default()
        };
        let scheduler = scheduler_with_blocks(config, 1, 1);
        let peer_id = PeerId::random();

        let batch = scheduler.next_batch_for_peer(peer_id, 10).await;
        assert_eq!(batch.blocks, vec![1]);

        scheduler.requeue_failed(&[1]).await;
        let batch = scheduler.next_batch_for_peer(peer_id, 10).await;
        assert_eq!(batch.blocks, vec![1]);

        scheduler.requeue_failed(&[1]).await;
        let batch = scheduler.next_batch_for_peer(peer_id, 10).await;
        assert_eq!(batch.blocks, vec![1]);
        assert_eq!(batch.mode, FetchMode::Escalation);
    }

    #[tokio::test]
    async fn peer_backoff_grows_exponentially() {
        let config = SchedulerConfig::default();
        let scheduler = scheduler_with_blocks(config, 0, 0);
        let peer_id = PeerId::random();

        scheduler.record_peer_failure(peer_id).await;
        assert!(scheduler.is_peer_cooling_down(peer_id).await);

        scheduler.record_peer_success(peer_id).await;
        assert!(!scheduler.is_peer_cooling_down(peer_id).await);

        scheduler.record_peer_failure(peer_id).await;
        scheduler.record_peer_failure(peer_id).await;
        assert!(scheduler.is_peer_cooling_down(peer_id).await);
    }

    #[tokio::test]
    async fn no_deadlock_mark_completed_vs_requeue_failed() {
        let config = SchedulerConfig::default();
        let scheduler = Arc::new(scheduler_with_blocks(config, 0, 99));
        let peer_id = PeerId::random();

        let batch = scheduler.next_batch_for_peer(peer_id, 200).await;
        assert!(!batch.blocks.is_empty());

        let even: Vec<u64> = batch.blocks.iter().copied().filter(|b| b % 2 == 0).collect();
        let odd: Vec<u64> = batch.blocks.iter().copied().filter(|b| b % 2 != 0).collect();

        let s1 = Arc::clone(&scheduler);
        let s2 = Arc::clone(&scheduler);

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async move {
            let h1 = tokio::spawn(async move { s1.mark_completed(&even).await });
            let h2 = tokio::spawn(async move { s2.requeue_failed(&odd).await });
            h1.await.map_err(|e| eyre::eyre!("{e}"))?;
            h2.await.map_err(|e| eyre::eyre!("{e}"))?;
            Ok::<_, eyre::Report>(())
        })
        .await;

        assert!(
            result.is_ok(),
            "deadlock detected: concurrent mark_completed + requeue_failed timed out"
        );
    }
}
