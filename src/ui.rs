//! Pretty terminal UI.
//!
//! Clean production output: startup banner, progress bar, follow status.
//! All output writes to stderr, keeping stdout clean for piping.
//! Suppressed when `--verbose` is set (falls back to tracing logs).

use crate::sync::engine::SyncOutcome;
use owo_colors::OwoColorize;
use std::time::Duration;
use tokio::time::Instant;

/// Braille spinner frames for animated status lines.
const SPINNER_FRAMES: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// Animated braille spinner for waiting-phase status lines.
pub struct Spinner {
    tick: usize,
}

impl Spinner {
    /// Create a new spinner at frame 0.
    pub const fn new() -> Self {
        Self { tick: 0 }
    }

    /// Return the current spinner character and advance to the next frame.
    pub fn frame(&mut self) -> char {
        let ch = SPINNER_FRAMES[self.tick % SPINNER_FRAMES.len()];
        self.tick += 1;
        ch
    }
}

/// Format a number with comma separators (e.g., `22,516,100`).
#[must_use]
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len <= 3 {
        return s;
    }
    let mut result = String::with_capacity(len + len / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}

/// Format a duration as human-readable (e.g., `2m 34s`).
#[must_use]
pub fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if hours > 0 {
        format!("{hours}h {minutes}m {seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds}s")
    } else {
        format!("{seconds}s")
    }
}

/// Render a progress bar string (e.g., `████████████░░░░░░░░░░░░░░░░░░`).
#[must_use]
pub fn render_progress_bar(fraction: f64) -> String {
    const WIDTH: usize = 30;
    let filled = (fraction * WIDTH as f64).round() as usize;
    let filled = filled.min(WIDTH);
    let empty = WIDTH - filled;

    let mut bar = String::with_capacity(WIDTH * 3); // UTF-8 chars are multi-byte
    for _ in 0..filled {
        bar.push('█');
    }
    for _ in 0..empty {
        bar.push('░');
    }
    bar
}

/// Estimate remaining time from elapsed duration and progress fraction.
///
/// Returns `None` when progress is too small for a meaningful estimate.
#[must_use]
pub fn format_eta(elapsed: Duration, fraction: f64) -> Option<String> {
    if fraction < 0.01 {
        return None;
    }
    let elapsed_secs = elapsed.as_secs_f64();
    let remaining_secs = (elapsed_secs * (1.0 - fraction) / fraction) as u64;
    Some(format_duration(Duration::from_secs(remaining_secs)))
}

/// Print the startup banner.
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn print_banner(
    version: &str,
    config_path: &str,
    database_url: &str,
    table_names: &[&str],
    api_port: Option<u16>,
) {
    eprintln!();
    eprintln!("  {} {}", "sieve".green(), version.dimmed());
    eprintln!();
    print_label("config", config_path);

    // Mask password in database URL for display
    let display_url = mask_db_password(database_url);
    print_label("database", &display_url);

    if !table_names.is_empty() {
        let tables = table_names.join(", ");
        print_label("tables", &tables);
    }

    if let Some(port) = api_port {
        let url = format!("http://localhost:{port}/graphql");
        eprintln!("  {}{}", pad_label("api"), url.green());
    }

    eprintln!();
}

/// Print the in-place sync progress line.
///
/// Prints with `\r` to overwrite the current line. Warnings from tracing
/// end with `\n`, so they push the cursor to a new line — the next
/// progress update naturally overwrites from there.
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn print_sync_progress(
    blocks_done: u64,
    total_blocks: u64,
    peer_count: usize,
    started_at: Instant,
) {
    let fraction = if total_blocks == 0 {
        0.0
    } else {
        blocks_done as f64 / total_blocks as f64
    };
    let pct = (fraction * 100.0) as u64;
    let bar = render_progress_bar(fraction);

    let elapsed = started_at.elapsed();
    let blk_per_sec = if elapsed.as_secs() > 0 {
        blocks_done / elapsed.as_secs()
    } else {
        0
    };

    let eta = format_eta(elapsed, fraction)
        .map_or_else(String::new, |eta| format!(" | remaining: ~{eta}"));

    eprint!(
        "\x1b[2K\r  {}  {} {pct}% | {} / {} | peers: {peer_count} | {} b/s{eta}",
        "syncing".dimmed(),
        bar.green(),
        format_number(blocks_done),
        format_number(total_blocks).dimmed(),
        format_number(blk_per_sec),
    );
}

/// Print the sync completion line.
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn print_sync_complete(outcome: &SyncOutcome) {
    let events = if outcome.events_stored > 0 {
        format!(", {} events", format_number(outcome.events_stored))
    } else {
        String::new()
    };
    eprintln!(
        "\x1b[2K\r  {} {} blocks in {} ({} blk/s{events})",
        "✓ synced".green(),
        format_number(outcome.blocks_fetched),
        format_duration(outcome.elapsed),
        format_number(
            outcome
                .blocks_fetched
                .checked_div(outcome.elapsed.as_secs().max(1))
                .unwrap_or(0)
        ),
    );
}

/// Print the in-place "discovering chain head" status line with spinner.
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn print_discovering(peer_count: usize, spinner: char) {
    eprint!(
        "\x1b[2K\r  {spinner} {}",
        format!("discovering chain head... | peers: {peer_count}").dimmed(),
    );
}

/// Print the in-place follow status line with pipe separators.
///
/// `block_timestamp` is unix seconds; when non-zero, shows "Xs ago".
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn print_follow_status(
    block_number: u64,
    peer_count: usize,
    block_timestamp: u64,
    reorg_depth: Option<u32>,
) {
    let ago = format_block_age(block_timestamp);
    let reorg_info = reorg_depth.map_or_else(String::new, |depth| {
        format!(" | {}", format!("⚠ reorg depth={depth}").yellow())
    });
    eprint!(
        "\x1b[2K\r  {} | block: {} |{ago} peers: {peer_count}{reorg_info}",
        "following".dimmed(),
        format_number(block_number),
    );
}

/// Format block age as " Xs ago |" from a unix timestamp.
///
/// Returns empty string if timestamp is zero (not yet known).
#[must_use]
fn format_block_age(block_timestamp: u64) -> String {
    if block_timestamp == 0 {
        return String::from(" ");
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let age = now.saturating_sub(block_timestamp);
    format!(" {} ago | ", format_duration(std::time::Duration::from_secs(age)))
}

/// Pad a label to a fixed width and dim it.
#[must_use]
fn pad_label(label: &str) -> String {
    format!("{label:<11}").dimmed().to_string()
}

/// Print a dimmed label with a value.
#[expect(clippy::print_stderr, reason = "pretty UI output")]
fn print_label(label: &str, value: &str) {
    eprintln!("  {}{value}", pad_label(label));
}

/// Print a message in pretty mode (e.g., "nothing to index").
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn print_info(message: &str) {
    eprintln!("  {}", message.dimmed());
}

/// Print "connecting to peers..." status with spinner.
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn print_connecting(spinner: char) {
    use std::io::Write;
    eprint!("\x1b[2K\r  {spinner} {}", "connecting to peers...".dimmed());
    std::io::stderr().flush().ok();
}

/// Clear the current line (used after "connecting..." resolves).
#[expect(clippy::print_stderr, reason = "pretty UI output")]
pub fn clear_line() {
    eprint!("\x1b[2K\r");
}

/// Mask the password in a PostgreSQL connection URL for display.
#[must_use]
fn mask_db_password(url: &str) -> String {
    // postgres://user:password@host/db → postgres://user:***@host/db
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            // Check that the colon is after "://" (scheme separator)
            if let Some(scheme_end) = url.find("://") {
                if colon_pos > scheme_end + 2 {
                    let mut masked = String::with_capacity(url.len());
                    masked.push_str(&url[..=colon_pos]);
                    masked.push_str("***");
                    masked.push_str(&url[at_pos..]);
                    return masked;
                }
            }
        }
    }
    url.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_number_small() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
    }

    #[test]
    fn format_number_thousands() {
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1_000_000), "1,000,000");
        assert_eq!(format_number(22_516_100), "22,516,100");
    }

    #[test]
    fn format_duration_seconds_only() {
        assert_eq!(format_duration(Duration::from_secs(45)), "45s");
    }

    #[test]
    fn format_duration_minutes_and_seconds() {
        assert_eq!(format_duration(Duration::from_secs(154)), "2m 34s");
    }

    #[test]
    fn format_duration_hours() {
        assert_eq!(format_duration(Duration::from_secs(3912)), "1h 5m 12s");
    }

    #[test]
    fn render_progress_bar_empty() {
        let bar = render_progress_bar(0.0);
        assert_eq!(bar.chars().filter(|&c| c == '░').count(), 30);
    }

    #[test]
    fn render_progress_bar_full() {
        let bar = render_progress_bar(1.0);
        assert_eq!(bar.chars().filter(|&c| c == '█').count(), 30);
    }

    #[test]
    fn render_progress_bar_half() {
        let bar = render_progress_bar(0.5);
        assert_eq!(bar.chars().filter(|&c| c == '█').count(), 15);
        assert_eq!(bar.chars().filter(|&c| c == '░').count(), 15);
    }

    #[test]
    fn mask_db_password_standard() {
        assert_eq!(
            mask_db_password("postgres://user:secret@localhost/db"),
            "postgres://user:***@localhost/db"
        );
    }

    #[test]
    fn mask_db_password_no_password() {
        assert_eq!(
            mask_db_password("postgres://localhost/db"),
            "postgres://localhost/db"
        );
    }

    #[test]
    fn mask_db_password_with_port() {
        assert_eq!(
            mask_db_password("postgres://postgres:sieve@localhost:5432/sieve"),
            "postgres://postgres:***@localhost:5432/sieve"
        );
    }

    #[test]
    fn spinner_cycles_through_frames() {
        let mut spinner = Spinner::new();
        assert_eq!(spinner.frame(), '⠋');
        assert_eq!(spinner.frame(), '⠙');
        assert_eq!(spinner.frame(), '⠹');
        // After 10 ticks, wraps around
        for _ in 0..7 {
            spinner.frame();
        }
        assert_eq!(spinner.frame(), '⠋');
    }

    #[test]
    fn format_eta_too_early() {
        assert!(format_eta(Duration::from_secs(1), 0.005).is_none());
    }

    #[test]
    fn format_eta_halfway() {
        // 30s elapsed at 50% → ~30s remaining
        let eta = format_eta(Duration::from_secs(30), 0.5);
        assert_eq!(eta.as_deref(), Some("30s"));
    }

    #[test]
    fn format_eta_near_complete() {
        // 100s elapsed at 90% → ~11s remaining
        let eta = format_eta(Duration::from_secs(100), 0.9);
        assert_eq!(eta.as_deref(), Some("11s"));
    }
}
