#!/usr/bin/env python3
"""Capture small RPC reference samples for offline archive acceptance tests.

This is test tooling, not an input source used by the indexer. Provider URLs
and API keys are never stored in fixtures. Set BASE_ACCEPTANCE_RPC_URL to use
a provider other than the public Base endpoint.
"""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import time
import urllib.parse
import urllib.request


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for attempt in range(4):
        try:
            request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=60) as response:
                data = json.load(response)
            if data.get("error") or data.get("result") is None:
                raise RuntimeError(f"{method} returned no usable result")
            return data["result"]
        except (OSError, ValueError, RuntimeError):
            if attempt == 3:
                raise RuntimeError(f"reference request failed: {method}") from None
            time.sleep(2 ** attempt)
    raise AssertionError("unreachable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("tests/fixtures/base-reference"))
    parser.add_argument("--blocks", type=int, nargs="+", default=[5_000_000, 9_101_527, 30_008_527, 47_810_527, 51_925_326])
    parser.add_argument("--env-file", type=pathlib.Path, help="local file containing BASE_ACCEPTANCE_RPC_URL or ALCHEMY_RPC_URL")
    args = parser.parse_args()
    if any(block < 0 for block in args.blocks):
        parser.error("block numbers must be nonnegative")
    settings = dict(os.environ)
    if args.env_file:
        for line in args.env_file.read_text().splitlines():
            name, separator, value = line.strip().removeprefix("export ").partition("=")
            if separator and name.strip() in ("BASE_ACCEPTANCE_RPC_URL", "ALCHEMY_RPC_URL"):
                settings.setdefault(name.strip(), value.strip().strip("\"'"))
    url = settings.get("BASE_ACCEPTANCE_RPC_URL") or settings.get("ALCHEMY_RPC_URL") or "https://mainnet.base.org"
    if int(rpc(url, "eth_chainId", []), 16) != 8453:
        raise RuntimeError("reference provider must serve Base mainnet")
    args.output.mkdir(parents=True, exist_ok=True)
    index = []
    for number in args.blocks:
        block = rpc(url, "eth_getBlockByNumber", [hex(number), True])
        receipts = rpc(url, "eth_getBlockReceipts", [hex(number)])
        if int(block["number"], 16) != number or len(receipts) != len(block["transactions"]):
            raise RuntimeError(f"incomplete reference block {number}")
        for position, (tx, receipt) in enumerate(zip(block["transactions"], receipts)):
            if (receipt["transactionHash"] != tx["hash"]
                    or receipt["blockHash"] != block["hash"]
                    or int(receipt["transactionIndex"], 16) != position):
                raise RuntimeError(f"reference receipt alignment mismatch at {number}:{position}")
        data = {
            "chain_id": 8453,
            "source_host": urllib.parse.urlparse(url).hostname,
            "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "block": block,
            "receipts": receipts,
        }
        raw = (json.dumps(data, separators=(",", ":")) + "\n").encode()
        name = f"{number}.json"
        args.output.joinpath(name).write_bytes(raw)
        index.append({"file": name, "block": number, "hash": block["hash"],
                      "transactions": len(receipts), "bytes": len(raw),
                      "sha256": hashlib.sha256(raw).hexdigest()})
        print(f"captured {number}: {len(receipts)} transactions, {len(raw)} bytes")
    args.output.joinpath("index.json").write_text(json.dumps(index, indent=2) + "\n")


if __name__ == "__main__":
    main()
