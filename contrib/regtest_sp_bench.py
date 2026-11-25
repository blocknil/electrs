#!/usr/bin/env python3
"""Compare electrs release binaries on the same disposable Silent Payments regtest chain."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import math
import os
import select
from pathlib import Path
import socket
import stat
import statistics
import subprocess
import sys
import tempfile
from threading import Barrier, Event, Thread
import time

from regtest_smoke import Electrum, Node, find_bitcoind, scripthash

METHOD = "blockchain.silent_payments.get"
MODES = ("baseline", "candidate_disabled", "candidate_enabled")


def local_ports(count):
    listeners = [socket.socket() for _ in range(count)]
    try:
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def chain_fixture(node, blocks, txs_per_block):
    cli = node.cli
    cli("createwallet", "bench")
    mining_address = cli("getnewaddress", "", "bech32", wallet="bench")
    recipient = cli("getnewaddress", "", "bech32m", wallet="bench")
    # A separate confirmed coinbase input per transaction avoids a mempool ancestor chain.
    count = blocks * txs_per_block
    print(f"mining {count + 101} fixture funding blocks", file=sys.stderr)
    cli("generatetoaddress", count + 101, mining_address)
    coins = json.loads(cli("listunspent", 101, wallet="bench"))
    coins = sorted(coins, key=lambda coin: (-coin["confirmations"], coin["txid"]))
    if len(coins) < count:
        raise RuntimeError("insufficient mature fixture outputs")

    taproot_outputs = 0
    for start in range(0, count, txs_per_block):
        for coin in coins[start:start + txs_per_block]:
            options = {"inputs": [{"txid": coin["txid"], "vout": coin["vout"]}],
                       "add_inputs": False}
            rpc = cli("-named", "send", "fee_rate=1",
                      "outputs=" + json.dumps({recipient: 0.00005}),
                      "options=" + json.dumps(options), wallet="bench")
            payment = json.loads(rpc)
            if not payment.get("complete") or "txid" not in payment:
                raise RuntimeError("fixture payment was not signed and broadcast")
        tx_count = len(json.loads(cli("getrawmempool")))
        if tx_count != txs_per_block:
            raise RuntimeError(f"expected {txs_per_block} transactions, found {tx_count}")
        block_hash = json.loads(cli("generatetoaddress", 1, mining_address))[0]
        mined = json.loads(cli("getblock", block_hash, 2))
        taproot_outputs += sum(out["scriptPubKey"]["type"] == "witness_v1_taproot"
                               for tx in mined["tx"][1:] for out in tx["vout"])
        completed = start // txs_per_block + 1
        if completed % 4 == 0 or completed == blocks:
            print(f"mined {completed}/{blocks} transaction-bearing fixture blocks", file=sys.stderr)

    tip = int(cli("getblockcount"))
    block_hash = cli("getblockhash", tip)
    latest = json.loads(cli("getblock", block_hash))
    if len(latest["tx"]) != txs_per_block + 1:
        raise RuntimeError("fixture tip did not include all payments")
    script = json.loads(cli("getaddressinfo", recipient, wallet="bench"))["scriptPubKey"]
    return (tip, block_hash, latest["tx"][-1], scripthash(script)), taproot_outputs


def allocated_bytes(path):
    total = 0
    for entry in path.rglob("*"):
        try:
            info = entry.stat()
        except FileNotFoundError:
            continue  # Compaction can remove a file between enumeration and stat.
        if stat.S_ISREG(info.st_mode):
            total += info.st_blocks * 512
    return total


def electrs_cmd(binary, db, node, enabled, electrum_port, metrics_port):
    flags = ["--index-silent-payments"] if enabled else []
    return [str(binary), "--network", "regtest", "--daemon-dir", node.datadir,
            "--daemon-rpc-addr", f"127.0.0.1:{node.rpc_port}", "--db-dir", str(db),
            "--electrum-rpc-addr", f"127.0.0.1:{electrum_port}",
            "--monitoring-addr", f"127.0.0.1:{metrics_port}",
            "--log-filters", "WARN", *flags]


def backfill(cmd, db, log):
    stop = Event()
    high_water = [0]

    def sample_disk():
        while not stop.is_set():
            high_water[0] = max(high_water[0], allocated_bytes(db))
            stop.wait(0.02)

    observer = Thread(target=sample_disk)
    observer.start()
    try:
        start = time.perf_counter()
        child = subprocess.Popen([*cmd, "--disable-electrum-rpc", "--sync-once"], stdout=log, stderr=log)
        # wait4 gives per-process resource usage even across multiple trials.
        try:
            while True:
                pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                if pid:
                    child.returncode = os.waitstatus_to_exitcode(status)
                    break
                if time.perf_counter() - start > 120:
                    raise TimeoutError("regtest backfill exceeded 120 seconds")
                time.sleep(0.01)
        finally:
            if child.returncode is None:
                child.kill()
                child.wait()
        elapsed = time.perf_counter() - start
    finally:
        stop.set()
        observer.join()
    if child.returncode != 0:
        raise RuntimeError(f"electrs backfill exited {child.returncode}, see {log.name}")
    final = allocated_bytes(db)
    return {"wall_s": elapsed, "cpu_s": usage.ru_utime + usage.ru_stime,
            "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
            "final_db_bytes": final, "peak_db_bytes": max(high_water[0], final)}


def wait_for_server(child, port, tip, log):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if child.poll() is not None:
            break
        conn = None
        try:
            conn = Electrum(port)
            conn.s.settimeout(3)
            if conn.ok("blockchain.headers.subscribe")["height"] == tip:
                return conn
        except (OSError, ValueError, RuntimeError):
            pass
        if conn is not None:
            conn.close()
        time.sleep(0.05)
    raise RuntimeError(f"electrs did not expose the fixture tip, see {log.name}")


def rpc_bundle(client, fixture):
    tip, _, txid, script_hash = fixture
    return [
        client.ok("server.ping"),
        client.ok("blockchain.block.header", [tip]),
        client.ok("blockchain.scripthash.get_history_paged", [script_hash, None, 10]),
        client.ok("blockchain.transaction.get_merkle", [txid, tip]),
    ]


def rpc_timing(client, fixture, samples):
    values = []
    for _ in range(samples):
        start = time.perf_counter_ns()
        rpc_bundle(client, fixture)
        values.append((time.perf_counter_ns() - start) / 1e6)
    values.sort()
    return {f"p{p}_ms": values[math.ceil(len(values) * p / 100) - 1] for p in (50, 95, 99)}


def scan_driver(port, tip, block, dense_blocks, txs_per_block, scanners, paced_rate=None):
    start = Barrier(scanners + 1, timeout=10)
    stop = Event()

    def scan(worker):
        with socket.create_connection(("127.0.0.1", port), timeout=10) as peer:
            served, busy = 0, 0
            with peer.makefile("rb") as reader:
                start.wait()
                next_send = time.monotonic() + (worker / paced_rate if paced_rate else 0)
                while not stop.is_set():
                    if paced_rate:
                        if stop.wait(max(0, next_send - time.monotonic())):
                            break
                        # Space sends from the actual dispatch time. A late wakeup must not
                        # trigger underspaced catch-up requests on the next iteration.
                        next_send = time.monotonic() + scanners / paced_rate
                    height = tip - (worker + served + busy) % dense_blocks
                    request = {"jsonrpc": "2.0", "id": 1, "method": METHOD,
                               "params": {"height": height, "end_height": tip, "end_hash": block}}
                    peer.sendall((json.dumps(request) + "\n").encode())
                    response = json.loads(reader.readline())
                    result = response.get("result")
                    if (result is not None and result["height"] == height
                            and len(result["entries"]) == txs_per_block):
                        served += 1
                    elif response.get("error", {}).get("code") == -32020:
                        busy += 1
                    else:
                        raise RuntimeError("unexpected Silent Payments scan response")
            return served, busy

    with ThreadPoolExecutor(max_workers=scanners) as pool:
        jobs = [pool.submit(scan, worker) for worker in range(scanners)]
        try:
            start.wait()
            began = time.monotonic()
            print("ready", flush=True)
            sys.stdin.read(1)
            scan_window = time.monotonic() - began
        finally:
            stop.set()
        completed = [job.result() for job in jobs]
    print(json.dumps({"scans_served": sum(served for served, _ in completed),
                      "scans_busy": sum(busy for _, busy in completed),
                      "scan_window_s": scan_window}))


@contextmanager
def scan_load(port, fixture, args):
    tip, block, _, _ = fixture
    spec = json.dumps([port, tip, block, args.blocks, args.txs_per_block, args.scanners,
                       getattr(args, "paced_scans_per_second", None)])
    # Keep the load generator's Python threads off the timed client's interpreter.
    with subprocess.Popen([sys.executable, "-B", __file__, "--scan-driver", spec],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as driver:
        try:
            ready, _, _ = select.select([driver.stdout], [], [], 15)
            if not ready or driver.stdout.readline().strip() != "ready":
                raise RuntimeError("scan driver failed to start")
            counters = {}
            yield counters
            output, _ = driver.communicate(input="\n", timeout=15)
            if driver.returncode != 0:
                raise RuntimeError("scan driver failed")
        finally:
            if driver.poll() is None:
                driver.kill()
                driver.wait()
    counters.update(json.loads(output))
    if counters["scans_served"] == 0:
        raise RuntimeError("no successful scans during the loaded trial")


def loaded_timing(ordinary, port, fixture, args):
    with scan_load(port, fixture, args) as counters:
        timing = rpc_timing(ordinary, fixture, args.samples)
    timing.update(counters)
    return timing


def trial(binary, db, node, enabled, fixture, args):
    port, metrics = local_ports(2)
    command = electrs_cmd(binary, db, node, enabled, port, metrics)
    with open(db.with_suffix(".log"), "w") as log:
        index = backfill(command, db, log)
        child = subprocess.Popen(command, stdout=log, stderr=log)
        try:
            client = wait_for_server(child, port, fixture[0], log)
            try:
                payload = json.dumps(rpc_bundle(client, fixture), sort_keys=True)
                fingerprint = hashlib.sha256(payload.encode()).hexdigest()
                rpc_timing(client, fixture, 10)
                idle = rpc_timing(client, fixture, args.samples)
                loaded = loaded_timing(client, port, fixture, args) if enabled else None
            finally:
                client.close()
        finally:
            child.terminate()
            try:
                child.wait(10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
    return {"index": index, "idle_rpc": idle, "scan_load_rpc": loaded,
            "rpc_fingerprint": fingerprint}


def aggregate(runs):
    return {
        mode: {
            section: {key: statistics.median(run[mode][section][key] for run in runs)
                      for key in runs[0][mode][section]}
            for section in runs[0][mode] if isinstance(runs[0][mode][section], dict)
        }
        for mode in MODES
    }


def run_trials(node, fixture, taproot_outputs, args, work):
    runs = []
    for run in range(args.runs):
        result = {}
        # Rotate the order so the same variant does not always get the warmest cache.
        for mode in MODES[run % 3:] + MODES[:run % 3]:
            print(f"trial {run + 1} {mode}", file=sys.stderr)
            binary = args.baseline if mode == "baseline" else args.candidate
            try:
                result[mode] = trial(binary, work / f"{run}-{mode}", node,
                                     mode == "candidate_enabled", fixture, args)
            except Exception:
                log = (work / f"{run}-{mode}.log")
                if log.exists():
                    print(log.read_text()[-4000:], file=sys.stderr)
                raise
        if len({item["rpc_fingerprint"] for item in result.values()}) != 1:
            raise RuntimeError("ordinary Electrum results diverged between modes")
        runs.append(result)
    return {"fixture": {"tip": fixture[0], "dense_blocks": args.blocks,
                                  "txs_per_block": args.txs_per_block,
                                  "taproot_outputs": taproot_outputs,
                                  "rpc_samples": args.samples, "scanners": args.scanners},
                      "runs": runs, "medians": aggregate(runs)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=lambda arg: Path(arg).resolve(strict=True))
    parser.add_argument("candidate", type=lambda arg: Path(arg).resolve(strict=True))
    parser.add_argument("--bitcoind-dir")
    parser.add_argument("--blocks", type=int, default=24)
    parser.add_argument("--txs-per-block", type=int, default=64)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--scanners", type=int, default=12)
    args = parser.parse_args()
    if not (1 <= args.blocks <= 40 and 1 <= args.txs_per_block <= 128
            and args.blocks * args.txs_per_block <= 4000
            and 1 <= args.runs <= 10 and 20 <= args.samples <= 2000 and 1 <= args.scanners <= 32):
        parser.error("fixture bounds exceeded")

    with tempfile.TemporaryDirectory(prefix="electrs-sp-bench-") as temp:
        work = Path(temp)
        data = work / "core"
        data.mkdir()
        ports = local_ports(2)
        node = Node(find_bitcoind(args.bitcoind_dir), str(data), *ports)
        try:
            node.start()
            if json.loads(node.cli("getnetworkinfo"))["version"] != 320100:
                raise RuntimeError("use the pinned Bitcoin Core 32.1 release")
            fixture, taproot_outputs = chain_fixture(node, args.blocks, args.txs_per_block)
            print(f"fixture tip={fixture[0]}, dense blocks={args.blocks}, "
                  f"eligible transactions={args.blocks * args.txs_per_block}, "
                  f"Taproot outputs={taproot_outputs}", file=sys.stderr)
            print(json.dumps(run_trials(node, fixture, taproot_outputs, args, work), indent=2))
        finally:
            node.stop()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--scan-driver":
        scan_driver(*json.loads(sys.argv[2]))
    else:
        main()
