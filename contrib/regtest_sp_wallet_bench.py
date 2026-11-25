#!/usr/bin/env python3
"""Exercise Blocknil WalletCore and LDK sync against electrs during admitted scan load."""

import argparse
from contextlib import contextmanager
import json
import math
from pathlib import Path
import select
import shlex
import subprocess
import sys
import tempfile
import time

from regtest_sp_bench import (
    backfill, chain_fixture, electrs_cmd, find_bitcoind, local_ports, Node, scan_load, wait_for_server,
)

WALLET_SATS = 100_000
LDK_SATS = 150_000


def json_line(process, label):
    ready, _, _ = select.select([process.stdout], [], [], 45)
    if ready:
        line = process.stdout.readline()
        if line:
            return json.loads(line)
    raise RuntimeError(f"wallet probe did not return {label}")


@contextmanager
def probe(exe, port):
    with subprocess.Popen([str(exe), f"127.0.0.1:{port}"], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, text=True) as child:
        try:
            def command(text):
                child.stdin.write(text + "\n")
                child.stdin.flush()
                return json_line(child, text)

            yield json_line(child, "public addresses"), command
            child.stdin.write("stop\n")
            child.stdin.flush()
            if child.wait(10) != 0:
                raise RuntimeError("wallet probe did not exit cleanly")
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()


def sample_probe(exe, port, expected_addresses, samples):
    def percentile(values, p):
        return sorted(values)[math.ceil(len(values) * p / 100) - 1]

    with probe(exe, port) as (addresses, call):
        if addresses != expected_addresses:
            raise RuntimeError("disposable probe addresses changed")
        startup = call("start")["startup_ms"]
        timings = [call("sync") for _ in range(samples + 1)]
    if any(row["wallet_sats"] != WALLET_SATS or row["ldk_sats"] != LDK_SATS for row in timings):
        raise RuntimeError("wallet or LDK did not retain its confirmed payment")
    first, *steady = timings
    return {"startup_ms": startup, **{
        name: {"first_ms": first[f"{name}_ms"], **{
            f"p{p}_ms": percentile([sample[f"{name}_ms"] for sample in steady], p)
            for p in (50, 95, 99)
        }} for name in ("wallet", "ldk")
    }}


def run_trial(args, work, node, fixture, tip, addresses, run, mode):
    port, metrics = local_ports(2)
    enabled = isinstance(mode, int)
    binary = args.baseline if mode == "baseline" else args.candidate
    db = work / f"{run}-{mode}"
    command = electrs_cmd(binary, db, node, enabled, port, metrics)
    if enabled:
        command.extend(["--silent-payments-scan-rps", str(mode)])
    with open(db.with_suffix(".log"), "w") as log:
        # Compare client sync against ready indexes. The baseline's headers.subscribe assumes
        # genesis is indexed, so polling it on a fresh listener can panic before the trial.
        backfill(command, db, log)
        child = subprocess.Popen(command, stdout=log, stderr=log)
        try:
            ready = wait_for_server(child, port, tip, log)
            ready.close()
            idle = sample_probe(args.probe, port, addresses, args.samples)
            if not enabled:
                return {"idle": idle}
            start = time.perf_counter()
            with scan_load(port, fixture, args) as counters:
                loaded = sample_probe(args.probe, port, addresses, args.samples)
            return {"idle": idle, "loaded": loaded, "load": {
                **counters, "wall_s": time.perf_counter() - start
            }}
        except Exception:
            print(db.with_suffix(".log").read_text()[-4000:], file=sys.stderr)
            raise
        finally:
            child.terminate()
            try:
                child.wait(10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


def replay_wrapper(work, core, blocks):
    for name in ("bitcoind", "bitcoin-cli"):
        executable = shlex.quote(str(Path(core) / name))
        options = shlex.quote(f"-loadblock={blocks}") if name == "bitcoind" else ""
        wrapper = work / name
        wrapper.write_text(f'#!/bin/sh\nexec {executable} {options} "$@"\n')
        wrapper.chmod(0o700)
    return str(work)


def fixture_from_replay(args, node):
    until = time.monotonic() + 90
    while int(node.cli("getblockcount")) < args.replay_height:
        if time.monotonic() >= until:
            raise RuntimeError("Core did not import the regtest fixture")
        time.sleep(0.1)
    tip = int(node.cli("getblockcount"))
    if tip != args.replay_height:
        raise RuntimeError("replayed chain height differs from the requested fixture")
    block_hash = node.cli("getblockhash", tip)
    if len(json.loads(node.cli("getblock", block_hash))["tx"]) != args.txs_per_block + 1:
        raise RuntimeError("wrong transaction density in the replayed regtest block")
    return tip, block_hash, None, None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("baseline", "candidate", "probe"):
        parser.add_argument(arg, type=lambda path: Path(path).resolve(strict=True))
    parser.add_argument("--bitcoind-dir")
    parser.add_argument("--blocks-file", type=lambda path: Path(path).resolve(strict=True),
                        help="optional raw Core block replay file containing only disposable regtest data")
    parser.add_argument("--replay-height", type=int)
    parser.add_argument("--blocks", type=int, default=8)
    parser.add_argument("--txs-per-block", type=int, default=32)
    parser.add_argument("--rates", nargs="+", type=int, default=[128, 256, 1024])
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--samples", type=int, default=40)
    parser.add_argument("--scanners", type=int, default=12)
    parser.add_argument("--paced-scans-per-second", type=int,
                        help="space requests at this total rate instead of sending a burst")
    args = parser.parse_args()
    if bool(args.blocks_file) != bool(args.replay_height):
        parser.error("--blocks-file and --replay-height must be set together")
    if not (1 <= args.blocks <= 40 and 1 <= args.txs_per_block <= 128
            and args.blocks * args.txs_per_block <= 4000 and 1 <= args.runs <= 10
            and 20 <= args.samples <= 2000 and 1 <= args.scanners <= 32
            and (args.paced_scans_per_second is None or 1 <= args.paced_scans_per_second <= 4096)
            and len(set(args.rates)) == len(args.rates) and all(1 <= x <= 4096 for x in args.rates)):
        parser.error("fixture or admission bounds exceeded")

    with tempfile.TemporaryDirectory(prefix="electrs-sp-wallet-bench-") as directory:
        work = Path(directory)
        data = work / "core"
        data.mkdir()
        binaries = find_bitcoind(args.bitcoind_dir)
        if args.blocks_file:
            binaries = replay_wrapper(work, binaries, args.blocks_file)
        node = Node(binaries, str(data), *local_ports(2))
        try:
            node.start()
            if json.loads(node.cli("getnetworkinfo"))["version"] != 320100:
                raise RuntimeError("use the pinned Bitcoin Core 32.1 release")
            fixture = (fixture_from_replay(args, node) if args.blocks_file
                       else chain_fixture(node, args.blocks, args.txs_per_block)[0])
            control_port = local_ports(1)[0]
            with probe(args.probe, control_port) as (addresses, _):
                pass
            node.cli("createwallet", "probe_funding")
            miner = node.cli("getnewaddress", "", "bech32", wallet="probe_funding")
            node.cli("generatetoaddress", 102, miner)
            node.cli("sendtoaddress", addresses["wallet_addr"], f"{WALLET_SATS / 1e8:.8f}",
                     wallet="probe_funding")
            node.cli("sendtoaddress", addresses["ldk_addr"], f"{LDK_SATS / 1e8:.8f}",
                     wallet="probe_funding")
            node.cli("generatetoaddress", 1, miner)
            tip = int(node.cli("getblockcount"))
            modes = ["baseline", "candidate_disabled", *args.rates]
            runs = []
            for run in range(args.runs):
                pivot = run % len(modes)
                rows = {}
                for mode in modes[pivot:] + modes[:pivot]:
                    print(f"wallet sync trial {run + 1} {mode}", file=sys.stderr)
                    rows[str(mode)] = run_trial(args, work, node, fixture, tip, addresses, run, mode)
                runs.append(rows)
            print(json.dumps({"fixture": {"wallet_height": tip, "scan_height": fixture[0],
                                          "wallet_sats": WALLET_SATS, "ldk_sats": LDK_SATS,
                                          "samples": args.samples, "scanners": args.scanners,
                                          "paced_scans_per_second": args.paced_scans_per_second},
                              "runs": runs}, indent=2))
        finally:
            node.stop()


if __name__ == "__main__":
    main()
