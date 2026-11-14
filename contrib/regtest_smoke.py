#!/usr/bin/env python3
"""Regtest smoke test: does this binary answer every Electrum method our clients call?

Starts a throwaway bitcoind 32.1 (regtest) and the electrs binary under test on a scratch
database, builds a history for one address (18 payments over 6 blocks, 6 spends, then one
unconfirmed payment), and calls every method the Blocknil wallet core, the push relay and
the Clearblock backend use -- plus ``blockchain.scripthash.get_history_paged`` in every
shape it accepts. It then restarts electrs with ``--index-lookup-limit`` at the boundary (a
history of exactly ``limit`` entries is SERVED) and one below it (the same history is
``limit + 1`` candidates and REFUSED), then at 5, and checks that each refusal is a plain
JSON-RPC error with code 1 and one exact message -- clients may match on it -- that the
server still answers ``server.ping`` afterwards, that a small address is still served, and
that the paged method is not affected by the limit.

Expectations are cross-checked against bitcoind itself (every block's transactions with
prevouts, ``getblock ... 3``), not against electrs' own other methods, so a change that
breaks both the paged and the unpaged history still fails here.

Nothing talks to a real server: a temporary datadir, a temporary electrs db, and ports that
are only used here (override with the SMOKE_* environment variables if they are taken).

Requires Bitcoin Core 32.1.0 and nothing else -- the REST endpoints electrs 0.12.1 uses for
block indexing got their final shape in 32.x, and a 32.1.x or 32.10 build is a different
release: the version is compared as a whole token, not as a prefix. Pass its ``bin``
directory with ``--bitcoind-dir``, or have the tarball unpacked at ``~/core-releases/32.1``
(the ops layout).

Usage:
    python3 contrib/regtest_smoke.py [--bitcoind-dir DIR] [--workdir DIR] [--keep] [BINARY]

``BINARY`` defaults to ``target/release/electrs`` next to this checkout. Exit status is 0
only if every check passed.
"""
import argparse
import glob
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

results = []

# The exact Core release this binary's REST block reads were verified on. Whole token, so
# v32.1.7 and v32.10.0 (both of which contain "v32.1") are refused.
BITCOIND_VERSION = "v32.1.0"


def check(name, ok, detail=None):
    results.append((name, ok, detail))
    note = "" if detail is None else " :: " + str(detail)[:300]
    print(("PASS " if ok else "FAIL ") + name + note, flush=True)


def alive(proc, what):
    """Something this script started must still be running at this point.

    A refusal that kills the server, or a node that died mid-run, would otherwise leave the
    later checks passing against a different process (or failing for the wrong reason), and a
    "refused" check would pass even if the process died while refusing.
    """
    rc = proc.poll()
    check(f"{what} still running", rc is None, None if rc is None else f"exited rc={rc}")
    return rc is None


def refusal_message(limit):
    """The refusal src/status.rs raises, as the client sees it (electrum.rs: RpcError::BadRequest
    -> {"code": 1, "message": <error>}). Clients may match on it, so the checks below compare the
    whole string rather than looking for "error"."""
    return f"index_lookup_limit: script hash history has more than {limit} transactions; refusing to build it"


def refused(r, limit):
    """True only for that exact refusal: JSON-RPC error code 1 and the message above."""
    err = r.get("error") or {}
    return err.get("code") == 1 and err.get("message") == refusal_message(limit)


def env_port(name, default):
    return int(os.environ.get(name) or default)


class Node:
    """bitcoind + bitcoin-cli in one scratch datadir."""

    def __init__(self, bindir, datadir, rpc_port, p2p_port):
        self.bindir = bindir
        self.datadir = datadir
        self.rpc_port = rpc_port
        self.p2p_port = p2p_port

    def cli(self, *args, wallet=None):
        cmd = [os.path.join(self.bindir, "bitcoin-cli"), "-regtest",
               f"-datadir={self.datadir}", f"-rpcport={self.rpc_port}"]
        if wallet:
            cmd.append(f"-rpcwallet={wallet}")
        out = subprocess.run(cmd + [str(a) for a in args], capture_output=True, text=True)
        if out.returncode:
            raise RuntimeError(f"bitcoin-cli {args}: {out.stderr}")
        return out.stdout.strip()

    def start(self):
        self.proc = subprocess.Popen(
            [os.path.join(self.bindir, "bitcoind"), "-regtest", f"-datadir={self.datadir}",
             f"-rpcport={self.rpc_port}", f"-port={self.p2p_port}", "-server=1", "-rest=1",
             "-txindex=0", "-listen=0", "-fallbackfee=0.0002", "-printtoconsole=0"])
        for _ in range(60):
            try:
                self.cli("getblockchaininfo")
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("bitcoind did not come up")

    def stop(self):
        try:
            self.cli("stop")
            self.proc.wait(60)
        except Exception:
            self.proc.kill()


class Electrum:
    """One JSON-RPC connection, skipping subscription notifications."""

    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.f = self.s.makefile("rb")
        self.id = 0

    def send(self, payload):
        self.s.sendall((json.dumps(payload) + "\n").encode())
        while True:
            msg = json.loads(self.f.readline())
            if isinstance(msg, dict) and "method" in msg and "id" not in msg:
                continue
            return msg

    def call(self, method, params=()):
        self.id += 1
        return self.send({"jsonrpc": "2.0", "id": self.id, "method": method,
                          "params": list(params)})

    def ok(self, method, params=()):
        r = self.call(method, params)
        if "error" in r and r["error"]:
            raise RuntimeError(f"{method}{list(params)}: {r['error']}")
        return r["result"]

    def close(self):
        self.s.close()


def scripthash(spk_hex):
    return hashlib.sha256(bytes.fromhex(spk_hex)).digest()[::-1].hex()


def main():
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("binary", nargs="?", default=None,
                    help="electrs binary to test (default: target/release/electrs)")
    ap.add_argument("--bitcoind-dir", default=None,
                    help="directory holding bitcoind and bitcoin-cli (default: "
                         "$BITCOIND_DIR, then ~/core-releases/32.1)")
    ap.add_argument("--workdir", default=None,
                    help="where to put the datadir and the electrs db (default: a temp dir)")
    ap.add_argument("--keep", action="store_true",
                    help="keep the work dir (and print it) for debugging")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    binary = os.path.abspath(args.binary or os.path.join(repo, "target", "release", "electrs"))
    if not os.access(binary, os.X_OK):
        sys.exit(f"regtest_smoke: {binary} is not executable (build it, or pass a path)")

    bindir = find_bitcoind(args.bitcoind_dir)
    version_line = subprocess.run([os.path.join(bindir, "bitcoind"), "--version"],
                                  capture_output=True, text=True).stdout.splitlines()[0]
    found = re.search(r"[Vv]ersion (v[0-9][0-9.]*)", version_line)
    version = found.group(1) if found else ""
    if version != BITCOIND_VERSION:
        sys.exit(f"regtest_smoke: {bindir}/bitcoind is '{version_line}' (version "
                 f"'{version or 'none'}'), not exactly Core {BITCOIND_VERSION} "
                 f"(electrs 0.12.1's REST block reads were verified on that release and a "
                 f"32.1.x is a different one; pass --bitcoind-dir with the 32.1 bin directory)")
    print(f"electrs:  {binary}")
    print(f"bitcoind: {bindir} ({version_line})")

    work = os.path.abspath(args.workdir or tempfile.mkdtemp(prefix="electrs-regtest-smoke-"))
    data = os.path.join(work, "data")
    shutil.rmtree(data, ignore_errors=True)
    shutil.rmtree(os.path.join(work, "edb"), ignore_errors=True)
    os.makedirs(data)
    print(f"workdir:  {work}")

    rpc_port = env_port("SMOKE_BTC_RPC_PORT", 38443)
    p2p_port = env_port("SMOKE_BTC_P2P_PORT", 38444)
    e_port = env_port("SMOKE_ELECTRUM_PORT", 60411)
    mon_port = env_port("SMOKE_MONITORING_PORT", 34224)

    node = Node(bindir, data, rpc_port, p2p_port)
    log = open(os.path.join(work, "electrs.log"), "a")

    def start_electrs(*extra):
        p = subprocess.Popen([binary, "--network", "regtest", "--daemon-dir", data,
                              "--daemon-rpc-addr", f"127.0.0.1:{rpc_port}",
                              "--db-dir", os.path.join(work, "edb"),
                              "--electrum-rpc-addr", f"127.0.0.1:{e_port}",
                              "--monitoring-addr", f"127.0.0.1:{mon_port}",
                              "--log-filters", "INFO", *extra],
                             stdout=log, stderr=log)
        for _ in range(120):
            try:
                c = Electrum(e_port)
                c.ok("server.ping")
                return p, c
            except Exception:
                time.sleep(0.5)
        p.kill()
        raise RuntimeError(f"electrs did not come up; see {work}/electrs.log")

    def wait_synced(c, height):
        for _ in range(120):
            if c.ok("blockchain.headers.subscribe")["height"] >= height:
                return
            time.sleep(0.5)
        raise RuntimeError("electrs did not reach height")

    electrs = None
    try:
        node.start()
        cli = node.cli
        cli("createwallet", "miner")
        cli("createwallet", "a")
        mine = cli("getnewaddress", wallet="miner")
        cli("generatetoaddress", 110, mine)
        A = cli("getnewaddress", "", "bech32", wallet="a")
        other = cli("getnewaddress", wallet="miner")
        # 18 funding txs to A over 6 blocks, then 6 spends from A back to the miner.
        for i in range(18):
            cli("sendtoaddress", A, "0.5", wallet="miner")
            if i % 3 == 2:
                cli("generatetoaddress", 1, mine)
        for i in range(6):
            cli("sendtoaddress", other, "0.3", wallet="a")
            cli("generatetoaddress", 1, mine)
        # A confirmed coinbase-free state; one unconfirmed payment to A stays in the mempool.
        mem_txid = cli("sendtoaddress", A, "0.1", wallet="miner")
        height = int(cli("getblockcount"))
        spk = json.loads(cli("getaddressinfo", A, wallet="a"))["scriptPubKey"]
        sh = scripthash(spk)

        electrs, c = start_electrs()
        wait_synced(c, height)
        time.sleep(2)

        # --- server.* ---
        check("server.version", isinstance(c.ok("server.version", ["blocknil-smoke", "1.4"]), list))
        check("server.ping", c.ok("server.ping") is None)
        feats = c.ok("server.features")
        check("server.features", isinstance(feats, dict), feats.get("server_version"))
        check("server.banner", isinstance(c.ok("server.banner"), str))

        # --- headers ---
        tip = c.ok("blockchain.headers.subscribe")
        check("blockchain.headers.subscribe", tip["height"] == height, tip["height"])
        hdr = c.ok("blockchain.block.header", [height])
        check("blockchain.block.header", hdr == tip["hex"])
        hs = c.ok("blockchain.block.headers", [height - 4, 5])
        check("blockchain.block.headers", hs["count"] == 5 and hs["hex"].endswith(tip["hex"]))

        # --- fees ---
        check("blockchain.estimatefee", isinstance(c.ok("blockchain.estimatefee", [6]), (int, float)))
        check("blockchain.relayfee", isinstance(c.ok("blockchain.relayfee"), (int, float)))
        check("mempool.get_fee_histogram", isinstance(c.ok("mempool.get_fee_histogram"), list))

        # --- scripthash ---
        status = c.ok("blockchain.scripthash.subscribe", [sh])
        check("blockchain.scripthash.subscribe", isinstance(status, str) and len(status) == 64)
        # Ground truth independent of electrs: every block's txs, with prevouts, from bitcoind.
        truth = set()
        for hgt in range(1, height + 1):
            blk = json.loads(cli("getblock", cli("getblockhash", hgt), 3))
            for tx in blk["tx"]:
                outs = any(o["scriptPubKey"]["hex"] == spk for o in tx["vout"])
                ins = any(i.get("prevout", {}).get("scriptPubKey", {}).get("hex") == spk
                          for i in tx["vin"])
                if outs or ins:
                    truth.add((tx["txid"], hgt))
        N = len(truth)
        print("ground truth confirmed entries:", N)
        hist = c.ok("blockchain.scripthash.get_history", [sh])
        conf = [h for h in hist if h["height"] > 0]
        unconf = [h for h in hist if h["height"] <= 0]
        check("blockchain.scripthash.get_history",
              set((h["tx_hash"], h["height"]) for h in conf) == truth
              and [u["tx_hash"] for u in unconf] == [mem_txid],
              f"confirmed={len(conf)} unconfirmed={len(unconf)}")
        bal = c.ok("blockchain.scripthash.get_balance", [sh])
        check("blockchain.scripthash.get_balance", bal["unconfirmed"] == 10_000_000, bal)
        lu = c.ok("blockchain.scripthash.listunspent", [sh])
        check("blockchain.scripthash.listunspent", len(lu) >= 1, len(lu))
        check("blockchain.scripthash.unsubscribe",
              c.ok("blockchain.scripthash.unsubscribe", [sh]) is True)
        gm = c.call("blockchain.scripthash.get_mempool", [sh])
        check("get_mempool still absent (as in 0.12.0 prod)", "error" in gm and gm["error"],
              gm.get("error"))

        # --- transactions ---
        t = conf[-1]
        raw = c.ok("blockchain.transaction.get", [t["tx_hash"], False])
        check("blockchain.transaction.get (raw)", isinstance(raw, str) and len(raw) > 100)
        raw_m = c.ok("blockchain.transaction.get", [mem_txid])
        check("blockchain.transaction.get (mempool tx)", isinstance(raw_m, str))
        vb = c.call("blockchain.transaction.get", [t["tx_hash"], True])
        check("blockchain.transaction.get verbose (informational)", True,
              "error" if vb.get("error") else "ok")
        merkle = c.ok("blockchain.transaction.get_merkle", [t["tx_hash"], t["height"]])
        check("blockchain.transaction.get_merkle", merkle["block_height"] == t["height"],
              merkle["pos"])
        idp = c.ok("blockchain.transaction.id_from_pos", [t["height"], merkle["pos"]])
        check("blockchain.transaction.id_from_pos", idp == t["tx_hash"])
        newtx = cli("createrawtransaction", "[]", json.dumps({other: 0.01}), wallet="miner")
        funded = json.loads(cli("fundrawtransaction", newtx, wallet="miner"))["hex"]
        signed = json.loads(cli("signrawtransactionwithwallet", funded, wallet="miner"))["hex"]
        bt = c.ok("blockchain.transaction.broadcast", [signed])
        check("blockchain.transaction.broadcast", len(bt) == 64, bt)

        # --- batch (BDK and ldk-node send JSON arrays) ---
        batch = c.send([{"jsonrpc": "2.0", "id": 900 + i,
                         "method": "blockchain.scripthash.get_history", "params": [sh]}
                        for i in range(3)]
                       + [{"jsonrpc": "2.0", "id": 990, "method": "blockchain.estimatefee",
                           "params": [2]},
                          {"jsonrpc": "2.0", "id": 991, "method": "mempool.get_fee_histogram",
                           "params": []}])
        check("batch request", isinstance(batch, list) and len(batch) == 5
              and all("result" in b for b in batch))

        # --- get_history_paged ---
        want = sorted(truth)

        def walk(limit, newest):
            seen, cursor, pages, cursors = [], None, 0, []
            while True:
                p = c.ok("blockchain.scripthash.get_history_paged", [sh, cursor, limit, newest])
                pages += 1
                seen += [(e["tx_hash"], e["height"]) for e in p["entries"]]
                if p["next_cursor"] is not None:
                    cursors.append(p["next_cursor"])
                if not p["more"]:
                    return seen, pages, cursors
                cursor = p["next_cursor"]
                if pages > 200:
                    raise RuntimeError("pager does not terminate")

        for limit in (1, 2, 5, 7, N - 1, N, N + 1, 100):
            for newest in (True, False):
                seen, pages, cursors = walk(limit, newest)
                heights = [h for _, h in seen]
                ordered = heights == sorted(heights, reverse=newest)
                monotonic = (cursors == sorted(cursors, reverse=newest)
                             and len(set(cursors)) == len(cursors))
                check(f"paged limit={limit} newest_first={newest}",
                      sorted(seen) == want and len(seen) == len(set(seen)) and ordered
                      and monotonic,
                      f"entries={len(seen)} pages={pages}")
        d = c.ok("blockchain.scripthash.get_history_paged", [sh])
        check("paged [sh] default (100, newest)", len(d["entries"]) == N and d["more"] is False)
        d = c.ok("blockchain.scripthash.get_history_paged", [sh, None])
        check("paged [sh, null]", len(d["entries"]) == N)
        d = c.ok("blockchain.scripthash.get_history_paged", [sh, None, 0])
        check("paged limit=0 clamps to 1", len(d["entries"]) == 1 and d["more"] is True)
        d = c.ok("blockchain.scripthash.get_history_paged", [sh, None, 5000])
        check("paged limit=5000 clamps (<=1000)", len(d["entries"]) == N)
        d = c.ok("blockchain.scripthash.get_history_paged", ["00" * 32, None, 10, True])
        check("paged unused scripthash",
              d == {"entries": [], "next_cursor": None, "more": False}, d)
        d = c.call("blockchain.scripthash.get_history_paged", [sh, "x", 5])
        check("paged bad cursor -> error", bool(d.get("error")), d.get("error"))
        d = c.call("blockchain.scripthash.get_history_paged", [sh, -1, 5])
        check("paged negative cursor -> error", bool(d.get("error")), d.get("error"))
        d = c.ok("blockchain.scripthash.get_history_paged", [sh, 0, 5, True])
        check("paged cursor=0 newest_first (edge)", True, d)
        d = c.ok("blockchain.scripthash.get_history_paged", [sh, 4294967295, 5, False])
        check("paged cursor=u32::MAX oldest_first (edge)", True, d)
        mem_in_paged = any(e["tx_hash"] == mem_txid
                           for e in c.ok("blockchain.scripthash.get_history_paged",
                                         [sh, None, 1000])["entries"])
        check("paged excludes mempool (documented: confirmed only)", not mem_in_paged)
        alive(electrs, "electrs after the paged sweep")
        c.close()
        electrs.terminate()
        electrs.wait(30)

        # --- index_lookup_limit: same db, at the boundary and below it ---
        #
        # A history of exactly N entries under --index-lookup-limit N is SERVED: the walk asks the
        # index for N + 1 candidates, gets N back, and stops. One below, the same history is
        # N = limit + 1 candidates and must be REFUSED -- with code 1 and this exact message,
        # because a client matches on it and a bare "some error" would let a regression through.
        electrs, c = start_electrs("--index-lookup-limit", str(N))
        wait_synced(c, height)
        r = c.call("blockchain.scripthash.get_history", [sh])
        served = r.get("result") or []
        conf_served = set((e["tx_hash"], e["height"]) for e in served if e["height"] > 0)
        check(f"index_lookup_limit={N} serves a history of exactly {N} entries (boundary)",
              r.get("error") is None and conf_served == truth,
              r.get("error") or f"confirmed={len(conf_served)} unconfirmed={len(served) - len(conf_served)}")
        alive(electrs, "electrs after serving the boundary")
        c.close()
        electrs.terminate()
        electrs.wait(30)

        electrs, c = start_electrs("--index-lookup-limit", str(N - 1))
        wait_synced(c, height)
        r = c.call("blockchain.scripthash.get_history", [sh])
        check(f"index_lookup_limit={N - 1} refuses {N} entries (limit + 1): code 1, exact message",
              refused(r, N - 1), r.get("error"))
        alive(electrs, "electrs after the boundary refusal")
        c.close()
        electrs.terminate()
        electrs.wait(30)

        # --- limit 5: far below the history, plus what has to keep working underneath it ---
        electrs, c = start_electrs("--index-lookup-limit", "5")
        wait_synced(c, height)
        r = c.call("blockchain.scripthash.get_history", [sh])
        check(f"index_lookup_limit=5 refuses get_history ({N} entries): code 1, exact message",
              refused(r, 5), r.get("error"))
        c = Electrum(e_port)
        r = c.call("blockchain.scripthash.subscribe", [sh])
        check("index_lookup_limit=5 refuses subscribe: code 1, exact message", refused(r, 5),
              r.get("error"))
        c = Electrum(e_port)
        check("server answers after refusal", c.ok("server.ping") is None)
        alive(electrs, "electrs after the refusal")
        d = c.ok("blockchain.scripthash.get_history_paged", [sh, None, 100])
        check("paged ignores index_lookup_limit", len(d["entries"]) == N)
        alive(node.proc, "bitcoind")
        small = cli("getnewaddress", wallet="miner")
        cli("sendtoaddress", small, "0.2", wallet="miner")
        cli("generatetoaddress", 1, mine)
        wait_synced(c, height + 1)
        ssh_ = scripthash(json.loads(cli("getaddressinfo", small, wallet="miner"))["scriptPubKey"])
        r = c.ok("blockchain.scripthash.get_history", [ssh_])
        check("index_lookup_limit=5 serves a small address", len(r) == 1, r)
    finally:
        if electrs:
            electrs.terminate()
            try:
                electrs.wait(30)
            except Exception:
                electrs.kill()
        try:
            node.stop()
        except Exception:
            pass
        log.close()
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)

    fails = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(fails)}/{len(results)} passed")
    if fails:
        print("failed: " + ", ".join(name for name, _, _ in fails))
    if args.keep:
        print(f"work dir kept: {work}")
    sys.exit(1 if fails else 0)


def find_bitcoind(explicit):
    candidates = []
    if explicit:
        candidates.append(explicit)
    else:
        roots = [os.environ.get("BITCOIND_DIR"),
                 os.path.expanduser("~/core-releases/32.1")]
        for root in roots:
            if not root:
                continue
            # ops layout: <root>/bitcoin-32.1/bin; tarball layout: <root>/<triple>/bitcoin-32.1/bin
            candidates.append(os.path.join(root, "bitcoin-32.1", "bin"))
            candidates.append(os.path.join(root, "bin"))
            candidates += sorted(glob.glob(os.path.join(root, "*", "bitcoin-32.1", "bin")))
    for cand in candidates:
        if (os.access(os.path.join(cand, "bitcoind"), os.X_OK)
                and os.access(os.path.join(cand, "bitcoin-cli"), os.X_OK)):
            return cand
    sys.exit("regtest_smoke: no bitcoind + bitcoin-cli pair found "
             f"({'searched ' + ', '.join(candidates)}); unpack Bitcoin Core 32.1 and pass "
             "--bitcoind-dir, or set BITCOIND_DIR")


if __name__ == "__main__":
    main()
