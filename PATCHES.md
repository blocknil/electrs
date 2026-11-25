# What this fork changes, and why

Base: **electrs 0.12.1** exactly as published on crates.io (`cargo install electrs
--version 0.12.1`), upstream commit `d0aa1c30b6f5285f01d89ff6c85e8a6046b226ee`.
`vendor/bindex` is **bindex 0.1.3**, also exactly as published, plus the changes described
below. Both are vendored so that a checkout of this repository builds the same code the
deployed binary runs, rather than depending on a patch someone applied by hand.

The 0.12.0 line carried the same code changes with `vendor/bindex` at 0.1.2: 0.12.1
changed only dependency versions and documentation, so the patches are the same and only
the vendored bindex version differs.

Two problems motivated the fork. Both are about the same address history path.

## 1. `index_lookup_limit` was dead code

`Config::index_lookup_limit` is parsed from the config file and then read **nowhere**: the
name appears only in `config.rs`. Its own help text claims it caps "the number of
transactions to lookup before server to get stuck", so setting it looks like protection
that is not there.

Without it, `blockchain.scripthash.get_history` on a large address walks the entire
history. Measured in production on a large address:

    index reads    859 MB
    RSS           1.05 GB
    threads       47, process alive and idle-looking
    effect        server.ping, headers.subscribe and estimatefee ALL time out;
                  every other client is starved until electrs is restarted

The process never crashes — it stops answering, which is why this presents as "some
addresses fail" rather than as a server that died.

**Change** (`src/tracker.rs`, `src/status.rs`): the value is threaded into the history
walk, and the walk decides on the index's *candidates* before it reads a single
transaction. It asks the index for at most `limit + 1` of them in one seek (`bindex`'s
`script_hash_page`, oldest first) and refuses if it gets more than `limit` back, so a
refused address costs about one page of index reads and no block reads at all. The
refusal is a normal JSON-RPC error, so a client can tell it apart from a timeout.

| | before | after |
|---|---|---|
| over-limit address | never returns; everyone starved | refused in ~1 ms on regtest, flat in `limit` |
| other clients during it | starved for minutes | no measurable wait |

The first version of this patch (0.12.0 line) counted entries *while walking them*: it
refused on the first entry past the limit, but only after reading and decoding `limit`
transaction bodies, so a refusal still cost 1.5-3.8 s in production and held up other
requests. The walk now stops at the limit instead. On regtest, with one address holding
5,000 confirmed entries and both binaries built from this tree on the same machine
(Bitcoin Core 32.1, local REST, `release`):

| `index_lookup_limit` | before (0.12.0 walk) | after |
|---|---|---|
| 200 | 13.9 ms (12.8-23.3 ms, n=5) | 0.9 ms |
| 2000 | 120 ms (117-122 ms, n=5) | 1.0 ms |
| `server.ping` during a refusal | up to 32.8 ms (limit 2000) | at most 1.5 ms (limit 200) |

The after column is flat in `limit`, which is the point: the cost is one page of index
reads, not one transaction body per entry. Five unit tests in `src/status.rs` pin the
budget (refusal at `limit + 1` with zero transactions read, a history of exactly the
limit, a limit of zero, paging without a limit, resuming from a txnum), and
`contrib/regtest_smoke.py` checks the same behaviour end to end against bitcoind
(`index_lookup_limit=5 refuses get_history`, `server answers after refusal`,
`index_lookup_limit=5 serves a small address`).

## 2. History had no pagination

Electrum has no paging: `get_history` returns the whole history or nothing. That leaves a
large address with only two bad options — an enormous response, or (with change 1) a flat
refusal. For a 2.9M-transaction address the first option is not survivable and the second
means the address cannot be browsed at all.

**Change**: a new method, additive and deliberately non-standard.

    blockchain.scripthash.get_history_paged

    params   [scripthash]
             [scripthash, cursor]
             [scripthash, cursor, limit]
             [scripthash, cursor, limit, newest_first]
             cursor        opaque u32; the `next_cursor` of the previous page
             limit         1..1000, default 100
             newest_first  default true
    result   {"entries": [{"tx_hash": "...", "height": 123}, ...],
              "next_cursor": 456,
              "more": true}

Why it is cheap: the script-hash index is keyed `(prefix ‖ txnum)` with `txnum`
big-endian, so lexicographic order is chronological order. A page is therefore a *seek*
plus `limit` body reads — cost independent of the size of the history — while
`locations_by_scripthash` collects every matching entry before the caller can use any of
them.

Two details that are not obvious:

- **Candidates must be post-filtered.** The index keys a script hash by its first **8
  bytes** only (`Prefix::LEN`), and bindex documents that lookups "require
  post-filtering". A page therefore re-checks each candidate: an output must pay the
  script, or an input must spend an output that does. The input side needs the prevout
  script, which a transaction does not carry, so it resolves the prevout through the
  index — and only when the output side did not already match, so a funding transaction
  costs no extra lookups.
- **The cursor advances over candidates, not over the entries kept.** A page whose
  candidates were all filtered out still moves the caller forward, so a prefix shared with
  another script cannot be paged forever.

`blockchain.scripthash.get_history` is untouched, so wallets are unaffected: they do not
know this method and cannot be broken by it.

## Changes in `vendor/bindex`

    src/index/mod.rs   TxNum::{MAX, from_u32, to_u32}         (build a seek position)
    src/db.rs          DB::scan_by_script_hash_page           (limited, directional scan)
    src/chain.rs       IndexedChain::script_hash_page         (page of (txnum, height))
                       IndexedChain::location_by_txnum        (body lookup by position)
                       ScriptHashPageEntry                    (the entry type)

`scan_by_script_hash_page` is the same seek as the existing
`scan_by_script_hash`, with a direction and a `limit` that stops the RocksDB iterator
early. Change 1's walk uses it, so a refusal stops after `limit + 1` candidates instead
of collecting millions of `TxNum`s first.

## Building

    ./build.sh

or the same thing by hand:

    CXXFLAGS="-include cstdint" cargo build --locked --release

The flag is needed because the bundled RocksDB 9.10.0 does not `#include <cstdint>`
itself: libstdc++ up to GCC 14 pulled it in transitively, GCC 15 and newer do not, and
every translation unit fails with `'uint64_t' has not been declared`. Upstream documents
the same workaround in `doc/install.md` as of 0.12.1. `CXXFLAGS` must be exported rather
than passed inline, because cc-rs reads it per spawned compiler.

`Cargo.lock` is committed and kept in sync with the `[patch.crates-io]` entry, so
`--locked` builds work.

## Testing

    python3 contrib/regtest_smoke.py target/release/electrs

starts a throwaway Bitcoin Core 32.1 regtest node and that binary, and calls every Electrum
method our clients use, `blockchain.scripthash.get_history_paged` in every shape, and the
`index_lookup_limit` refusal. Its expectations come from bitcoind (`getblock ... 3`), not from
electrs, so the two cannot agree on something wrong. Pass `--bitcoind-dir` if Core 32.1 is not
at `~/core-releases/32.1`.

## Silent Payments block index

`IndexedChain::open_with_silent_payments` is the opt-in storage entry point.
Use a separate database directory. The paged index uses the
`silent_payments_v3` column family. Databases built without Silent Payments
or with the earlier experimental format require a fresh index directory. Set
`--index-silent-payments` to enable it on a scratch or staging index.

The index uses the block and spent outputs already fetched from Core. A
block's metadata and 32-row shards share its height and hash as their key
prefix. Rows include all eligible transactions and their Taproot outputs,
including spent outputs and small amounts. A zero row count distinguishes
an indexed empty block. RocksDB writes metadata, shards and canonical
block records in one batch. Rollback deletes the metadata and shard range
for the old block in the same batch as its canonical records.

Run the vendored bindex suite against our pinned Bitcoin Core 32.1 release
using `BITCOIND_EXE=/absolute/path/to/bitcoind cargo test --locked
--manifest-path vendor/bindex/Cargo.toml`. The Silent Payments regtest compares
all rows of the fixture block with SPDK, reopens the database, replaces the
block at the same height, and verifies the old row stays absent after
restart and branch extension.

### Experimental Electrum endpoint

`server.features` advertises the version and indexed range on an ordinary
connection. Fetch pages on a dedicated connection to the same port. Its
first request selects the scan protocol, and each request is standalone.
A minimal parameter object follows.

```json
{"height":106,"end_height":107,"end_hash":"<hash of block 107>","offset":0}
```

Use JSON-RPC method `blockchain.silent_payments.get`. A page returns the
block height and hash, echoed anchor, entry offset, entries, and `next`
as `[height, offset]` or null. Each entry has the txid, compressed public
tweak and outputs as `[vout, x_only_key_hex, sats]`. An empty indexed block
returns an empty entry list. Unknown or unindexed blocks return -32022.
A changed anchor returns -32021. A saturated or expired queue returns
retryable -32020. Restart at a fresh anchor on a chain change.

The handler checks the anchor and block records from one RocksDB snapshot.
Each request reads at most nine shards to serve up to 256 entries and 4 MiB
on the wire, including at an unaligned offset. There are two scan workers
and eight queue slots. With the endpoint enabled, up to 16 first requests
are classified concurrently under a three-second deadline. The established
connection budgets are 16 scan sessions and 256 ordinary Electrum sessions.
These are pilot limits. Check reconnect traffic and wallet sync latency
under representative load before rollout. A requested range can
cover at most 128 blocks. Responses and writes run outside the normal
Electrum event loop. When serving Electrum, the enabled index syncs in
32-block batches. Offline builds with `--disable-electrum-rpc` use
256-block batches to reduce repeated WAL syncs and flushes.

Run `contrib/regtest_smoke.py --with-silent-payments <electrs-binary>`
for the enabled endpoint, anchor and reorg checks and a local concurrent
scan smoke test. Repeat without the flag for the disabled path. Mainnet
backfill, device profiling and release load gates need separate validation.

### Paired local benchmark

Build the patched baseline and candidate in separate Cargo target directories
with the same toolchain and release profile. Run the benchmark on the pinned
Bitcoin Core 32.1 regtest release.

```sh
python3 -B contrib/regtest_sp_bench.py /path/to/baseline/electrs /path/to/candidate/electrs \
  --bitcoind-dir /path/to/bitcoin-32.1/bin
```

The harness generates one disposable chain, gives every run a fresh database
and rotates the order of the three variants. It measures backfill wall time,
electrs CPU and peak RSS, allocated database size, and steady-state latency for
a bundle of ping, header, paged history and Merkle proof calls. During the
loaded run, concurrent clients in a separate load-generator process scan
over the fixture's transaction-bearing blocks. Ordinary RPC result fingerprints
must match across all three variants.
The JSON report includes individual trials and medians.

On an arm64 Mac with 12 logical CPUs, the fixture used 1,661 blocks, 1,536
eligible transactions, and 3,072 Taproot outputs. Three rotated release-build
trials on the same Core 32.1 chain gave these medians.

| Variant | Backfill wall | electrs CPU | Peak RSS | Final database | Peak database |
| --- | ---: | ---: | ---: | ---: | ---: |
| Patched baseline `dfd1aad` | 0.231 s | 0.374 s | 20.3 MB | 0.532 MB | 0.541 MB |
| Candidate, tweaks disabled | 0.235 s | 0.393 s | 20.6 MB | 0.532 MB | 0.532 MB |
| Candidate, tweaks enabled | 0.325 s | 0.529 s | 23.5 MB | 0.983 MB | 1.012 MB |

The earlier enabled build took 0.832 s wall time, 1.074 s CPU and 3.006 MB
peak database space on the same fixture. The tuned build groups more
blocks per durable write during offline backfill and avoids pre-serializing
every response entry just to calculate its maximum size.

At 12 sustained scan clients, the tuned build's ordinary four-RPC bundle
went from 1.84 to 3.40 ms p95 and from 2.39 to 3.62 ms p99. Across the
three loaded trials, the median run served 2,450 pages and returned
1,458 retryable busy responses. Ordinary RPCs completed successfully and
result fingerprints matched. The latency increase exceeds the proposed
+10% p95 and +20% p99 budgets in this local test.

The fixture has many coinbase-only blocks and repeats recipient scripts.
Its storage and compaction measurements are only for local comparison.
Mainnet backfill, cold storage reads, real wallet and LDK workloads, and
staging performance remain open.

### Scan admission and real wallet sync

The scan service admits at most 128 block pages per second by default across
all connections. Operators can change the experimental ceiling with
`--silent-payments-scan-rps` in the range 1 to 4096. An excess request receives
`-32020` and `error.data.retry_after_ms`. The server backs off that connection
before reading its next request. The existing queue and response size limits
still apply. The candidate also returns a readiness error for header
subscriptions before genesis is indexed.

The sharded candidate passed 20 electrs tests, 9 vendored bindex tests
and the Core 32.1 smoke suite with scanning enabled (71 checks) and
disabled (61 checks). A local replay of the 1,661-block fixture on
2026-11-25 used 120 sync samples per mode and targeted 105 scan pages
per second. The two less contended trials served about 88 pages per
second. Staging still needs a representative sync latency run and an
index backfill and disk-size measurement.

Build `core/examples/electrum_scan_probe.rs` from wallet source and pass its
binary to the local driver. It uses fresh regtest wallets and the production
`WalletCore::sync_onchain` and LDK `Node::sync_wallets` entry points.

```sh
python3 -B contrib/regtest_sp_wallet_bench.py \
  /path/to/baseline/electrs /path/to/candidate/electrs /path/to/electrum_scan_probe \
  --bitcoind-dir /path/to/bitcoin-32.1/bin
```

Three rotated Core 32.1 trials used the patched electrs baseline `dfd1aad`,
wallet source `2e64893`, and 12 concurrent scanning connections over 24
blocks with 64 eligible transactions each. Each mode measured one initial
sync and 40 follow-up syncs per trial. Every sample preserved both confirmed
test balances.

| Admission ceiling | Blocknil wallet p95 idle / loaded | LDK p95 idle / loaded |
| ---: | ---: | ---: |
| 128 pages/s | 0.83 / 0.83 ms | 3.30 / 3.20 ms |
| 256 pages/s | 0.88 / 1.00 ms | 3.50 / 3.35 ms |
| 1,024 pages/s | 0.76 / 0.77 ms | 2.88 / 3.03 ms |

At the selected 128 ceiling, competing clients delivered about 22–31
successful pages/s in these trials. The rest received retryable busy
responses. A separate paced run used 120 follow-up syncs per trial and
served about 88 pages/s with no busy responses. At that rate, the loaded
wallet and LDK p95 were 0.69 and 2.70 ms. LDK p99 was 3.97 ms, versus
3.55 ms with the candidate idle and 3.11 ms on the patched baseline.
The baseline-relative p99 exceeds the proposed 20% budget. These
measurements cover on-chain and LDK wallet sync to a fixed chain tip.
The full admission ceiling, active Lightning channel traffic under
scan load, and mainnet backfill and compaction still need validation.
