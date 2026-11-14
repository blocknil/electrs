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
| `server.ping` during a refusal, limit 2000 | up to 32.8 ms | 1.5 ms |

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
