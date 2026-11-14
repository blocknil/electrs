use crate::bitcoin::{
    consensus::Decodable,
    hashes::{sha256, Hash, HashEngine},
    Amount, BlockHash, OutPoint, SignedAmount, Transaction, Txid,
};
use anyhow::Result;
use bindex::{IndexedChain, ScriptHash};
use serde::ser::{Serialize, Serializer};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;

use crate::{mempool::Mempool, types::StatusHash};

/// Given a scripthash, store relevant inputs and outputs of a specific transaction
struct TxEntry {
    txid: Txid,
    outputs: Vec<TxOutput>, // relevant funded outputs and their amounts
    spent: Vec<OutPoint>,   // relevant spent outpoints
}

// Funded outputs of a transaction
struct TxOutput {
    index: u32,
    value: Amount,
}

impl TxEntry {
    fn new(txid: Txid) -> Self {
        Self {
            txid,
            outputs: Vec::new(),
            spent: Vec::new(),
        }
    }

    /// Relevant (scripthash-wise) funded outpoints
    fn funding_outpoints(&self) -> impl Iterator<Item = OutPoint> + '_ {
        make_outpoints(self.txid, &self.outputs)
    }
}

// Confirmation height of a transaction or its mempool state:
// https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-history
// https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-mempool
enum Height {
    Confirmed { height: usize },
    Unconfirmed { has_unconfirmed_inputs: bool },
}

impl Height {
    fn as_i64(&self) -> i64 {
        match self {
            Self::Confirmed { height } => i64::try_from(*height).unwrap(),
            Self::Unconfirmed {
                has_unconfirmed_inputs: true,
            } => -1,
            Self::Unconfirmed {
                has_unconfirmed_inputs: false,
            } => 0,
        }
    }
}

impl Serialize for Height {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.as_i64())
    }
}

impl std::fmt::Display for Height {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_i64().fmt(f)
    }
}

// A single history entry:
// https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-history
// https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-mempool
#[derive(Serialize)]
pub(crate) struct HistoryEntry {
    #[serde(rename = "tx_hash")]
    txid: Txid,
    height: Height,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::bitcoin::amount::serde::as_sat::opt"
    )]
    fee: Option<Amount>,
}

impl HistoryEntry {
    // Hash to compute ScriptHash status, as defined here:
    // https://electrum-protocol.readthedocs.io/en/latest/protocol-basics.html#status
    fn hash(&self, engine: &mut sha256::HashEngine) {
        let s = format!("{}:{}:", self.txid, self.height);
        engine.input(s.as_bytes());
    }

    fn confirmed(txid: Txid, height: usize) -> Self {
        Self {
            txid,
            height: Height::Confirmed { height },
            fee: None,
        }
    }

    fn unconfirmed(txid: Txid, has_unconfirmed_inputs: bool, fee: Amount) -> Self {
        Self {
            txid,
            height: Height::Unconfirmed {
                has_unconfirmed_inputs,
            },
            fee: Some(fee),
        }
    }
}

/// Make sure blocks are sorted by ascending height.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BlockKey(usize, BlockHash);

impl From<bindex::Location<'_>> for BlockKey {
    fn from(location: bindex::Location<'_>) -> Self {
        Self(location.block_height(), location.block_hash())
    }
}

/// Candidates read per index seek when no `index_lookup_limit` applies.
///
/// Large enough that an ordinary address is a single seek, small enough that a history of
/// millions is never collected into one `Vec`.
const HISTORY_SCAN_PAGE: usize = 4_096;

/// Walk a script hash's history candidates in chronological order, one bounded seek at a
/// time, and refuse as soon as the index holds more than `lookup_limit` of them.
///
/// This is the `index_lookup_limit` enforcement. It asks the index for at most `limit + 1`
/// candidates in total and decides on the page it gets back, so a refused address costs
/// about one page of key reads — the same as one page of
/// `blockchain.scripthash.get_history_paged` — instead of one transaction body read per
/// entry up to the limit. The check happens before `visit` is called for any candidate in
/// that page, so a refusal reads no transaction at all.
///
/// `fetch_page(from, want)` must return at most `want` candidates, in ascending order,
/// starting at `from` inclusive (the same seek semantics as the index). `visit` is called
/// once per candidate, in that order.
///
/// Split out from `sync_confirmed` so the budget can be tested without an index.
fn walk_candidates<F, V>(
    lookup_limit: Option<usize>,
    from: u32,
    mut fetch_page: F,
    mut visit: V,
) -> Result<usize>
where
    F: FnMut(u32, usize) -> Result<Vec<u32>>,
    V: FnMut(u32) -> Result<()>,
{
    let mut from = from;
    let mut seen = 0usize;
    loop {
        let want = match lookup_limit {
            // Never ask for more than `limit + 1`: the extra candidate is the proof that
            // the history does not fit, and there is no reason to read past it.
            Some(limit) => limit.saturating_add(1).saturating_sub(seen),
            None => HISTORY_SCAN_PAGE,
        };
        if want == 0 {
            break;
        }
        let page = fetch_page(from, want)?;
        if page.is_empty() {
            break;
        }
        if let Some(limit) = lookup_limit {
            if seen + page.len() > limit {
                anyhow::bail!(
                    "index_lookup_limit: script hash history has more than {limit} \
                     transactions; refusing to build it"
                );
            }
        }
        for &txnum in &page {
            visit(txnum)?;
            seen += 1;
        }
        // The index seek is inclusive, so continue past the last candidate examined.
        match page.last().and_then(|txnum| txnum.checked_add(1)) {
            Some(next) => from = next,
            None => break,
        }
        if page.len() < want {
            break;
        }
    }
    Ok(seen)
}

/// ScriptHash subscription status
pub struct ScriptHashStatus {
    scripthash: ScriptHash, // specific scripthash to be queried
    confirmed: BTreeMap<BlockKey, Vec<TxEntry>>, // confirmed entries (chronologically ordered), partitioned per block
    mempool: Vec<TxEntry>,                       // unconfirmed entries
    history: Vec<HistoryEntry>,                  // computed from confirmed and mempool entries
    statushash: Option<StatusHash>,              // computed from history
}

/// Specific scripthash balance
/// https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-balance
#[derive(Default, Eq, PartialEq, Serialize)]
pub(crate) struct Balance {
    #[serde(with = "crate::bitcoin::amount::serde::as_sat", rename = "confirmed")]
    confirmed_balance: Amount,
    #[serde(with = "crate::bitcoin::amount::serde::as_sat", rename = "unconfirmed")]
    mempool_delta: SignedAmount,
}

/// A single unspent transaction output entry
/// https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-listunspent
#[derive(Serialize)]
pub(crate) struct UnspentEntry {
    height: usize, // 0 = mempool entry
    tx_hash: Txid,
    tx_pos: u32,
    #[serde(with = "crate::bitcoin::amount::serde::as_sat")]
    value: Amount,
}

#[derive(Default)]
struct Unspent {
    // mapping an outpoint to its value & confirmation height
    outpoints: HashMap<OutPoint, (Amount, usize)>,
    balance: Balance,
}

impl Unspent {
    fn build(status: &ScriptHashStatus) -> Self {
        let mut unspent = Unspent::default();
        // First, add all relevant entries' funding outputs to the outpoints' map
        status
            .confirmed_height_entries()
            .for_each(|(height, entries)| entries.iter().for_each(|e| unspent.insert(e, height)));
        // Then, remove spent outpoints from the map
        status.confirmed_entries().for_each(|e| unspent.remove(e));

        unspent.balance.confirmed_balance = unspent.balance();
        // Now, do the same over the mempool (first add funding outputs, and then remove spent ones)
        status.mempool.iter().for_each(|e| unspent.insert(e, 0)); // mempool height = 0
        status.mempool.iter().for_each(|e| unspent.remove(e));

        let total_balance = unspent.balance().to_signed().unwrap();
        let confirmed_balance = unspent.balance.confirmed_balance.to_signed().unwrap();
        unspent.balance.mempool_delta = total_balance - confirmed_balance;
        unspent
    }

    fn into_entries(self) -> Vec<UnspentEntry> {
        self.outpoints
            .into_iter()
            .map(|(outpoint, (value, height))| UnspentEntry {
                height,
                tx_hash: outpoint.txid,
                tx_pos: outpoint.vout,
                value,
            })
            .collect()
    }

    /// Total amount of unspent outputs
    fn balance(&self) -> Amount {
        self.outpoints
            .values()
            .fold(Amount::default(), |acc, v| acc + v.0)
    }

    fn insert(&mut self, entry: &TxEntry, height: usize) {
        for output in &entry.outputs {
            let outpoint = OutPoint {
                txid: entry.txid,
                vout: output.index,
            };
            self.outpoints.insert(outpoint, (output.value, height));
        }
    }

    fn remove(&mut self, entry: &TxEntry) {
        for spent in &entry.spent {
            self.outpoints.remove(spent);
        }
    }
}

impl ScriptHashStatus {
    /// Return non-synced (empty) status for a given script hash.
    pub fn new(scripthash: ScriptHash) -> Self {
        Self {
            scripthash,
            confirmed: BTreeMap::new(),
            mempool: Vec::new(),
            history: Vec::new(),
            statushash: None,
        }
    }

    /// Iterate through confirmed TxEntries with their corresponding block heights.
    /// Skip entries from stale blocks.
    fn confirmed_height_entries<'a>(&'a self) -> impl Iterator<Item = (usize, &'a [TxEntry])> + 'a {
        self.confirmed
            .iter()
            .map(move |(blockid, entries)| (blockid.0, &entries[..]))
    }

    /// Iterate through confirmed TxEntries.
    fn confirmed_entries<'a>(&'a self) -> impl Iterator<Item = &'a TxEntry> + 'a {
        self.confirmed_height_entries()
            .flat_map(|(_height, entries)| entries)
    }

    /// Collect all funded and confirmed outpoints (as a set).
    fn confirmed_outpoints(&self) -> HashSet<OutPoint> {
        self.confirmed_entries()
            .flat_map(TxEntry::funding_outpoints)
            .collect()
    }

    /// Collect unspent transaction entries
    pub(crate) fn get_unspent(&self) -> Vec<UnspentEntry> {
        Unspent::build(self).into_entries()
    }

    /// Collect unspent transaction balance
    pub(crate) fn get_balance(&self) -> Balance {
        Unspent::build(self).balance
    }

    /// Collect transaction history entries
    pub(crate) fn get_history(&self) -> &[HistoryEntry] {
        &self.history
    }

    /// Collect all confirmed history entries (in block order).
    fn get_confirmed_history(&self) -> Vec<HistoryEntry> {
        self.confirmed_height_entries()
            .collect::<BTreeMap<usize, &[TxEntry]>>()
            .into_iter()
            .flat_map(|(height, entries)| {
                entries
                    .iter()
                    .map(move |e| HistoryEntry::confirmed(e.txid, height))
            })
            .collect()
    }

    /// Collect all mempool history entries (keeping transactions with unconfirmed parents last).
    fn get_mempool_history(&self, mempool: &Mempool) -> Vec<HistoryEntry> {
        let mut entries = self
            .mempool
            .iter()
            .filter_map(|e| mempool.get(&e.txid))
            .collect::<Vec<_>>();
        entries.sort_by_key(|e| (e.has_unconfirmed_inputs, e.txid));
        entries
            .into_iter()
            .map(|e| HistoryEntry::unconfirmed(e.txid, e.has_unconfirmed_inputs, e.fee))
            .collect()
    }

    /// Get funding and spending entries from new blocks.
    /// Also cache relevant transactions and their merkle proofs.
    fn sync_confirmed(
        &mut self,
        index: &IndexedChain,
        lookup_limit: Option<usize>,
    ) -> Result<HashSet<OutPoint>> {
        let headers = index.headers();
        let mut latest_header = None;
        // Drop entries from stale blocks
        while let Some(entry) = self.confirmed.last_entry() {
            let BlockKey(height, hash) = entry.key();
            match headers.get_header(*hash, *height) {
                Ok(header) => {
                    latest_header = Some(header);
                    break;
                }
                Err(err) => {
                    warn!("drop reorged block: {}", err);
                    entry.remove();
                    continue;
                }
            }
        }
        // Recompute all funded outpoints
        let mut outpoints = self.confirmed_outpoints();
        // Process transactions in chronological order, one bounded index seek at a time.
        //
        // `lookup_limit` is enforced by `walk_candidates` before the first transaction is
        // read: the walk asks the index for at most `limit + 1` candidates, so an address
        // that exceeds the limit costs about one page of key reads and nothing else — no
        // body fetch, no decode, no merkle proof. Refusing is the entire point: the
        // alternative is materialising a multi-hundred-MB history that starves every other
        // client on this port.
        let scripthash = self.scripthash;
        let from = latest_header
            .map(|header| header.next_txnum().to_u32())
            .unwrap_or_default();
        let outpoints = &mut outpoints;
        walk_candidates(
            lookup_limit,
            from,
            |from, want| {
                Ok(index
                    .script_hash_page(&scripthash, from, want, false)?
                    .into_iter()
                    .map(|entry| entry.txnum)
                    .collect())
            },
            |txnum| {
                let location = index.location_by_txnum(txnum);
                let tx_bytes = index.get_tx_bytes(&location)?;
                let tx = Transaction::consensus_decode_from_finite_reader(&mut &tx_bytes[..])?;

                // Check if this transaction has relevant inputs/outputs:
                let spent = filter_inputs(&tx, outpoints);
                let outputs = filter_outputs(&tx, scripthash);
                if spent.is_empty() && outputs.is_empty() {
                    return Ok(());
                }

                // Build new TxEntry and add new outpoints (for next transactions)
                let mut tx_entry = TxEntry::new(tx.compute_txid());
                tx_entry.spent = spent;
                tx_entry.outputs = outputs;
                outpoints.extend(tx_entry.funding_outpoints());

                // Add new per-block entry (if needed)
                self.confirmed
                    .entry(BlockKey::from(location))
                    .or_default()
                    .push(tx_entry);
                Ok(())
            },
        )?;

        // `outpoints` is borrowed mutably for the walk, so it is moved out instead of cloned:
        // the caller wants the set itself, and copying it once more is exactly the kind of cost
        // the walk exists to bound.
        Ok(std::mem::take(outpoints))
    }

    /// Get funding and spending entries from current mempool.
    /// Also cache relevant transactions.
    fn sync_mempool(&self, mempool: &Mempool, mut outpoints: HashSet<OutPoint>) -> Vec<TxEntry> {
        let mut result = HashMap::<Txid, TxEntry>::new();
        // extract relevant funding transactions
        for entry in mempool.filter_by_funding(&self.scripthash) {
            let funding_outputs = filter_outputs(&entry.tx, self.scripthash);
            assert!(!funding_outputs.is_empty());
            // store funded outpoints (to check for spending later)
            outpoints.extend(make_outpoints(entry.txid, &funding_outputs));
            result
                .entry(entry.txid) // the transaction may already exist
                .or_insert_with(|| TxEntry::new(entry.txid))
                .outputs = funding_outputs;
        }
        for entry in outpoints
            .iter()
            .flat_map(|outpoint| mempool.filter_by_spending(outpoint))
        {
            let spent_outpoints = filter_inputs(&entry.tx, &outpoints);
            assert!(!spent_outpoints.is_empty());
            result
                .entry(entry.txid) // the transaction may already exist
                .or_insert_with(|| TxEntry::new(entry.txid))
                .spent = spent_outpoints;
        }
        result.into_values().collect()
    }

    /// Sync with currently confirmed txs and mempool, downloading non-cached transactions via REST API.
    /// After a successful sync, script-hash status is updated.
    pub(crate) fn sync(
        &mut self,
        index: &IndexedChain,
        mempool: &Mempool,
        lookup_limit: Option<usize>,
    ) -> Result<()> {
        let outpoints = self.sync_confirmed(index, lookup_limit)?;
        if !self.confirmed.is_empty() {
            debug!(
                "{} transactions from {} blocks",
                self.confirmed.values().map(Vec::len).sum::<usize>(),
                self.confirmed.len()
            );
        }
        self.mempool = self.sync_mempool(mempool, outpoints);
        if !self.mempool.is_empty() {
            debug!("{} mempool transactions", self.mempool.len());
        }
        // update history entries and status hash
        self.history.clear();
        self.history.extend(self.get_confirmed_history());
        self.history.extend(self.get_mempool_history(mempool));

        self.statushash = compute_status_hash(&self.history);
        Ok(())
    }

    /// Get current status hash.
    pub fn statushash(&self) -> Option<StatusHash> {
        self.statushash
    }
}

fn make_outpoints(txid: Txid, outputs: &[TxOutput]) -> impl Iterator<Item = OutPoint> + '_ {
    outputs
        .iter()
        .map(move |out| OutPoint::new(txid, out.index))
}

/// Collect outputs that fund given scripthash.
fn filter_outputs(tx: &Transaction, scripthash: ScriptHash) -> Vec<TxOutput> {
    let outputs = tx.output.iter().zip(0u32..);
    outputs
        .filter(|&(txo, _vout)| ScriptHash::new(&txo.script_pubkey) == scripthash)
        .map(|(txo, vout)| TxOutput {
            index: vout,
            value: txo.value,
        })
        .collect()
}

/// Collect inputs that spend one of the specified outpoints.
fn filter_inputs(tx: &Transaction, outpoints: &HashSet<OutPoint>) -> Vec<OutPoint> {
    let inputs = tx.input.iter();
    inputs
        .filter(|&txi| outpoints.contains(&txi.previous_output))
        .map(|txi| txi.previous_output)
        .collect()
}

// See https://electrum-protocol.readthedocs.io/en/latest/protocol-basics.html#status for details
fn compute_status_hash(history: &[HistoryEntry]) -> Option<StatusHash> {
    if history.is_empty() {
        return None;
    }
    let mut engine = StatusHash::engine();
    for entry in history {
        entry.hash(&mut engine);
    }
    Some(StatusHash::from_engine(engine))
}

#[cfg(test)]
mod tests {
    use super::{walk_candidates, HistoryEntry, HISTORY_SCAN_PAGE};
    use crate::bitcoin::Amount;
    use serde_json::json;

    /// A stand-in for the index's script-hash scan: `total` candidates numbered `0..total`,
    /// seeked inclusively at `from` and cut at `want`, exactly like `scan_by_script_hash_page`.
    ///
    /// Every call is recorded, so a test can assert how many candidates the walk asked the
    /// index to read — that is the cost the limit is supposed to bound.
    fn fake_scan(
        total: u32,
        seeks: &mut Vec<(u32, usize)>,
    ) -> impl FnMut(u32, usize) -> anyhow::Result<Vec<u32>> + '_ {
        move |from, want| {
            seeks.push((from, want));
            Ok((from..total).take(want).collect())
        }
    }

    /// The fix: an over-limit history is refused after reading `limit + 1` candidates and
    /// looking at no transaction at all. Before, the walk collected every candidate and
    /// then read `limit` transaction bodies before it could refuse.
    #[test]
    fn test_walk_refuses_at_limit_plus_one_without_reading_a_transaction() {
        let mut seeks = Vec::new();
        let mut visited = Vec::new();
        let err = walk_candidates(Some(5), 0, fake_scan(10_000, &mut seeks), |txnum| {
            visited.push(txnum);
            Ok(())
        })
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("script hash history has more than 5 transactions"),
            "unexpected error: {err}"
        );
        // One seek, for limit + 1 candidates. The 10_000-entry history is never collected.
        assert_eq!(seeks, vec![(0, 6)]);
        assert!(visited.is_empty(), "refusal must not read a transaction");
    }

    /// A history of exactly `limit` entries is served, not refused (off-by-one guard), and
    /// the walk still only asks for `limit + 1`.
    #[test]
    fn test_walk_serves_history_of_exactly_the_limit() {
        let mut seeks = Vec::new();
        let mut visited = Vec::new();
        let seen = walk_candidates(Some(5), 0, fake_scan(5, &mut seeks), |txnum| {
            visited.push(txnum);
            Ok(())
        })
        .unwrap();

        assert_eq!(seen, 5);
        assert_eq!(visited, vec![0, 1, 2, 3, 4]);
        assert_eq!(seeks, vec![(0, 6)]);
    }

    /// A limit of zero is the strictest setting, not "unlimited" (`Some(0)`, not `None`):
    /// the first candidate is enough to refuse, and an empty history is still fine.
    #[test]
    fn test_walk_limit_zero() {
        let mut seeks = Vec::new();
        let err = walk_candidates(Some(0), 0, fake_scan(3, &mut seeks), |_| Ok(())).unwrap_err();
        assert!(err.to_string().contains("more than 0 transactions"));
        assert_eq!(seeks, vec![(0, 1)]);

        let mut seeks = Vec::new();
        assert_eq!(
            walk_candidates(Some(0), 0, fake_scan(0, &mut seeks), |_| Ok(())).unwrap(),
            0
        );
    }

    /// Without a limit the walk still pages (a huge history is never one `Vec`) and visits
    /// every candidate exactly once, in order.
    #[test]
    fn test_walk_without_limit_pages_through_everything() {
        let total = 10_000u32;
        let mut seeks = Vec::new();
        let mut visited = Vec::new();
        let seen = walk_candidates(None, 0, fake_scan(total, &mut seeks), |txnum| {
            visited.push(txnum);
            Ok(())
        })
        .unwrap();

        assert_eq!(seen, total as usize);
        assert_eq!(visited, (0..total).collect::<Vec<_>>());
        assert!(
            seeks.iter().all(|&(_, want)| want == HISTORY_SCAN_PAGE),
            "unbounded walk must stay in pages: {seeks:?}"
        );
        assert!(seeks.len() > 1, "10k candidates must not be one seek");
        assert_eq!(seeks[0], (0, HISTORY_SCAN_PAGE));
    }

    /// An incremental sync starts where the last indexed block ended, and a reorg-resumed
    /// walk must not revisit what it already has.
    #[test]
    fn test_walk_starts_at_from() {
        let mut seeks = Vec::new();
        let mut visited = Vec::new();
        walk_candidates(None, 9_000, fake_scan(10_000, &mut seeks), |txnum| {
            visited.push(txnum);
            Ok(())
        })
        .unwrap();

        assert_eq!(visited.first(), Some(&9_000));
        assert_eq!(visited.len(), 1_000);
        assert_eq!(seeks, vec![(9_000, HISTORY_SCAN_PAGE)]);
    }

    #[test]
    fn test_txinfo_json() {
        let txid = "5b75086dafeede555fc8f9a810d8b10df57c46f9f176ccc3dd8d2fa20edd685b"
            .parse()
            .unwrap();
        assert_eq!(
            json!(HistoryEntry::confirmed(txid, 123456)),
            json!({"tx_hash": "5b75086dafeede555fc8f9a810d8b10df57c46f9f176ccc3dd8d2fa20edd685b", "height": 123456})
        );
        assert_eq!(
            json!(HistoryEntry::unconfirmed(txid, true, Amount::from_sat(123))),
            json!({"tx_hash": "5b75086dafeede555fc8f9a810d8b10df57c46f9f176ccc3dd8d2fa20edd685b", "height": -1, "fee": 123})
        );
        assert_eq!(
            json!(HistoryEntry::unconfirmed(
                txid,
                false,
                Amount::from_sat(123)
            )),
            json!({"tx_hash": "5b75086dafeede555fc8f9a810d8b10df57c46f9f176ccc3dd8d2fa20edd685b", "height": 0, "fee": 123})
        );
    }
}
