use std::fmt::Debug;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use bitcoin::BlockHash;
use bitcoin::{hashes::Hash, Network};
use log::*;

use crate::{client, db, headers, index, Location};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("client failed: {0}")]
    Client(#[from] client::Error),

    #[error("use https://github.com/bitcoin/bitcoin/pull/33657")]
    NotSupported,

    #[error("indexing failed: {0:?}")]
    Index(#[from] index::Error),

    #[error("RocksDB failed: {0}")]
    RocksDB(#[from] rust_rocksdb::Error),

    #[error("Genesis block hash mismatch: {0} != {1}")]
    ChainMismatch(bitcoin::BlockHash, bitcoin::BlockHash),

    #[error("invalid address: {0}")]
    Address(#[from] bitcoin::address::ParseError),

    #[error("block not found: {0}")]
    BlockNotFound(#[from] headers::Reorg),

    #[error("Silent Payments requires a fresh or current-format bindex database")]
    SilentPaymentsFormat,

    #[error("missing Silent Payments index for block={0} at height={1}")]
    SilentPaymentsMissing(BlockHash, u32),
}

#[derive(Debug)]
pub struct Stats {
    pub tip: bitcoin::BlockHash,
    pub indexed_blocks: usize,
    pub size_read: usize,
    pub elapsed: Duration,
}

impl Stats {
    fn new(tip: bitcoin::BlockHash) -> Self {
        Self {
            tip,
            indexed_blocks: 0,
            size_read: 0,
            elapsed: Duration::ZERO,
        }
    }
}

pub struct IndexedChain {
    genesis_hash: bitcoin::BlockHash,
    headers: headers::Headers,
    client: client::Client,
    store: db::DB,
    chunk_size: usize,
    silent_payments_start: Option<u32>,
}

#[derive(Debug)]
pub struct Config {
    db_path: PathBuf,
    url: String,
}

struct HeaderChunk {
    headers: Vec<bitcoin::block::Header>,
    to_skip: usize,
}

impl HeaderChunk {
    fn get(&self) -> &[bitcoin::block::Header] {
        &self.headers[self.to_skip..]
    }
}

struct PerBlockData {
    blockhash: BlockHash,
    block_bytes: index::BlockBytes,
    spent_bytes: index::SpentBytes,
    txs_count: u32,
}

struct Builder<'a> {
    items: Vec<(index::TxNumRange, &'a PerBlockData)>,
    tip: BlockHash,
    first_height: usize,
    silent_payments_start: Option<u32>,
}

impl<'a> Builder<'a> {
    fn new(
        tip: Option<&index::IndexedHeader>,
        items: &'a [PerBlockData],
        first_height: usize,
        silent_payments_start: Option<u32>,
    ) -> Self {
        let mut next_txnum = tip.map_or_else(index::TxNum::default, |header| header.next_txnum());
        let tip = tip.map_or_else(bitcoin::BlockHash::all_zeros, |header| header.hash());

        let items = items
            .iter()
            .map(|data| {
                let first_txnum = next_txnum;
                next_txnum.increment_by(data.txs_count);
                let range = index::TxNumRange::new(first_txnum, next_txnum);
                (range, data)
            })
            .collect();
        Self {
            items,
            tip,
            first_height,
            silent_payments_start,
        }
    }

    fn build(self) -> Result<Vec<index::Batch>, Error> {
        use rayon::prelude::*;

        for pair in self.items.windows(2) {
            assert!(index::TxNumRange::adjacent(&pair[0].0, &pair[1].0));
        }
        let batches = self
            .items
            .into_par_iter()
            .enumerate()
            .map(|(offset, (txnum_range, data))| {
                let PerBlockData {
                    blockhash,
                    block_bytes,
                    spent_bytes,
                    txs_count,
                } = data;
                assert_eq!(txnum_range.len(), *txs_count);
                let mut batch =
                    index::Batch::build(txnum_range, *blockhash, block_bytes, spent_bytes)?;
                let height =
                    u32::try_from(self.first_height + offset).expect("block height fits u32");
                if self
                    .silent_payments_start
                    .is_some_and(|start| height >= start)
                {
                    batch.index_silent_payments(height, block_bytes, spent_bytes)?;
                }
                Ok(batch)
            })
            .collect::<Result<Vec<_>, index::Error>>()?;
        let mut tip = self.tip;
        for batch in &batches {
            let header = &batch.header;
            assert_eq!(tip, header.header().prev_blockhash);
            tip = header.hash();
        }
        Ok(batches)
    }
}

/// One transaction position that touched a script hash, and the block it is in.
///
/// `txnum` is the chronological position in the chain and doubles as the page cursor:
/// callers treat it as opaque and hand it back to continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScriptHashPageEntry {
    pub txnum: u32,
    pub block_height: usize,
}

impl IndexedChain {
    /// Open an existing DB, or create if missing.
    /// Use binary format REST API for fetching the data from bitcoind.
    /// Allow setting REST server URL (e.g. "http://hostname:8332").
    pub fn open(
        db_dir: impl AsRef<Path>,
        network: Network,
        url: Option<String>,
    ) -> Result<Self, Error> {
        let db_path = db_dir.as_ref().to_path_buf().join(network.to_string());
        let url = url.unwrap_or_else(|| format!("http://localhost:{}", default_rpc_port(network)));
        Self::from_config(Config { db_path, url }, None)
    }

    /// Use a separate index directory. Legacy indexes lack the historical tweak rows.
    pub fn open_with_silent_payments(
        db_dir: impl AsRef<Path>,
        network: Network,
        url: Option<String>,
    ) -> Result<Self, Error> {
        let db_path = db_dir.as_ref().join(network.to_string());
        let url = url.unwrap_or_else(|| format!("http://localhost:{}", default_rpc_port(network)));
        let start = if network == Network::Bitcoin {
            709_632
        } else {
            0
        };
        Self::from_config(Config { db_path, url }, Some(start))
    }

    fn from_config(config: Config, silent_payments_start: Option<u32>) -> Result<Self, Error> {
        let silent_payments = silent_payments_start.is_some();
        if silent_payments
            && config.db_path.join("CURRENT").exists()
            && !db::DB::has_silent_payments(&config.db_path)?
        {
            return Err(Error::SilentPaymentsFormat);
        }
        info!("index: {:?}", config);
        let chunk_size = rayon::current_num_threads();
        let agent = ureq::Agent::new_with_config(
            ureq::config::Config::builder()
                .max_response_header_size(usize::MAX) // Disabled as a workaround
                .max_idle_connections_per_host(chunk_size)
                .max_idle_connections(chunk_size)
                .build(),
        );
        let client = client::Client::new(agent, config.url);
        let genesis_hash = client.get_blockhash_by_height(0)?;
        let genesis_block = client.get_block_bytes(genesis_hash)?;

        // make sure bitcoind supports the required REST API endpoints
        // * /rest/getspenttxouts/ (added in https://github.com/bitcoin/bitcoin/pull/32540)
        match client.get_spent_bytes(genesis_hash) {
            Err(client::Error::Http(ureq::Error::StatusCode(404))) => Err(Error::NotSupported)?,
            res => res?,
        };
        // * /rest/blockpart/ (added in https://github.com/bitcoin/bitcoin/pull/33657)
        let txpos = index::TxBlockPos {
            offset: 0,
            size: genesis_block
                .len()
                .try_into()
                .expect("too large genesis block"),
        };
        match client.get_block_part(genesis_hash, txpos) {
            Err(client::Error::Http(ureq::Error::StatusCode(404))) => Err(Error::NotSupported)?,
            res => assert_eq!(index::BlockBytes::new(res?), genesis_block),
        };

        let store = db::DB::open(config.db_path, silent_payments)?;
        let headers = headers::Headers::new(store.headers()?);
        if let Some(indexed_genesis) = headers.genesis() {
            if indexed_genesis.hash() != genesis_hash {
                return Err(Error::ChainMismatch(indexed_genesis.hash(), genesis_hash));
            }
            info!(
                "block={} height={} headers loaded",
                headers.tip_hash(),
                headers.tip_height().unwrap(),
            );
        }
        Ok(IndexedChain {
            genesis_hash,
            headers,
            client,
            store,
            chunk_size,
            silent_payments_start,
        })
    }

    fn drop_tip(&mut self) -> Result<bitcoin::BlockHash, Error> {
        let height = self
            .headers
            .tip_height()
            .expect("cannot drop tip of an empty chain");
        let stale = self
            .headers
            .tip()
            .expect("cannot drop tip of an empty chain");
        let hash = stale.hash();
        let mut batches = self.index(&[*stale.header()], height, None, None)?;
        let height = u32::try_from(height).expect("block height fits u32");
        if self
            .silent_payments_start
            .is_some_and(|start| height >= start)
        {
            // Delete by the stored identity, without re-deriving the original tweak bytes.
            batches[0].silent_payments = Some((height, Vec::new()));
        }
        self.store.delete(&batches)?;
        self.headers.pop();
        Ok(hash)
    }

    pub fn silent_payments_reader(&self) -> Option<crate::SilentPaymentsReader> {
        self.store.silent_payments_reader()
    }

    pub fn silent_payments_start(&self) -> Option<u32> {
        self.silent_payments_start
    }

    #[cfg(test)]
    fn silent_payment_block(
        &self,
        height: u32,
        hash: BlockHash,
    ) -> Result<Vec<crate::SilentPaymentEntry>, Error> {
        self.headers.get_header(hash, height as usize)?;
        let reader = self
            .silent_payments_reader()
            .ok_or(Error::SilentPaymentsMissing(hash, height))?;
        let mut rows = Vec::new();
        loop {
            let (_, count, chunk) = reader
                .anchored_page(height, height, hash, rows.len())
                .map_err(|_| Error::SilentPaymentsMissing(hash, height))?;
            rows.extend(chunk);
            if rows.len() == count {
                return Ok(rows);
            }
        }
    }

    fn fetch_new_headers(&mut self, limit: usize) -> Result<HeaderChunk, Error> {
        loop {
            // usually the first header is already part of the current chain
            let (mut blockhash, mut to_skip) = (self.headers.tip_hash(), 1);
            if blockhash == bitcoin::BlockHash::all_zeros() {
                // but if the chain is empty, we need also to fetch the genesis header
                (blockhash, to_skip) = (self.genesis_hash, 0)
            }
            let headers = self.client.get_headers(blockhash, limit)?;
            if !headers.is_empty() {
                return Ok(HeaderChunk { headers, to_skip });
            }
            warn!(
                "block={} height={} was rolled back",
                blockhash,
                self.headers.tip_height().unwrap(),
            );
            // drop stale tip and retry fetching
            assert_eq!(blockhash, self.drop_tip()?);
        }
    }

    fn fetch_data(&self, blockhash: BlockHash) -> Result<PerBlockData, Error> {
        // TODO: can be done concurrently
        let block_bytes = self.client.get_block_bytes(blockhash)?;
        let spent_bytes = self.client.get_spent_bytes(blockhash)?;
        let txs_count = block_bytes.txs_count();
        assert_eq!(txs_count, spent_bytes.txs_count());
        Ok(PerBlockData {
            blockhash,
            block_bytes,
            spent_bytes,
            txs_count,
        })
    }

    fn index(
        &self,
        headers: &[bitcoin::block::Header],
        first_height: usize,
        silent_payments_start: Option<u32>,
        mut stats: Option<&mut Stats>,
    ) -> Result<Vec<index::Batch>, Error> {
        use rayon::prelude::*;

        let mut batches = Vec::with_capacity(headers.len());
        for (chunk_index, chunk) in headers.chunks(self.chunk_size).enumerate() {
            let items: Vec<_> = chunk
                .par_iter()
                .map(|header| self.fetch_data(header.block_hash()))
                .collect::<Result<Vec<_>, Error>>()?;

            let tip = batches.last().map_or_else(
                || {
                    first_height
                        .checked_sub(1)
                        .and_then(|height| self.headers.at_height(height))
                },
                |b: &index::Batch| Some(&b.header),
            );
            let height = first_height + chunk_index * self.chunk_size;
            batches.extend(Builder::new(tip, &items, height, silent_payments_start).build()?);

            for item in items {
                if let Some(s) = stats.as_mut() {
                    s.tip = item.blockhash;
                    s.size_read += item.block_bytes.len() + item.spent_bytes.len();
                    s.indexed_blocks += 1;
                }
            }
        }
        Ok(batches)
    }

    /// Synchornize index with bitcoind.
    /// Compactions are started when no new blocks are indexed.
    pub fn sync(&mut self, limit: usize) -> Result<Stats, Error> {
        let t = std::time::Instant::now();
        // get new headers (and drop stale ones if needed)
        let chunk = self.fetch_new_headers(limit)?;
        // start indexing from a valid tip
        let mut stats = Stats::new(self.headers.tip_hash());
        let first_height = self.headers.tip_height().map_or(0, |height| height + 1);
        let batches = self.index(
            chunk.get(),
            first_height,
            self.silent_payments_start,
            Some(&mut stats),
        )?;
        self.store.write(&batches)?;
        for batch in batches {
            self.headers.add(batch.header);
        }

        stats.elapsed = t.elapsed();
        if stats.indexed_blocks > 0 {
            self.store.flush()?;
            info!(
                "block={} height={}: indexed {} blocks, {:.3}[MB], dt = {:.3}[s]: {:.3} [ms/block], {:.3} [MB/block], {:.3} [MB/s]",
                self.headers.tip_hash(),
                self.headers.tip_height().unwrap(),
                stats.indexed_blocks,
                stats.size_read as f64 / (1e6),
                stats.elapsed.as_secs_f64(),
                stats.elapsed.as_secs_f64() * 1e3 / (stats.indexed_blocks as f64),
                stats.size_read as f64 / (1e6 * stats.indexed_blocks as f64),
                stats.size_read as f64 / (1e6 * stats.elapsed.as_secs_f64()),
            );
        } else {
            // Start autocompactions when there are no new indexed blocks
            self.store.start_compactions()?;
        }
        Ok(stats)
    }

    /// Collect transactions' locations spending/funding this scripthash.
    /// False-positive may occur, so post-filtering should be applied.
    pub fn locations_by_scripthash(
        &self,
        script_hash: &index::ScriptHash,
        latest_header: Option<&index::IndexedHeader>,
    ) -> Result<impl Iterator<Item = Location<'_>>, Error> {
        let from = latest_header
            .map(|header| header.next_txnum())
            .unwrap_or_default();
        let txnums = self.store.scan_by_script_hash(script_hash, from)?;
        Ok(txnums
            .into_iter()
            // chain and store must be in sync
            .map(|txnum| self.headers.find_by_txnum(txnum)))
    }

    /// The location of a transaction by its position in the chain.
    pub fn location_by_txnum(&self, txnum: u32) -> Location<'_> {
        self.headers.find_by_txnum(index::TxNum::from_u32(txnum))
    }

    /// One page of a script hash's history, newest-first or oldest-first.
    ///
    /// `from` is a seek position — a `txnum`, opaque to callers, passed back as the
    /// cursor to continue. `limit` bounds the index work, so a page's cost does not grow
    /// with the size of the history, which is the whole point: `locations_by_scripthash`
    /// collects every entry before the caller can use any of them.
    ///
    /// The index keys a script hash by its FIRST 8 BYTES only, so — exactly as with
    /// `locations_by_scripthash` — callers must post-filter what they read; the paged
    /// history in electrs does.
    pub fn script_hash_page(
        &self,
        script_hash: &index::ScriptHash,
        from: u32,
        limit: usize,
        newest_first: bool,
    ) -> Result<Vec<ScriptHashPageEntry>, Error> {
        let txnums = self.store.scan_by_script_hash_page(
            script_hash,
            index::TxNum::from_u32(from),
            limit,
            newest_first,
        )?;
        Ok(txnums
            .into_iter()
            .map(|txnum| {
                let location = self.headers.find_by_txnum(txnum);
                ScriptHashPageEntry {
                    txnum: txnum.to_u32(),
                    block_height: location.block_height(),
                }
            })
            .collect())
    }

    /// Collect transactions' locations matching this txid.
    /// False-positive may occur, so post-filtering should be applied.
    pub fn locations_by_txid(
        &self,
        txid: &bitcoin::Txid,
    ) -> Result<impl Iterator<Item = Location<'_>>, Error> {
        let txnums = self.store.scan_by_txid(txid)?;
        Ok(txnums
            .into_iter()
            // chain and store must be in sync
            .map(|txnum| self.headers.find_by_txnum(txnum)))
    }

    /// Fetch transaction's bytes from bitcoind.
    pub fn get_tx_bytes(&self, location: &Location) -> Result<Vec<u8>, Error> {
        // Lookup tx position within its block (offset & size)
        let pos = self.store.get_tx_block_pos(location.txnum)?;
        // Fetch the bytes from bitcoind
        Ok(self
            .client
            .get_block_part(location.indexed_header.hash(), pos)?)
    }

    pub fn headers(&self) -> &headers::Headers {
        &self.headers
    }
}

fn default_rpc_port(nework: Network) -> u16 {
    match nework {
        Network::Bitcoin => 8332,
        Network::Testnet => 18332,
        Network::Testnet4 => 48332,
        Network::Signet => 38332,
        Network::Regtest => 18443,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{consensus::deserialize, Amount, Transaction};
    use corepc_node::{exe_path, Conf, Node};

    #[test]
    fn test_chain_bitcoind() -> Result<(), Box<dyn std::error::Error>> {
        let mut conf = Conf::default();
        conf.args.push("-rest");

        let node = Node::with_conf(exe_path().unwrap(), &conf).unwrap();

        let get_tip = || {
            node.client
                .get_best_block_hash()
                .unwrap()
                .block_hash()
                .unwrap()
        };
        let addr = node.client.new_address().unwrap();

        const BLOCKS: usize = 101; // so that the first coinbase will be spendable
        node.client.generate_to_address(BLOCKS, &addr).unwrap();

        let dir = tempfile::TempDir::with_prefix("bindex_db").unwrap();
        let config = Config {
            db_path: dir.path().to_path_buf(),
            url: format!("http://{}", node.params.rpc_socket),
        };
        let mut chain = IndexedChain::from_config(config, None).unwrap();
        let stats = chain.sync(1000).unwrap();
        assert_eq!(stats.indexed_blocks, BLOCKS + 1);
        assert_eq!(stats.tip, get_tip());

        let addr1 = node.client.new_address().unwrap();
        let addr2 = node.client.new_address().unwrap();

        let txid1 = node
            .client
            .send_to_address(&addr1, Amount::from_int_btc(20))
            .unwrap()
            .txid()
            .unwrap();
        let tx1 = node
            .client
            .get_raw_transaction(txid1)
            .unwrap()
            .transaction()
            .unwrap();
        let txid2 = node
            .client
            .send_to_address(&addr2, Amount::from_int_btc(40))
            .unwrap()
            .txid()
            .unwrap();
        let tx2 = node
            .client
            .get_raw_transaction(txid2)
            .unwrap()
            .transaction()
            .unwrap();
        node.client.generate_to_address(1, &addr).unwrap();
        // Core 32 omits fullrbf, which older corepc models require.
        let mempool: serde_json::Value = node.client.call("getmempoolinfo", &[]).unwrap();
        assert_eq!(mempool["size"].as_u64(), Some(0));

        let stats = chain.sync(1000).unwrap();
        assert_eq!(stats.indexed_blocks, 1);
        assert_eq!(stats.tip, get_tip());
        let stats = chain.sync(1000).unwrap();
        assert_eq!(stats.indexed_blocks, 0);
        assert_eq!(stats.tip, get_tip());

        let loc1 = exactly_one(chain.locations_by_txid(&txid1).unwrap());
        assert_eq!(loc1.block_height, BLOCKS + 1);
        let tx_bytes = chain.get_tx_bytes(&loc1).unwrap();
        assert_eq!(deserialize::<Transaction>(&tx_bytes).unwrap(), tx1);

        let loc2 = exactly_one(chain.locations_by_txid(&txid2).unwrap());
        assert_eq!(loc1.block_height, BLOCKS + 1);
        let tx_bytes = chain.get_tx_bytes(&loc2).unwrap();
        assert_eq!(deserialize::<Transaction>(&tx_bytes).unwrap(), tx2);

        let txs: Vec<_> = chain
            .locations_by_scripthash(&index::ScriptHash::new(&addr.script_pubkey()), None)
            .unwrap()
            .collect();
        assert_eq!(txs.len(), BLOCKS + 2);

        let locations: Vec<_> = chain
            .locations_by_scripthash(&index::ScriptHash::new(&addr1.script_pubkey()), None)
            .unwrap()
            .collect();
        assert_eq!(locations, vec![loc1, loc2]);

        let locations: Vec<_> = chain
            .locations_by_scripthash(&index::ScriptHash::new(&addr2.script_pubkey()), None)
            .unwrap()
            .collect();
        assert_eq!(locations, vec![loc2]);

        // check reorg
        let old_tip = get_tip();
        node.client.invalidate_block(old_tip).unwrap();
        let stats = chain.sync(1000).unwrap();
        assert!(old_tip != stats.tip);
        assert_eq!(stats.indexed_blocks, 0);
        assert_eq!(stats.tip, get_tip());

        assert_eq!(chain.locations_by_txid(&txid1).unwrap().next(), None);
        assert_eq!(chain.locations_by_txid(&txid2).unwrap().next(), None);
        assert_eq!(
            chain
                .locations_by_scripthash(&index::ScriptHash::new(&addr1.script_pubkey()), None)
                .unwrap()
                .next(),
            None,
        );

        assert_eq!(
            chain
                .locations_by_scripthash(&index::ScriptHash::new(&addr2.script_pubkey()), None)
                .unwrap()
                .next(),
            None,
        );

        Ok(())
    }

    #[test]
    fn silent_payments_follow_the_canonical_block_after_restart_and_reorg() {
        use bitcoin::{consensus::encode::deserialize_hex, Txid};
        use serde_json::{json, Value};
        use silentpayments::utils::{
            receiving::{calculate_tweak_data, get_pubkey_from_input},
            OutPoint as SpOutPoint,
        };

        let mut conf = Conf::default();
        conf.args.extend(["-rest", "-txindex=1"]);
        let node = Node::with_conf(exe_path().unwrap(), &conf).unwrap();
        let rpc = |method: &str, args: &[Value]| node.client.call::<Value>(method, args).unwrap();
        let miner = rpc("getnewaddress", &[json!(""), json!("bech32")]);
        rpc("generatetoaddress", &[json!(105), miner.clone()]);
        let recipient = rpc("getnewaddress", &[json!(""), json!("bech32m")]);
        let txid: Txid = rpc("sendtoaddress", &[recipient, json!(0.01)])
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        rpc("generatetoaddress", &[json!(1), miner]);
        let old: BlockHash = rpc("getbestblockhash", &[])
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let height = 106;

        let dir = tempfile::TempDir::new().unwrap();
        let url = || Some(format!("http://{}", node.params.rpc_socket));
        let opt_in = || {
            IndexedChain::open_with_silent_payments(dir.path(), Network::Regtest, url()).unwrap()
        };
        let mut chain = opt_in();
        chain.sync(1000).unwrap();

        // Independently derive each candidate from the canonical raw block and Core's raw prevouts.
        let raw = rpc("getblock", &[json!(old), json!(0)]);
        let block: bitcoin::Block = deserialize_hex(raw.as_str().unwrap()).unwrap();
        let mut expected = Vec::new();
        for tx in &block.txdata {
            let outputs: Vec<_> = tx
                .output
                .iter()
                .enumerate()
                .filter(|(_, out)| out.script_pubkey.is_p2tr())
                .map(|(vout, out)| crate::SilentPaymentOutput {
                    vout: vout as u32,
                    key: out.script_pubkey.as_bytes()[2..].try_into().unwrap(),
                    sats: out.value.to_sat(),
                })
                .collect();
            if tx.is_coinbase() || outputs.is_empty() {
                continue;
            }
            let mut keys = Vec::new();
            let mut outpoints = Vec::new();
            for input in &tx.input {
                let outpoint = input.previous_output;
                let raw = rpc("getrawtransaction", &[json!(outpoint.txid)]);
                let parent: Transaction = deserialize_hex(raw.as_str().unwrap()).unwrap();
                if let Some(key) = get_pubkey_from_input(
                    input.script_sig.as_bytes(),
                    &input.witness.to_vec(),
                    parent.output[outpoint.vout as usize]
                        .script_pubkey
                        .as_bytes(),
                )
                .unwrap()
                {
                    keys.push(key);
                }
                outpoints.push(SpOutPoint::from_txid_bytes_and_vout(
                    outpoint.txid.to_byte_array(),
                    outpoint.vout,
                ));
            }
            if !keys.is_empty() {
                let tweak = calculate_tweak_data(&keys.iter().collect::<Vec<_>>(), &outpoints)
                    .unwrap()
                    .serialize();
                expected.push(crate::SilentPaymentEntry {
                    txid: tx.compute_txid(),
                    tweak,
                    outputs,
                });
            }
        }
        assert!(expected.iter().any(|row| row.txid == txid));
        assert_eq!(chain.silent_payment_block(height, old).unwrap(), expected);
        drop(chain);
        let mut chain = opt_in();
        assert_eq!(chain.silent_payment_block(height, old).unwrap(), expected);

        rpc("invalidateblock", &[json!(old)]);
        let miner = rpc("getnewaddress", &[json!(""), json!("bech32")]);
        let replacement = rpc("generateblock", &[miner.clone(), json!([])]);
        let replacement: BlockHash = replacement["hash"].as_str().unwrap().parse().unwrap();
        assert_ne!(old, replacement);
        assert_eq!(chain.sync(1000).unwrap().tip, replacement);
        assert!(chain.silent_payment_block(height, old).is_err());
        assert!(!chain.store.has_silent_payment_keys(height, old).unwrap());
        assert!(chain
            .silent_payment_block(height, replacement)
            .unwrap()
            .is_empty());
        drop(chain);

        let mut chain = opt_in();
        let extension = rpc("generateblock", &[miner, json!([])]);
        let extension: BlockHash = extension["hash"].as_str().unwrap().parse().unwrap();
        assert_eq!(chain.sync(1000).unwrap().tip, extension);
        assert!(chain
            .silent_payment_block(height + 1, extension)
            .unwrap()
            .is_empty());
        assert!(chain
            .silent_payment_block(height, replacement)
            .unwrap()
            .is_empty());
        assert!(!chain.store.has_silent_payment_keys(height, old).unwrap());

        let legacy_dir = tempfile::TempDir::new().unwrap();
        let legacy = IndexedChain::open(legacy_dir.path(), Network::Regtest, url()).unwrap();
        drop(legacy);
        assert!(matches!(
            IndexedChain::open_with_silent_payments(legacy_dir.path(), Network::Regtest, url()),
            Err(Error::SilentPaymentsFormat)
        ));
        IndexedChain::open(legacy_dir.path(), Network::Regtest, url()).unwrap();
    }

    fn exactly_one<T>(mut iter: impl Iterator<Item = T>) -> T {
        let res = iter.next().unwrap();
        assert!(iter.next().is_none());
        res
    }
}
