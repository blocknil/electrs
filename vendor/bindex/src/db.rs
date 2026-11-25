use bitcoin::hashes::Hash;
use std::{path::Path, sync::Arc};

use crate::index::{self, TxBlockPosRow};

use log::*;
use rust_rocksdb as rocksdb;

/// Key-value database
pub struct DB {
    db: Arc<rocksdb::DB>,
    compacting: bool,
    silent_payments: bool,
}

fn default_opts() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_max_open_files(256);
    opts.set_keep_log_file_num(10);
    opts.set_disable_auto_compactions(true);

    let parallelism = std::thread::available_parallelism()
        .ok()
        .and_then(|v| u16::try_from(v.get()).ok())
        .unwrap_or(2)
        .clamp(1, 8);
    opts.increase_parallelism(parallelism.into());
    opts.set_max_subcompactions(parallelism.into());
    opts
}

const CONFIG_CF: &str = "config";
const CONFIG_KEY: &[u8] = b"C";

const HEADERS_CF: &str = "headers";
const SILENT_PAYMENTS_CF: &str = "silent_payments_v3";
const SP_SHARD_ROWS: usize = 32;
pub const SP_PAGE_ROWS: usize = 256;
const SCRIPT_HASH_CF: &str = "script_hash";
const TXPOS_CF: &str = "txpos";
const TXID_CF: &str = "txid";

// Will be used later:
const SPENDING_CF: &str = "spending";

// Deprecated CFs:
const FUNDING_CF: &str = "funding"; // used by electrs (before bindex)

const COLUMN_FAMILIES: &[&str] = &[
    CONFIG_CF,
    HEADERS_CF,
    TXPOS_CF,
    TXID_CF,
    SCRIPT_HASH_CF,
    SPENDING_CF,
    FUNDING_CF,
];

#[derive(Debug, serde::Deserialize)]
struct Config {
    format: u64, // bumped to enforce reindexing
}

fn column_families(silent_payments: bool) -> impl Iterator<Item = &'static str> {
    COLUMN_FAMILIES
        .iter()
        .copied()
        .chain(silent_payments.then_some(SILENT_PAYMENTS_CF))
}

impl DB {
    pub fn has_silent_payments(path: &Path) -> Result<bool, rocksdb::Error> {
        Ok(rocksdb::DB::list_cf(&default_opts(), path)?
            .iter()
            .any(|name| name == SILENT_PAYMENTS_CF))
    }

    pub fn open(path: impl AsRef<Path>, silent_payments: bool) -> Result<Self, rocksdb::Error> {
        let opts = default_opts();
        let descriptors = column_families(silent_payments)
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(name, opts.clone()));
        let db = rocksdb::DB::open_cf_descriptors(&opts, path, descriptors)?;

        let store = Self {
            db: Arc::new(db),
            compacting: false,
            silent_payments,
        };

        if let Some(config) = store.get_config()? {
            const CURRENT: u64 = 1;
            if config.format < CURRENT {
                panic!(
                    "Unsupported DB format ({} < {}) - please reindex!",
                    config.format, CURRENT
                );
            }
        }

        for cf_name in column_families(silent_payments) {
            let cf = store.cf(cf_name);
            let metadata = store.db.get_column_family_metadata_cf(cf);
            info!(
                "CF {}: {} files, {:.6} MBs",
                cf_name,
                metadata.file_count,
                metadata.size as f64 / 1e6
            );
        }
        Ok(store)
    }

    fn get_config(&self) -> Result<Option<Config>, rocksdb::Error> {
        Ok(self
            .db
            .get_cf(self.cf(CONFIG_CF), CONFIG_KEY)?
            .map(|value| serde_json::from_slice::<Config>(&value).expect("invalid config")))
    }

    fn cf(&self, name: &str) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(name)
            .unwrap_or_else(|| panic!("missing CF: {name}"))
    }

    fn apply_batch<F>(
        &self,
        f: F,
        batches: &[index::Batch],
        delete: bool,
    ) -> Result<(), rocksdb::Error>
    where
        F: Fn(&mut rocksdb::WriteBatch, &rocksdb::ColumnFamily, &[u8], &[u8]),
    {
        if batches.is_empty() {
            return Ok(());
        }
        let mut write_batch = rocksdb::WriteBatch::default();

        // key = scripthash prefix || matching txnum, value = b""
        let cf = self.cf(SCRIPT_HASH_CF);
        let mut scripthash_rows = vec![];
        for batch in batches {
            scripthash_rows.extend(batch.scripthash_rows.iter().map(index::HashPrefixRow::key));
        }
        scripthash_rows.sort_unstable();
        for row in scripthash_rows {
            f(&mut write_batch, cf, row, b"");
        }

        // key = txid prefix || matching txnum, value = b""
        let cf = self.cf(TXID_CF);
        let mut txid_rows = vec![];
        for batch in batches {
            txid_rows.extend(batch.txid_rows.iter().map(index::HashPrefixRow::key));
        }
        txid_rows.sort_unstable();
        for row in txid_rows {
            f(&mut write_batch, cf, row, b"");
        }

        // key = last_txnum, value = chunk of offsets
        let cf = self.cf(TXPOS_CF);
        // Rows are sorted by `txnum`.
        batches
            .iter()
            .flat_map(|batch| batch.txpos_rows.iter())
            .map(index::TxBlockPosRow::serialize)
            .for_each(|(k, v)| f(&mut write_batch, cf, &k, &v));

        // key = next_txnum, value = blockhash || blockheader
        let cf = self.cf(HEADERS_CF);
        for batch in batches {
            let (key, value) = batch.header.serialize();
            f(&mut write_batch, cf, &key, &value);
        }

        if self.silent_payments {
            let cf = self.cf(SILENT_PAYMENTS_CF);
            for batch in batches {
                if let Some((height, entries)) = &batch.silent_payments {
                    let hash = batch.header.hash();
                    let key = silent_payments_key(*height, hash);
                    if delete {
                        // All shard keys sort immediately after the metadata key.
                        write_batch.delete_range_cf(
                            cf,
                            key.as_slice(),
                            shard_key(*height, hash, u32::MAX).as_slice(),
                        );
                        write_batch.delete_cf(cf, key);
                    } else {
                        write_batch.put_cf(cf, key, (entries.len() as u32).to_le_bytes());
                        for (shard, rows) in entries.chunks(SP_SHARD_ROWS).enumerate() {
                            write_batch.put_cf(
                                cf,
                                shard_key(*height, hash, shard as u32),
                                index::silent_payments::encode(rows),
                            );
                        }
                    }
                    f(
                        &mut write_batch,
                        cf,
                        &canonical_key(*height),
                        batch.header.hash().as_byte_array(),
                    );
                }
            }
        }

        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(self.silent_payments);
        self.db.write_opt(write_batch, &opts)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn has_silent_payment_keys(
        &self,
        height: u32,
        hash: bitcoin::BlockHash,
    ) -> Result<bool, rocksdb::Error> {
        let prefix = silent_payments_key(height, hash);
        let mode = rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward);
        let mut rows = self.db.iterator_cf(self.cf(SILENT_PAYMENTS_CF), mode);
        Ok(rows
            .next()
            .transpose()?
            .is_some_and(|(key, _)| key.starts_with(&prefix)))
    }

    pub fn silent_payments_reader(&self) -> Option<SilentPaymentsReader> {
        self.silent_payments
            .then(|| SilentPaymentsReader(Arc::clone(&self.db)))
    }

    pub fn write(&self, batches: &[index::Batch]) -> Result<(), rocksdb::Error> {
        self.apply_batch(|wb, cf, k, v| wb.put_cf(cf, k, v), batches, false)
    }

    pub fn delete(&self, batches: &[index::Batch]) -> Result<(), rocksdb::Error> {
        // All keys contains txnum, so they are safe to delete in case of a reorg.
        self.apply_batch(|wb, cf, k, _v| wb.delete_cf(cf, k), batches, true)
    }

    pub fn flush(&self) -> Result<(), rocksdb::Error> {
        let opts = rocksdb::FlushOptions::new();
        for cf in column_families(self.silent_payments) {
            self.db.flush_cf_opt(self.cf(cf), &opts)?;
        }
        Ok(())
    }

    pub fn start_compactions(&mut self) -> Result<(), rocksdb::Error> {
        if !self.compacting {
            const OPTION: (&str, &str) = ("disable_auto_compactions", "false");
            for cf_name in column_families(self.silent_payments) {
                let cf = self.cf(cf_name);
                self.db.set_options_cf(cf, &[OPTION])?;
            }
            info!("started auto compactions");
            self.compacting = true;
        }
        Ok(())
    }

    /// Collect a list of `TxNum`s for specified `script_hash`.
    pub fn scan_by_script_hash(
        &self,
        script_hash: &index::ScriptHash,
        from: index::TxNum,
    ) -> Result<Vec<index::TxNum>, rocksdb::Error> {
        let cf = self.cf(SCRIPT_HASH_CF);
        let mut txnums = Vec::new();

        let hash_prefix = (*script_hash).into();
        let start = index::HashPrefixRow::new(hash_prefix, from);
        // Allow resuming iteration from a specified txnum (for incremental sync)
        let mode = rocksdb::IteratorMode::From(start.key(), rocksdb::Direction::Forward);
        for kv in self.db.iterator_cf(cf, mode) {
            let (key, _) = kv?;
            if !key.starts_with(hash_prefix.as_bytes()) {
                break;
            }
            let row = index::HashPrefixRow::from_bytes(key[..].try_into().unwrap());
            assert!(row.txnum() >= from);
            txnums.push(row.txnum());
        }
        Ok(txnums)
    }

    /// Collect AT MOST `limit` `TxNum`s for a script hash, in either direction.
    ///
    /// The keys are `(prefix ‖ txnum)` with `txnum` big-endian, so lexicographic order is
    /// chronological order and `from` is a seek rather than a walk. `newest_first`
    /// iterates backwards from `from`, which reaches the most recent entries without
    /// touching the older ones.
    ///
    /// `limit` is the point of this function: `scan_by_script_hash` collects EVERY
    /// matching txnum before the caller can look at any of them, so for a script hash with
    /// millions of transactions the cost is paid whether or not the caller uses them all.
    /// Stopping inside the RocksDB iterator keeps a page's cost independent of the
    /// history's size.
    pub fn scan_by_script_hash_page(
        &self,
        script_hash: &index::ScriptHash,
        from: index::TxNum,
        limit: usize,
        newest_first: bool,
    ) -> Result<Vec<index::TxNum>, rocksdb::Error> {
        let cf = self.cf(SCRIPT_HASH_CF);
        let mut txnums = Vec::with_capacity(limit.min(1_024));

        let hash_prefix = (*script_hash).into();
        let start = index::HashPrefixRow::new(hash_prefix, from);
        let direction = if newest_first {
            rocksdb::Direction::Reverse
        } else {
            rocksdb::Direction::Forward
        };
        let mode = rocksdb::IteratorMode::From(start.key(), direction);
        for kv in self.db.iterator_cf(cf, mode) {
            let (key, _) = kv?;
            if !key.starts_with(hash_prefix.as_bytes()) {
                break;
            }
            let row = index::HashPrefixRow::from_bytes(key[..].try_into().unwrap());
            txnums.push(row.txnum());
            if txnums.len() >= limit {
                break;
            }
        }
        Ok(txnums)
    }

    /// Collect a list of `TxNum`s for specified `txid`.
    pub fn scan_by_txid(&self, txid: &bitcoin::Txid) -> Result<Vec<index::TxNum>, rocksdb::Error> {
        let cf = self.cf(TXID_CF);
        let mut txnums = Vec::new();

        let hash_prefix = (*txid.as_raw_hash()).into();
        let start = index::HashPrefixRow::new(hash_prefix, index::TxNum::default());
        let mode = rocksdb::IteratorMode::From(start.key(), rocksdb::Direction::Forward);
        for kv in self.db.iterator_cf(cf, mode) {
            let (key, _) = kv?;
            if !key.starts_with(hash_prefix.as_bytes()) {
                break;
            }
            let row = index::HashPrefixRow::from_bytes(key[..].try_into().unwrap());
            txnums.push(row.txnum());
        }
        Ok(txnums)
    }

    /// Lookup transaction position (offset & size) within its block.
    pub fn get_tx_block_pos(
        &self,
        txnum: index::TxNum,
    ) -> Result<index::TxBlockPos, rocksdb::Error> {
        let cf = self.cf(TXPOS_CF);

        let from_key = txnum.serialize();
        let mode = rocksdb::IteratorMode::From(&from_key, rocksdb::Direction::Forward);
        if let Some(kv) = self.db.iterator_cf(cf, mode).next() {
            let (key, value) = kv?;
            assert!(from_key[..] <= key[..]);
            let row = TxBlockPosRow::deserialize(&key, &value);
            return Ok(row.get_tx_block_pos(txnum));
        }
        panic!("Missing {:?}", txnum)
    }

    /// Load all headers from DB.
    pub fn headers(&self) -> Result<Vec<index::IndexedHeader>, rocksdb::Error> {
        let cf = self.cf(HEADERS_CF);
        let mut result = Vec::with_capacity(1_000_000);
        for kv in self.db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
            let (key, value) = kv?;
            let row = index::IndexedHeader::deserialize((
                key[..].try_into().unwrap(),
                value[..].try_into().unwrap(),
            ));
            result.push(row)
        }
        Ok(result)
    }
}

fn silent_payments_key(height: u32, hash: bitcoin::BlockHash) -> [u8; 36] {
    use bitcoin::hashes::Hash;
    let mut key = [0; 36];
    key[..4].copy_from_slice(&height.to_be_bytes());
    key[4..].copy_from_slice(hash.as_byte_array());
    key
}

fn shard_key(height: u32, hash: bitcoin::BlockHash, shard: u32) -> [u8; 40] {
    let mut key = [0; 40];
    key[..36].copy_from_slice(&silent_payments_key(height, hash));
    key[36..].copy_from_slice(&shard.to_be_bytes());
    key
}

fn canonical_key(height: u32) -> [u8; 4] {
    height.to_be_bytes()
}

#[derive(Clone)]
pub struct SilentPaymentsReader(Arc<rocksdb::DB>);

#[derive(Debug, thiserror::Error)]
pub enum SilentPaymentsReadError {
    #[error("chain anchor has changed")]
    StaleAnchor,
    #[error("silent payments block data is not available")]
    Unavailable,
    #[error("invalid Silent Payments offset")]
    InvalidOffset,
    #[error("invalid Silent Payments record")]
    Corrupt,
    #[error(transparent)]
    Database(#[from] rocksdb::Error),
    #[error(transparent)]
    Decode(#[from] index::Error),
}

impl SilentPaymentsReader {
    /// Read the anchor, block identity and at most one page of rows from the same RocksDB view.
    pub fn anchored_page(
        &self,
        height: u32,
        end_height: u32,
        end_hash: bitcoin::BlockHash,
        offset: usize,
    ) -> Result<(bitcoin::BlockHash, usize, Vec<crate::SilentPaymentEntry>), SilentPaymentsReadError>
    {
        if height > end_height {
            return Err(SilentPaymentsReadError::Unavailable);
        }
        let cf = self.0.cf_handle(SILENT_PAYMENTS_CF).expect("enabled index");
        let snapshot = self.0.snapshot();
        let block_hash = |height| {
            let raw = snapshot
                .get_cf(cf, canonical_key(height))?
                .ok_or(SilentPaymentsReadError::Unavailable)?;
            let hash = raw
                .as_slice()
                .try_into()
                .map_err(|_| SilentPaymentsReadError::Corrupt)?;
            Ok::<_, SilentPaymentsReadError>(bitcoin::BlockHash::from_byte_array(hash))
        };
        if block_hash(end_height)? != end_hash {
            return Err(SilentPaymentsReadError::StaleAnchor);
        }
        let hash = block_hash(height)?;
        let count = snapshot
            .get_cf(cf, silent_payments_key(height, hash))?
            .ok_or(SilentPaymentsReadError::Unavailable)?;
        let count = u32::from_le_bytes(
            count
                .as_slice()
                .try_into()
                .map_err(|_| SilentPaymentsReadError::Corrupt)?,
        ) as usize;
        if offset >= count && offset != 0 {
            return Err(SilentPaymentsReadError::InvalidOffset);
        }
        let end = count.min(offset.saturating_add(SP_PAGE_ROWS));
        let mut rows = Vec::with_capacity(end - offset);
        for shard in offset / SP_SHARD_ROWS..end.div_ceil(SP_SHARD_ROWS) {
            let raw = snapshot
                .get_cf(cf, shard_key(height, hash, shard as u32))?
                .ok_or(SilentPaymentsReadError::Corrupt)?;
            let decoded = index::silent_payments::decode(&raw)?;
            let start = shard * SP_SHARD_ROWS;
            if decoded.len() != (count - start).min(SP_SHARD_ROWS) {
                return Err(SilentPaymentsReadError::Corrupt);
            }
            rows.extend(
                decoded
                    .into_iter()
                    .skip(offset.saturating_sub(start))
                    .take(end - offset - rows.len()),
            );
        }
        Ok((hash, count, rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchored_page_reads_only_its_shards_at_an_unaligned_offset() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(dir.path(), true).unwrap();
        let cf = db.cf(SILENT_PAYMENTS_CF);
        let height = 100;
        let hash = bitcoin::BlockHash::from_byte_array([3; 32]);
        let count = 330u32;
        db.db
            .put_cf(cf, canonical_key(height), hash.as_byte_array())
            .unwrap();
        db.db
            .put_cf(cf, silent_payments_key(height, hash), count.to_le_bytes())
            .unwrap();
        for shard in 1..=9 {
            let start = shard * SP_SHARD_ROWS;
            let rows: Vec<_> = (start..start + SP_SHARD_ROWS)
                .map(|position| {
                    let mut id = [0; 32];
                    id[..4].copy_from_slice(&(position as u32).to_le_bytes());
                    crate::SilentPaymentEntry {
                        txid: bitcoin::Txid::from_byte_array(id),
                        tweak: [2; 33],
                        outputs: Vec::new(),
                    }
                })
                .collect();
            db.db
                .put_cf(
                    cf,
                    shard_key(height, hash, shard as u32),
                    index::silent_payments::encode(&rows),
                )
                .unwrap();
        }
        // Shards 0 and 10 are deliberately absent. Neither belongs to this page.
        let reader = db.silent_payments_reader().unwrap();
        let offset = 41;
        let (actual_hash, total, rows) =
            reader.anchored_page(height, height, hash, offset).unwrap();
        assert_eq!(
            (actual_hash, total, rows.len()),
            (hash, count as usize, SP_PAGE_ROWS)
        );
        for (position, row) in (offset..).zip(rows) {
            assert_eq!(
                &row.txid.as_byte_array()[..4],
                &(position as u32).to_le_bytes()
            );
        }
        assert!(matches!(
            reader.anchored_page(height, height, hash, count as usize),
            Err(SilentPaymentsReadError::InvalidOffset)
        ));
    }
}
