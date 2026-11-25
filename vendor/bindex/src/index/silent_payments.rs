use bitcoin::{
    consensus::{deserialize, Decodable, Encodable},
    hashes::{hash160, sha256, Hash, HashEngine},
    io::Cursor,
    secp256k1::{Parity, PublicKey, Scalar, Secp256k1, VerifyOnly, XOnlyPublicKey},
    Block, Script, Transaction, TxIn, TxOut, Txid, VarInt,
};

use super::{BlockBytes, Error, SpentBytes};

const NUMS_H: [u8; 32] = [
    0x50, 0x92, 0x9b, 0x74, 0xc1, 0xa0, 0x49, 0x54, 0xb7, 0x8b, 0x4b, 0x60, 0x35, 0xe9, 0x7a, 0x5e,
    0x07, 0x8a, 0x5a, 0x0f, 0x28, 0xec, 0x96, 0xd5, 0x47, 0xbf, 0xee, 0x9a, 0xce, 0x80, 0x3a, 0xc0,
];

#[derive(Debug, PartialEq, Eq)]
pub struct SilentPaymentOutput {
    pub vout: u32,
    pub key: [u8; 32],
    pub sats: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SilentPaymentEntry {
    pub txid: Txid,
    pub tweak: [u8; 33],
    pub outputs: Vec<SilentPaymentOutput>,
}

pub(super) fn index(block: &BlockBytes, spent: &SpentBytes) -> Result<Vec<u8>, Error> {
    let block: Block = deserialize(&block.0)?;
    let mut prevouts = Cursor::new(spent.0.as_slice());
    if VarInt::consensus_decode_from_finite_reader(&mut prevouts)?.0 != block.txdata.len() as u64 {
        return Err(Error::SpentAlignment);
    }
    let secp = Secp256k1::verification_only();
    let mut entries = Vec::new();
    for tx in &block.txdata {
        let expected = if tx.is_coinbase() { 0 } else { tx.input.len() };
        if VarInt::consensus_decode_from_finite_reader(&mut prevouts)?.0 != expected as u64 {
            return Err(Error::SpentAlignment);
        }
        let spent: Vec<_> = (0..expected)
            .map(|_| TxOut::consensus_decode_from_finite_reader(&mut prevouts))
            .collect::<Result<_, _>>()?;
        if let Some(entry) = transaction_entry(&secp, tx, &spent) {
            entries.push(entry);
        }
    }
    if prevouts.position() != spent.0.len() as u64 {
        return Err(Error::SpentAlignment);
    }
    Ok(encode(&entries))
}

fn transaction_entry(
    secp: &Secp256k1<VerifyOnly>,
    tx: &Transaction,
    prevouts: &[TxOut],
) -> Option<SilentPaymentEntry> {
    if tx.is_coinbase() || !tx.output.iter().any(|out| out.script_pubkey.is_p2tr()) {
        return None;
    }
    if prevouts.iter().any(|out| {
        out.script_pubkey
            .witness_version()
            .is_some_and(|v| v.to_num() > 1)
    }) {
        return None;
    }
    let keys: Vec<_> = tx
        .input
        .iter()
        .zip(prevouts)
        .filter_map(|(input, prevout)| input_key(input, &prevout.script_pubkey))
        .collect();
    let aggregate = PublicKey::combine_keys(&keys.iter().collect::<Vec<_>>()).ok()?;
    // BIP352 orders outpoints by their wire bytes, including every input.
    let first = tx
        .input
        .iter()
        .map(|input| {
            let outpoint = &input.previous_output;
            let mut bytes = [0; 36];
            bytes[..32].copy_from_slice(outpoint.txid.as_byte_array());
            bytes[32..].copy_from_slice(&outpoint.vout.to_le_bytes());
            bytes
        })
        .min()?;
    let tag = sha256::Hash::hash(b"BIP0352/Inputs");
    let mut engine = sha256::Hash::engine();
    engine.input(tag.as_ref());
    engine.input(tag.as_ref());
    engine.input(&first);
    engine.input(&aggregate.serialize());
    let scalar = Scalar::from_be_bytes(sha256::Hash::from_engine(engine).to_byte_array()).ok()?;
    if scalar == Scalar::ZERO {
        return None;
    }
    let tweak = aggregate.mul_tweak(secp, &scalar).ok()?.serialize();
    let outputs = tx
        .output
        .iter()
        .enumerate()
        .filter(|(_, out)| out.script_pubkey.is_p2tr())
        .map(|(vout, out)| SilentPaymentOutput {
            vout: u32::try_from(vout).expect("consensus block weight bounds output count"),
            key: out.script_pubkey.as_bytes()[2..].try_into().unwrap(),
            sats: out.value.to_sat(),
        })
        .collect();
    Some(SilentPaymentEntry {
        txid: tx.compute_txid(),
        tweak,
        outputs,
    })
}

fn input_key(input: &TxIn, script: &Script) -> Option<PublicKey> {
    if script.is_p2tr() {
        let witness = &input.witness;
        let last = witness.last()?;
        let annex = witness.len() > 1 && last.first() == Some(&0x50);
        let items = witness.len() - usize::from(annex);
        if items > 1 && witness.nth(items - 1)?.get(1..33) == Some(NUMS_H.as_slice()) {
            return None;
        }
        let key = XOnlyPublicKey::from_slice(&script.as_bytes()[2..]).ok()?;
        return Some(PublicKey::from_x_only_public_key(key, Parity::Even));
    }

    fn compressed_key(bytes: &[u8], hash: &[u8]) -> Option<PublicKey> {
        if bytes.len() == 33 && hash160::Hash::hash(bytes).as_byte_array().as_slice() == hash {
            return PublicKey::from_slice(bytes).ok();
        }
        None
    }

    if script.is_p2wpkh() {
        return compressed_key(input.witness.last()?, &script.as_bytes()[2..]);
    }
    if script.is_p2sh() {
        let (push, redeem) = input.script_sig.as_bytes().split_first()?;
        if *push == 22
            && Script::from_bytes(redeem).is_p2wpkh()
            && hash160::Hash::hash(redeem).as_byte_array().as_slice() == &script.as_bytes()[2..22]
        {
            return compressed_key(input.witness.last()?, &redeem[2..]);
        }
        return None;
    }
    if script.is_p2pkh() {
        // The scriptSig can have a nonstandard layout, so match the prevout's key hash.
        return input
            .script_sig
            .as_bytes()
            .windows(33)
            .rev()
            .find_map(|bytes| compressed_key(bytes, &script.as_bytes()[3..23]));
    }
    None
}

fn encode(entries: &[SilentPaymentEntry]) -> Vec<u8> {
    let mut bytes = Vec::new();
    VarInt(entries.len() as u64)
        .consensus_encode(&mut bytes)
        .unwrap();
    for entry in entries {
        entry.txid.consensus_encode(&mut bytes).unwrap();
        bytes.extend_from_slice(&entry.tweak);
        VarInt(entry.outputs.len() as u64)
            .consensus_encode(&mut bytes)
            .unwrap();
        for out in &entry.outputs {
            bytes.extend_from_slice(&out.vout.to_le_bytes());
            bytes.extend_from_slice(&out.key);
            bytes.extend_from_slice(&out.sats.to_le_bytes());
        }
    }
    bytes
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<SilentPaymentEntry>, Error> {
    use bitcoin::io::Read;
    let mut reader = Cursor::new(bytes);
    let count = VarInt::consensus_decode_from_finite_reader(&mut reader)?.0;
    let mut entries = Vec::new();
    for _ in 0..count {
        let txid = Txid::consensus_decode_from_finite_reader(&mut reader)?;
        let mut tweak = [0; 33];
        reader
            .read_exact(&mut tweak)
            .map_err(bitcoin::consensus::encode::Error::Io)?;
        let count = VarInt::consensus_decode_from_finite_reader(&mut reader)?.0;
        let mut outputs = Vec::new();
        for _ in 0..count {
            outputs.push(SilentPaymentOutput {
                vout: u32::consensus_decode_from_finite_reader(&mut reader)?,
                key: <[u8; 32]>::consensus_decode_from_finite_reader(&mut reader)?,
                sats: u64::consensus_decode_from_finite_reader(&mut reader)?,
            });
        }
        entries.push(SilentPaymentEntry {
            txid,
            tweak,
            outputs,
        });
    }
    let remaining = bytes.len() - reader.position() as usize;
    if remaining != 0 {
        return Err(Error::Leftover(remaining));
    }
    Ok(entries)
}
