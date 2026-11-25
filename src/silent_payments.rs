use std::{
    io::{self, BufRead, Read, Write},
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bindex::{SilentPaymentsReadError, SilentPaymentsReader};
use crossbeam_channel::{bounded, Sender};
use serde_json::{json, Value};

use crate::{
    bitcoin::{hex::DisplayHex, BlockHash},
    thread::spawn,
};

const METHOD: &str = "blockchain.silent_payments.get";
const MAX_REQUEST: usize = 4096;
const MAX_PAGE_ROWS: usize = 256;
const MAX_RANGE: u32 = 128;
const MAX_RESPONSE: usize = 4 * 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const QUEUE_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(6);
const BUSY_BACKOFF: Duration = Duration::from_millis(50);

pub(crate) fn features(start: u32, tip: Option<usize>) -> Value {
    json!({
        "version": 1,
        "method": METHOD,
        "start_height": start,
        "indexed_height": tip,
        "max_range_blocks": MAX_RANGE,
        "max_response_bytes": MAX_RESPONSE
    })
}

// Use a separate connection on the same port so regular RPCs and scan writes never share
// a socket or the peer event loop. A dedicated connection may fetch multiple pages.
pub(crate) fn is_scan(line: &str) -> bool {
    if !line.contains(METHOD) && !line.contains('\\') {
        return false;
    }
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    fn matches(request: &Value) -> bool {
        request["method"] == METHOD
    }
    if let Some(batch) = value.as_array() {
        batch.iter().any(matches)
    } else {
        matches(&value)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    height: u32,
    end_height: u32,
    end_hash: BlockHash,
    #[serde(default)]
    offset: usize,
}

struct Request {
    id: Value,
    params: Params,
}

#[derive(Debug)]
enum Failure {
    Invalid,
    Busy(Duration),
    Stale,
    Unavailable,
    Internal,
}

impl Failure {
    fn response(self, id: Value) -> Value {
        let (code, message, retry) = match self {
            Self::Invalid => (-32602, "invalid Silent Payments request", None),
            Self::Busy(backoff) => (-32020, "Silent Payments scanner is busy", Some(backoff)),
            Self::Stale => (-32021, "Silent Payments chain anchor has changed", None),
            Self::Unavailable => (-32022, "Silent Payments block is unavailable", None),
            Self::Internal => (-32603, "Silent Payments index read failed", None),
        };
        let mut error = json!({"code": code, "message": message});
        if let Some(delay) = retry {
            error["data"] = json!({"retry_after_ms": delay.as_millis().max(1) as u64});
        }
        json!({"jsonrpc": "2.0", "id": id, "error": error})
    }
}

fn parse_request(line: &str) -> Result<Request, Value> {
    let invalid = |id| Failure::Invalid.response(id);
    if line.len() > MAX_REQUEST {
        return Err(invalid(Value::Null));
    }
    let value: Value = serde_json::from_str(line).map_err(|_| invalid(Value::Null))?;
    let id = value["id"].clone();
    if !id.is_number() && !id.as_str().is_some_and(|id| id.len() <= 64) {
        return Err(invalid(Value::Null));
    }
    if !value.is_object() || value["method"] != METHOD {
        return Err(invalid(id));
    }
    let params: Params =
        serde_json::from_value(value["params"].clone()).map_err(|_| invalid(id.clone()))?;
    if !params
        .end_height
        .checked_sub(params.height)
        .is_some_and(|span| span < MAX_RANGE)
        || params.offset > 1_000_000
    {
        return Err(invalid(id));
    }
    Ok(Request { id, params })
}

fn page(reader: &SilentPaymentsReader, params: Params) -> Result<Value, Failure> {
    let (hash, rows) = reader
        .anchored_block(params.height, params.end_height, params.end_hash)
        .map_err(|err| match err {
            SilentPaymentsReadError::StaleAnchor => Failure::Stale,
            SilentPaymentsReadError::Unavailable => Failure::Unavailable,
            _ => Failure::Internal,
        })?;
    build_page(params, hash, &rows)
}

fn row_wire_size_bound(output_count: usize) -> usize {
    // All string payloads are fixed-width hexadecimal. Include the row and output commas.
    const ROW: usize = r#"{"txid":"","tweak":"","outputs":[]}"#.len() + 64 + 66 + 1;
    const OUTPUT: usize = r#"[, "", ]"#.len() + 10 + 64 + 20 + 1;
    ROW.saturating_add(OUTPUT.saturating_mul(output_count))
}

fn build_page(
    params: Params,
    hash: BlockHash,
    rows: &[bindex::SilentPaymentEntry],
) -> Result<Value, Failure> {
    if params.offset > rows.len() || (params.offset != 0 && params.offset == rows.len()) {
        return Err(Failure::Invalid);
    }

    let mut entries = Vec::new();
    // Leave space for fixed-size metadata, commas and the capped request ID.
    let mut remaining = MAX_RESPONSE - MAX_REQUEST;
    for row in rows.iter().skip(params.offset).take(MAX_PAGE_ROWS) {
        let size = row_wire_size_bound(row.outputs.len());
        if size > remaining {
            if entries.is_empty() {
                return Err(Failure::Internal);
            }
            break;
        }
        remaining -= size;
        let outputs: Vec<_> = row
            .outputs
            .iter()
            .map(|out| json!([out.vout, out.key.to_lower_hex_string(), out.sats]))
            .collect();
        let entry =
            json!({"txid": row.txid, "tweak": row.tweak.to_lower_hex_string(), "outputs": outputs});
        entries.push(entry);
    }

    let next_offset = params.offset + entries.len();
    let next = if next_offset < rows.len() {
        Some((params.height, next_offset))
    } else if params.height < params.end_height {
        Some((params.height + 1, 0))
    } else {
        None
    };
    Ok(json!({
        "height": params.height,
        "block_hash": hash,
        "end_height": params.end_height,
        "end_hash": params.end_hash,
        "offset": params.offset,
        "entries": entries,
        "next": next,
    }))
}

struct Job {
    request: Request,
    stream: TcpStream,
    queued: Instant,
    done: Sender<bool>,
    cancelled: Arc<AtomicBool>,
}

struct Admission {
    interval: Duration,
    next: Instant,
}

impl Admission {
    fn allow(&mut self, now: Instant) -> Result<(), Duration> {
        if now < self.next {
            return Err(self.next.duration_since(now).max(BUSY_BACKOFF));
        }
        self.next = now + self.interval;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct ScanPool {
    queue: Sender<Job>,
    admission: Arc<Mutex<Admission>>,
}

impl ScanPool {
    pub(crate) fn new(reader: SilentPaymentsReader, scans_per_second: u64) -> Self {
        let (send, receive) = bounded::<Job>(8);
        for _ in 0..2 {
            let receive = receive.clone();
            let reader = reader.clone();
            spawn("silent_payments_scan", move || {
                for job in receive {
                    if job.cancelled.load(Ordering::Relaxed) {
                        continue;
                    }
                    let Request { id, params } = job.request;
                    let result = if job.queued.elapsed() > QUEUE_TIMEOUT {
                        Err(Failure::Busy(BUSY_BACKOFF))
                    } else {
                        page(&reader, params)
                    };
                    let reply = match result {
                        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                        Err(error) => error.response(id),
                    };
                    let wrote = !job.cancelled.load(Ordering::Relaxed)
                        && write_reply(&job.stream, reply).is_ok();
                    let _ = job.done.send(wrote);
                }
                Ok(())
            });
        }
        Self {
            queue: send,
            admission: Arc::new(Mutex::new(Admission {
                interval: Duration::from_nanos(1_000_000_000 / scans_per_second),
                next: Instant::now(),
            })),
        }
    }

    pub(crate) fn run(
        &self,
        stream: &TcpStream,
        mut reader: impl BufRead,
        mut line: String,
    ) -> io::Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        loop {
            match parse_request(&line) {
                Ok(request) => {
                    let admission = self
                        .admission
                        .lock()
                        .expect("scan admission lock")
                        .allow(Instant::now());
                    if let Err(delay) = admission {
                        write_reply(stream, Failure::Busy(delay).response(request.id))?;
                        // Prevent immediate retries on the same connection from adding contention.
                        std::thread::sleep(delay);
                    } else {
                        let (done, status) = bounded(1);
                        let cancelled = Arc::new(AtomicBool::new(false));
                        let job = Job {
                            request,
                            stream: stream.try_clone()?,
                            queued: Instant::now(),
                            done,
                            cancelled: Arc::clone(&cancelled),
                        };
                        if let Err(error) = self.queue.try_send(job) {
                            write_reply(
                                stream,
                                Failure::Busy(BUSY_BACKOFF).response(error.into_inner().request.id),
                            )?;
                            std::thread::sleep(BUSY_BACKOFF);
                        } else if status.recv_timeout(REQUEST_TIMEOUT) != Ok(true) {
                            cancelled.store(true, Ordering::Relaxed);
                            return Ok(());
                        }
                    }
                }
                Err(reply) => write_reply(stream, reply)?,
            }
            line.clear();
            if !read_line(&mut reader, &mut line, MAX_REQUEST)? {
                return Ok(());
            }
        }
    }
}

pub(crate) fn read_line(
    reader: &mut impl BufRead,
    line: &mut String,
    limit: usize,
) -> io::Result<bool> {
    let count = reader.take((limit + 1) as u64).read_line(line)?;
    if count > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "request line too long",
        ));
    }
    Ok(count != 0)
}

fn write_reply(mut stream: &TcpStream, response: Value) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(&response)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_RESPONSE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "response limit"));
    }
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut remaining = bytes.as_slice();
    while !remaining.is_empty() {
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        stream.set_write_timeout(Some(timeout))?;
        match stream.write(remaining) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => remaining = &remaining[written..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitcoin::{hashes::Hash, Txid};

    #[test]
    fn admission_does_not_accumulate_a_burst_when_idle() {
        let now = Instant::now();
        let interval = Duration::from_millis(10);
        let mut admission = Admission {
            interval,
            next: now,
        };
        assert_eq!(admission.allow(now), Ok(()));
        assert_eq!(admission.allow(now), Err(BUSY_BACKOFF));
        let later = now + Duration::from_secs(1);
        assert_eq!(admission.allow(later), Ok(()));
        assert_eq!(admission.allow(later), Err(BUSY_BACKOFF));
        assert_eq!(admission.allow(later + interval), Ok(()));
    }

    #[test]
    fn dense_block_continues_in_place_before_advancing_height() {
        let hash = BlockHash::from_byte_array([7; 32]);
        let params = |offset| Params {
            height: 100,
            end_height: 101,
            end_hash: hash,
            offset,
        };
        let rows: Vec<_> = (0..=MAX_PAGE_ROWS)
            .map(|i| {
                let mut txid = [0; 32];
                txid[..8].copy_from_slice(&(i as u64).to_le_bytes());
                bindex::SilentPaymentEntry {
                    txid: Txid::from_byte_array(txid),
                    tweak: [2; 33],
                    outputs: vec![bindex::SilentPaymentOutput {
                        vout: 1,
                        key: [3; 32],
                        sats: 400,
                    }],
                }
            })
            .collect();
        let first = build_page(params(0), hash, &rows).unwrap();
        assert_eq!(first["entries"].as_array().unwrap().len(), MAX_PAGE_ROWS);
        assert_eq!(first["next"], json!([100, MAX_PAGE_ROWS]));
        let second = build_page(params(MAX_PAGE_ROWS), hash, &rows).unwrap();
        assert_eq!(
            second["entries"][0]["txid"],
            json!(rows[MAX_PAGE_ROWS].txid)
        );
        assert_eq!(second["next"], json!([101, 0]));
    }

    #[test]
    fn row_budget_covers_full_width_output_values() {
        let hash = BlockHash::from_byte_array([7; 32]);
        let row = bindex::SilentPaymentEntry {
            txid: Txid::from_byte_array([8; 32]),
            tweak: [2; 33],
            outputs: vec![
                bindex::SilentPaymentOutput {
                    vout: u32::MAX,
                    key: [3; 32],
                    sats: u64::MAX,
                },
                bindex::SilentPaymentOutput {
                    vout: u32::MAX,
                    key: [4; 32],
                    sats: u64::MAX,
                },
            ],
        };
        let params = Params {
            height: 100,
            end_height: 100,
            end_hash: hash,
            offset: 0,
        };
        let bound = row_wire_size_bound(row.outputs.len());
        let page = build_page(params, hash, &[row]).unwrap();
        assert!(page["entries"][0].to_string().len() < bound);
    }

    #[test]
    fn recognize_escaped_scan_methods_and_reject_batches() {
        let escaped = r#"{"method":"blockchain.silent_payments.\u0067et","id":1}"#;
        let batch = format!("[{escaped}]");
        assert!(is_scan(escaped));
        assert!(is_scan(&batch));
        assert_eq!(
            parse_request(&batch).err().unwrap()["error"]["code"],
            -32602
        );
        assert!(!is_scan(r#"{"method":"server.ping","id":0}"#));
    }
}
