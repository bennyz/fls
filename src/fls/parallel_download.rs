//! Parallel ranged HTTP download.
//!
//! Splits `[start, total)` into fixed-size segments fetched over several
//! connections at once and re-emits them as one ordered byte stream. Each
//! segment retries on its own, resuming from the bytes it already delivered,
//! so a dropped connection costs only the remainder of that segment.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{stream, Stream, StreamExt};
use reqwest::StatusCode;
use tokio::sync::mpsc;

use crate::fls::download_error::{handle_download_retry, DownloadError};

pub(crate) type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, DownloadError>> + Send>>;

/// Opens a request for the inclusive byte range `start..=end`.
pub(crate) type RangeFetcher = Arc<
    dyn Fn(u64, u64) -> BoxFuture<'static, Result<reqwest::Response, DownloadError>> + Send + Sync,
>;

pub(crate) const SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub(crate) struct ParallelConfig {
    pub connections: usize,
    pub max_retries: usize,
    pub retry_delay_secs: u64,
    pub debug: bool,
}

/// Adapt a response body into the stream type the pipelines consume.
pub(crate) fn response_stream(response: reqwest::Response) -> ByteStream {
    Box::pin(
        response
            .bytes_stream()
            .map(|r| r.map_err(DownloadError::from_reqwest)),
    )
}

/// Parse a `Content-Range: bytes <start>-<end>/<total>` header value.
pub(crate) fn parse_content_range(value: &str) -> Option<(u64, u64, Option<u64>)> {
    let rest = value.trim().strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let total = if total.trim() == "*" {
        None
    } else {
        Some(total.trim().parse().ok()?)
    };
    Some((start.trim().parse().ok()?, end.trim().parse().ok()?, total))
}

/// The `(start, end, total)` a 206 response covers, from its Content-Range header.
pub(crate) fn partial_content_range(
    response: &reqwest::Response,
) -> Option<(u64, u64, Option<u64>)> {
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return None;
    }
    response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)?
        .to_str()
        .ok()
        .and_then(parse_content_range)
}

/// Stream `[start, total)` using parallel range requests.
///
/// `first` must be a 206 response covering `start..=first_end`. The remainder
/// is fetched in `SEGMENT_SIZE` pieces, `config.connections` at a time, and
/// forwarded strictly in order. The stream ends early with an `Err` if a
/// segment exhausts its retries.
pub(crate) fn parallel_stream(
    first: reqwest::Response,
    start: u64,
    first_end: u64,
    total: u64,
    fetch: RangeFetcher,
    config: ParallelConfig,
) -> ByteStream {
    let (out_tx, out_rx) = mpsc::channel::<Result<Bytes, DownloadError>>(64);

    tokio::spawn(async move {
        let connections = config.connections.max(1);
        let mut pending = VecDeque::new();
        pending.push_back(spawn_segment(
            Some(first),
            start,
            first_end,
            fetch.clone(),
            config.clone(),
        ));
        let mut next_start = first_end + 1;

        loop {
            while pending.len() < connections && next_start < total {
                let end = (next_start + SEGMENT_SIZE - 1).min(total - 1);
                pending.push_back(spawn_segment(
                    None,
                    next_start,
                    end,
                    fetch.clone(),
                    config.clone(),
                ));
                next_start = end + 1;
            }
            let Some(mut rx) = pending.pop_front() else {
                break;
            };
            while let Some(item) = rx.recv().await {
                let failed = item.is_err();
                if out_tx.send(item).await.is_err() || failed {
                    return;
                }
            }
        }
    });

    Box::pin(stream::unfold(out_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

enum SegmentError {
    ConsumerGone,
    Failed(DownloadError, u64),
}

fn spawn_segment(
    initial: Option<reqwest::Response>,
    start: u64,
    end: u64,
    fetch: RangeFetcher,
    config: ParallelConfig,
) -> mpsc::Receiver<Result<Bytes, DownloadError>> {
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(async move {
        let mut initial = initial;
        let mut position = start;
        let mut retry_count = 0;

        loop {
            let response = match initial.take() {
                Some(response) => Ok(response),
                None => fetch(position, end).await,
            };
            let error = match response {
                Ok(response) => match forward_segment(response, position, end, &tx).await {
                    Ok(()) | Err(SegmentError::ConsumerGone) => return,
                    Err(SegmentError::Failed(e, delivered)) => {
                        position += delivered;
                        e
                    }
                },
                Err(e) => e,
            };

            if config.debug {
                eprintln!(
                    "[DEBUG] Segment {}-{} failed at byte {}: {}",
                    start,
                    end,
                    position,
                    error.format_error()
                );
            }
            match handle_download_retry(
                &error,
                &mut retry_count,
                config.max_retries,
                config.retry_delay_secs,
            ) {
                Some(delay) => tokio::time::sleep(delay).await,
                None => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
        }
    });

    rx
}

async fn forward_segment(
    response: reqwest::Response,
    start: u64,
    end: u64,
    tx: &mpsc::Sender<Result<Bytes, DownloadError>>,
) -> Result<(), SegmentError> {
    let expected = end - start + 1;
    match partial_content_range(&response) {
        Some((s, _, _)) if s == start => {}
        _ => {
            return Err(SegmentError::Failed(
                DownloadError::HttpClientError(
                    response.status().as_u16(),
                    format!(
                        "server did not honour Range request for bytes {}-{}",
                        start, end
                    ),
                ),
                0,
            ))
        }
    }

    let mut body = response.bytes_stream();
    let mut received = 0u64;
    loop {
        match tokio::time::timeout(CHUNK_TIMEOUT, body.next()).await {
            Ok(Some(Ok(mut chunk))) => {
                let remaining = expected - received;
                if chunk.len() as u64 > remaining {
                    chunk.truncate(remaining as usize);
                }
                received += chunk.len() as u64;
                if tx.send(Ok(chunk)).await.is_err() {
                    return Err(SegmentError::ConsumerGone);
                }
                if received == expected {
                    return Ok(());
                }
            }
            Ok(Some(Err(e))) => {
                return Err(SegmentError::Failed(
                    DownloadError::from_reqwest(e),
                    received,
                ))
            }
            Ok(None) => {
                return Err(SegmentError::Failed(
                    DownloadError::ConnectionError(format!(
                        "segment {}-{} ended after {} of {} bytes",
                        start, end, received, expected
                    )),
                    received,
                ))
            }
            Err(_) => {
                return Err(SegmentError::Failed(
                    DownloadError::TimeoutError(format!(
                        "no data for {}s on segment {}-{}",
                        CHUNK_TIMEOUT.as_secs(),
                        start,
                        end
                    )),
                    received,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_range() {
        assert_eq!(
            parse_content_range("bytes 0-1023/4096"),
            Some((0, 1023, Some(4096)))
        );
        assert_eq!(parse_content_range("bytes 10-19/*"), Some((10, 19, None)));
        assert_eq!(parse_content_range("bytes 0-1023"), None);
        assert_eq!(parse_content_range("items 0-1/2"), None);
    }
}
