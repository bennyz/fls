use crate::fls::download_error::DownloadError;
use crate::fls::options::HttpClientOptions;
use crate::fls::parallel_download::{
    parallel_stream, partial_content_range, response_stream, ByteStream, ParallelConfig,
    RangeFetcher, SEGMENT_SIZE,
};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::time::Duration;

pub(crate) async fn setup_http_client(
    options: &HttpClientOptions,
) -> Result<Client, Box<dyn std::error::Error>> {
    if options.debug {
        eprintln!("\n[DEBUG] Initializing HTTP Client:");
        eprintln!("[DEBUG]   TLS Backend: rustls");
        eprintln!("[DEBUG]   HTTP/2: Adaptive mode enabled");
        eprintln!("[DEBUG]   HTTP/2 Stream Window: 16 MB");
        eprintln!("[DEBUG]   HTTP/2 Connection Window: 32 MB");
        eprintln!("[DEBUG]   Connection Pool: max 10 idle per host");
        eprintln!("[DEBUG]   Pool Idle Timeout: 90s");
        eprintln!("[DEBUG]   TCP Keepalive: 10s");
        eprintln!("[DEBUG]   TCP Nodelay: enabled");
        eprintln!("[DEBUG]   Request Timeout: 7200s (2 hours)");
        eprintln!("[DEBUG]   Connect Timeout: 30s");
        eprintln!("[DEBUG]   DNS Resolver: system resolver");
    }

    let mut builder = Client::builder()
        // Explicitly use rustls TLS backend for better custom CA support
        .use_rustls_tls()
        // Enable HTTP/2 adaptive mode (will use HTTP/2 if server supports it)
        .http2_adaptive_window(true)
        .http2_initial_stream_window_size(Some(1024 * 1024 * 16)) // 16MB stream window
        .http2_initial_connection_window_size(Some(1024 * 1024 * 32)) // 32MB connection window
        // Increase connection pool settings
        .pool_max_idle_per_host(10)
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        // Enable TCP keepalive to prevent connection drops
        .tcp_keepalive(Some(Duration::from_secs(10)))
        .tcp_nodelay(true) // Disable Nagle's algorithm for lower latency
        // Very long timeout for large downloads
        .timeout(Duration::from_secs(2 * 3600))
        .connect_timeout(Duration::from_secs(30))
        // Use system DNS resolver for better performance
        .no_hickory_dns();

    // Add custom CA certificate if provided
    if let Some(ca_cert_path) = &options.cacert {
        println!("Loading CA certificate from: {}", ca_cert_path.display());
        if options.debug {
            eprintln!(
                "[DEBUG]   Custom CA Certificate: {}",
                ca_cert_path.display()
            );
        }
        let cert_bytes = std::fs::read(ca_cert_path)
            .map_err(|e| format!("Failed to read CA certificate file: {}", e))?;

        let cert = reqwest::Certificate::from_pem(&cert_bytes)
            .map_err(|e| format!("Failed to parse CA certificate: {}", e))?;

        builder = builder.add_root_certificate(cert);
        println!("CA certificate loaded successfully");
    }

    if options.insecure_tls {
        println!("Warning: Certificate verification is disabled");
        if options.debug {
            eprintln!("[DEBUG]   Certificate Verification: DISABLED (insecure)");
        }
        builder = builder.danger_accept_invalid_certs(true);
    }

    if options.http1_only {
        if options.debug {
            eprintln!("[DEBUG]   HTTP/1.1 only: parallel requests use separate connections");
        }
        builder = builder.http1_only();
    }

    if options.debug {
        eprintln!("[DEBUG] HTTP Client initialized successfully\n");
    }

    Ok(builder.build()?)
}

/// Inclusive byte range to request: `(start, Some(end))` or open-ended `(start, None)`.
type ByteRange = (u64, Option<u64>);

fn build_request(
    client: &Client,
    url: &str,
    custom_headers: &[(String, String)],
    range: Option<ByteRange>,
) -> reqwest::RequestBuilder {
    let mut request = client
        .get(url)
        .header("User-Agent", "fls/0.1.0")
        .header("Accept", "*/*")
        .header("Accept-Encoding", "identity"); // Don't compress, we're handling .xz ourselves

    for (name, value) in custom_headers {
        request = request.header(name, value);
    }

    if let Some((start, end)) = range {
        let value = match end {
            Some(end) => format!("bytes={}-{}", start, end),
            None => format!("bytes={}-", start),
        };
        request = request.header("Range", value);
    }

    request
}

fn range_fetcher(url: &str, client: &Client, custom_headers: &[(String, String)]) -> RangeFetcher {
    let url = url.to_string();
    let client = client.clone();
    let headers = custom_headers.to_vec();
    Arc::new(move |start, end| {
        let request = build_request(&client, &url, &headers, Some((start, Some(end))));
        Box::pin(async move {
            let response = request.send().await.map_err(DownloadError::from_reqwest)?;
            if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
                return Err(DownloadError::from_http_response(&response));
            }
            Ok(response)
        })
    })
}

/// An opened download: the body stream, the number of bytes it will deliver
/// (when known), and whether the server honoured our Range request.
pub(crate) struct OpenedDownload {
    pub stream: ByteStream,
    pub remaining: Option<u64>,
    pub ranged: bool,
}

/// Open a download from `resume_from` (or the start), fetching with parallel
/// range requests when `parallel` is given and the server honours Range.
pub(crate) async fn open_download(
    url: &str,
    client: &Client,
    resume_from: Option<u64>,
    custom_headers: &[(String, String)],
    parallel: Option<&ParallelConfig>,
    debug: bool,
) -> Result<OpenedDownload, DownloadError> {
    let start = resume_from.unwrap_or(0);
    let range = match (parallel, resume_from) {
        (Some(_), _) => Some((start, Some(start + SEGMENT_SIZE - 1))),
        (None, Some(offset)) => Some((offset, None)),
        (None, None) => None,
    };

    let response = start_download(url, client, range, custom_headers, debug).await?;

    if let Some(config) = parallel {
        match partial_content_range(&response) {
            Some((s, _, _)) if s != start => {
                return Err(DownloadError::Other(format!(
                    "Server returned range starting at byte {} instead of {}",
                    s, start
                )));
            }
            Some((_, end, Some(total))) if end + 1 < total => {
                println!(
                    "Downloading with {} parallel connections ({} MB segments)",
                    config.connections,
                    SEGMENT_SIZE / (1024 * 1024)
                );
                let fetch = range_fetcher(url, client, custom_headers);
                return Ok(OpenedDownload {
                    stream: parallel_stream(response, start, end, total, fetch, config.clone()),
                    remaining: Some(total - start),
                    ranged: true,
                });
            }
            Some((_, end, _)) => {
                return Ok(OpenedDownload {
                    stream: response_stream(response),
                    remaining: Some(end + 1 - start),
                    ranged: true,
                });
            }
            None => {}
        }
    }

    let ranged = response.status() == StatusCode::PARTIAL_CONTENT;
    if range.is_some() && !ranged && debug {
        eprintln!("[DEBUG] Server ignored Range request, using a single stream");
    }
    if resume_from.is_some() && !ranged {
        println!("Warning: Server does not support range requests, starting from beginning");
    }
    let remaining = response.content_length();
    Ok(OpenedDownload {
        stream: response_stream(response),
        remaining,
        ranged,
    })
}

async fn start_download(
    url: &str,
    client: &Client,
    range: Option<ByteRange>,
    custom_headers: &[(String, String)],
    debug: bool,
) -> Result<reqwest::Response, DownloadError> {
    match range {
        Some((offset, _)) if offset > 0 => {
            println!("Resuming download from: {} (byte offset: {})", url, offset)
        }
        _ => println!("Starting download from: {}", url),
    }

    let request = build_request(client, url, custom_headers, range);

    // Debug: Log request details
    if debug {
        eprintln!("\n[DEBUG] HTTP Request:");
        eprintln!("[DEBUG]   Method: GET");
        eprintln!("[DEBUG]   URL: {}", url);
        eprintln!("[DEBUG]   Headers:");
        eprintln!("[DEBUG]     User-Agent: fls/0.1.0");
        eprintln!("[DEBUG]     Accept: */*");
        eprintln!("[DEBUG]     Accept-Encoding: identity");
        for (name, value) in custom_headers {
            eprintln!("[DEBUG]     {}: {}", name, value);
        }
        if let Some((start, end)) = range {
            match end {
                Some(end) => eprintln!("[DEBUG]     Range: bytes={}-{}", start, end),
                None => eprintln!("[DEBUG]     Range: bytes={}-", start),
            }
        }
    }

    let response = request.send().await.map_err(DownloadError::from_reqwest)?;

    // Debug: Log response details
    if debug {
        eprintln!("\n[DEBUG] HTTP Response:");
        eprintln!(
            "[DEBUG]   Status: {} {}",
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("")
        );
        eprintln!("[DEBUG]   Version: {:?}", response.version());
        eprintln!("[DEBUG]   Headers:");
        for (name, value) in response.headers() {
            if let Ok(val_str) = value.to_str() {
                eprintln!("[DEBUG]     {}: {}", name, val_str);
            } else {
                eprintln!("[DEBUG]     {}: <binary data>", name);
            }
        }
        if let Some(remote_addr) = response.remote_addr() {
            eprintln!("[DEBUG]   Remote Address: {}", remote_addr);
        }
        eprintln!();
    }

    // Accept both 200 (full content) and 206 (partial content) as success
    if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(DownloadError::from_http_response(&response));
    }

    Ok(response)
}
