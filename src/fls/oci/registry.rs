/// OCI Registry client
///
/// Implements the OCI Distribution Specification for pulling images:
/// - GET /v2/ - API version check
/// - GET /v2/<name>/manifests/<reference> - Fetch manifest
/// - GET /v2/<name>/blobs/<digest> - Fetch blob
use std::sync::Arc;

use reqwest::{Client, Response, StatusCode};

use super::auth::{request_token, Credentials, WwwAuthenticate};
use super::manifest::{media_types, Manifest};
use super::reference::ImageReference;
use crate::fls::download_error::DownloadError;
use crate::fls::options::{HttpClientOptions, OciOptions};
use crate::fls::parallel_download::{
    parallel_stream, partial_content_range, response_stream, ByteStream, ParallelConfig,
    RangeFetcher, SEGMENT_SIZE,
};

/// OCI Registry client
pub struct RegistryClient {
    client: Client,
    image_ref: ImageReference,
    credentials: Credentials,
    token: Option<String>,
    debug: bool,
}

impl RegistryClient {
    /// Create a new registry client
    pub async fn new(
        image_ref: ImageReference,
        options: &OciOptions,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Use HttpClientOptions for clean HTTP client setup
        let http_options: HttpClientOptions = options.into();
        let client = crate::fls::http::setup_http_client(&http_options).await?;

        let credentials = Credentials::new(options.username.clone(), options.password.clone());

        Ok(Self {
            client,
            image_ref,
            credentials,
            token: None,
            debug: options.common.debug,
        })
    }

    /// Authenticate with the registry
    pub async fn authenticate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let base_url = self.image_ref.registry_url();
        let v2_url = format!("{}/v2/", base_url);

        if self.debug {
            eprintln!("[DEBUG] Checking registry API: {}", v2_url);
        }

        // Try anonymous access first
        let response = self.client.get(&v2_url).send().await?;

        match response.status() {
            StatusCode::OK => {
                if self.debug {
                    eprintln!("[DEBUG] Registry allows anonymous access");
                }
                // Anonymous access works, no token needed
                Ok(())
            }
            StatusCode::UNAUTHORIZED => {
                // Need to authenticate
                let www_auth_header = response
                    .headers()
                    .get("www-authenticate")
                    .ok_or("No WWW-Authenticate header in 401 response")?
                    .to_str()
                    .map_err(|e| format!("Invalid WWW-Authenticate header: {}", e))?;

                if self.debug {
                    eprintln!("[DEBUG] WWW-Authenticate: {}", www_auth_header);
                }

                let www_auth = WwwAuthenticate::parse(www_auth_header)?;

                let token = request_token(
                    &self.client,
                    &www_auth,
                    self.image_ref.api_repository(),
                    &self.credentials,
                    self.debug,
                )
                .await?;

                if self.debug {
                    eprintln!("[DEBUG] Obtained bearer token");
                }

                self.token = Some(token);
                Ok(())
            }
            status => Err(format!("Unexpected status from /v2/: {}", status).into()),
        }
    }

    /// Authorization header value for registry requests, if any
    fn auth_header_value(&self) -> Option<String> {
        match &self.token {
            Some(token) => Some(format!("Bearer {}", token)),
            None => self.credentials.basic_auth_header(),
        }
    }

    /// Add authorization header to request if we have a token
    fn add_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.auth_header_value() {
            Some(value) => request.header("Authorization", value),
            None => request,
        }
    }

    fn blob_url(&self, digest: &str) -> String {
        format!(
            "{}/v2/{}/blobs/{}",
            self.image_ref.registry_url(),
            self.image_ref.api_repository(),
            digest
        )
    }

    /// Fetch the image manifest
    pub async fn fetch_manifest(&self) -> Result<Manifest, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/v2/{}/manifests/{}",
            self.image_ref.registry_url(),
            self.image_ref.api_repository(),
            self.image_ref.reference_string()
        );

        if self.debug {
            eprintln!("[DEBUG] Fetching manifest: {}", url);
        }

        let request = self.client.get(&url).header(
            "Accept",
            format!(
                "{}, {}, {}, {}",
                media_types::OCI_MANIFEST,
                media_types::DOCKER_MANIFEST_V2,
                media_types::OCI_INDEX,
                media_types::DOCKER_MANIFEST_LIST
            ),
        );

        let response = self.add_auth(request).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Failed to fetch manifest: {} - {}", status, body).into());
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let bytes = response.bytes().await?;

        if self.debug {
            eprintln!("[DEBUG] Manifest content-type: {:?}", content_type);
            eprintln!("[DEBUG] Manifest size: {} bytes", bytes.len());
        }

        Manifest::parse(&bytes, content_type.as_deref())
            .map_err(|e| format!("Failed to parse manifest: {}", e).into())
    }

    /// Fetch manifest for a specific digest (used for resolving manifest indexes)
    pub async fn fetch_manifest_by_digest(
        &self,
        digest: &str,
    ) -> Result<Manifest, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/v2/{}/manifests/{}",
            self.image_ref.registry_url(),
            self.image_ref.api_repository(),
            digest
        );

        if self.debug {
            eprintln!("[DEBUG] Fetching manifest by digest: {}", url);
        }

        let request = self.client.get(&url).header(
            "Accept",
            format!(
                "{}, {}",
                media_types::OCI_MANIFEST,
                media_types::DOCKER_MANIFEST_V2
            ),
        );

        let response = self.add_auth(request).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Failed to fetch manifest: {} - {}", status, body).into());
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let bytes = response.bytes().await?;
        Manifest::parse(&bytes, content_type.as_deref())
            .map_err(|e| format!("Failed to parse manifest: {}", e).into())
    }

    /// Start streaming a blob
    pub async fn get_blob_stream(
        &self,
        digest: &str,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        self.get_blob_stream_range(digest, None).await
    }

    /// Fetch a blob as a streaming response, optionally resuming from a byte offset.
    ///
    /// When `resume_from` is `Some(offset)`, sends a `Range: bytes=<offset>-` header.
    /// The caller should check for `StatusCode::PARTIAL_CONTENT` (206) to confirm
    /// the registry supports range requests.
    pub async fn get_blob_stream_range(
        &self,
        digest: &str,
        resume_from: Option<u64>,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        self.get_blob_range(digest, resume_from.unwrap_or(0), None)
            .await
    }

    /// Fetch `start..=end` of a blob (`end` None = to the end). No Range header
    /// is sent for the whole blob.
    async fn get_blob_range(
        &self,
        digest: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let url = self.blob_url(digest);

        if self.debug {
            match (start, end) {
                (0, None) => eprintln!("[DEBUG] Starting blob download: {}", url),
                (offset, None) => eprintln!(
                    "[DEBUG] Resuming blob download from byte {}: {}",
                    offset, url
                ),
                (s, Some(e)) => eprintln!("[DEBUG] Fetching blob bytes {}-{}: {}", s, e, url),
            }
        }

        let mut request = self.client.get(&url);
        match (start, end) {
            (0, None) => {}
            (s, Some(e)) => request = request.header("Range", format!("bytes={}-{}", s, e)),
            (s, None) => request = request.header("Range", format!("bytes={}-", s)),
        }
        let response = self.add_auth(request).send().await?;

        if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(DownloadError::from_http_status(response.status()).into());
        }

        if self.debug {
            if let Some(len) = response.content_length() {
                eprintln!("[DEBUG] Blob size: {} bytes", len);
            }
        }

        Ok(response)
    }

    /// Fetcher for `start..=end` byte ranges of a blob, for parallel download.
    fn blob_range_fetcher(&self, digest: &str) -> RangeFetcher {
        let client = self.client.clone();
        let url = self.blob_url(digest);
        let auth = self.auth_header_value();
        Arc::new(move |start, end| {
            let mut request = client
                .get(&url)
                .header("Range", format!("bytes={}-{}", start, end));
            if let Some(value) = &auth {
                request = request.header("Authorization", value.clone());
            }
            Box::pin(async move {
                let response = request.send().await.map_err(DownloadError::from_reqwest)?;
                if !response.status().is_success()
                    && response.status() != StatusCode::PARTIAL_CONTENT
                {
                    return Err(DownloadError::from_http_status(response.status()));
                }
                Ok(response)
            })
        })
    }

    /// Open a blob download, returning its byte stream and total size if known.
    ///
    /// With `parallel` set, the first segment is requested with a Range header;
    /// if the registry honours it, the rest is fetched over several connections.
    /// Registries that ignore Range get a single stream.
    pub async fn open_blob_download(
        &self,
        digest: &str,
        parallel: Option<&ParallelConfig>,
    ) -> Result<(ByteStream, Option<u64>), Box<dyn std::error::Error>> {
        let Some(config) = parallel else {
            let response = self.get_blob_stream(digest).await?;
            let len = response.content_length();
            return Ok((response_stream(response), len));
        };

        let response = self
            .get_blob_range(digest, 0, Some(SEGMENT_SIZE - 1))
            .await?;
        match partial_content_range(&response) {
            Some((start, _, _)) if start != 0 => Err(format!(
                "Registry returned range starting at byte {} instead of 0",
                start
            )
            .into()),
            Some((_, end, Some(total))) if end + 1 < total => {
                println!(
                    "Downloading with {} parallel connections ({} MB segments)",
                    config.connections,
                    SEGMENT_SIZE / (1024 * 1024)
                );
                let fetch = self.blob_range_fetcher(digest);
                Ok((
                    parallel_stream(response, 0, end, total, fetch, config.clone()),
                    Some(total),
                ))
            }
            Some((_, end, _)) => Ok((response_stream(response), Some(end + 1))),
            None => {
                if self.debug {
                    eprintln!("[DEBUG] Registry ignored Range request, using a single stream");
                }
                let len = response.content_length();
                Ok((response_stream(response), len))
            }
        }
    }

    /// Get the image reference
    #[allow(dead_code)]
    pub fn image_ref(&self) -> &ImageReference {
        &self.image_ref
    }
}
