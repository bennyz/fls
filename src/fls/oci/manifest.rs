/// OCI manifest types and parsing
///
/// Supports:
/// - OCI Image Manifest (application/vnd.oci.image.manifest.v1+json)
/// - Docker Manifest V2 (application/vnd.docker.distribution.manifest.v2+json)
use serde::Deserialize;

/// Media types for OCI/Docker manifests
pub mod media_types {
    pub const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
    pub const DOCKER_MANIFEST_V2: &str = "application/vnd.docker.distribution.manifest.v2+json";
    pub const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
    pub const DOCKER_MANIFEST_LIST: &str =
        "application/vnd.docker.distribution.manifest.list.v2+json";

    // Layer media types
    #[cfg(test)]
    pub const OCI_LAYER_GZIP: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
    #[cfg(test)]
    pub const OCI_LAYER_ZSTD: &str = "application/vnd.oci.image.layer.v1.tar+zstd";
    pub const DOCKER_LAYER: &str = "application/vnd.docker.image.rootfs.diff.tar.gzip";
}

/// OCI content descriptor
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Descriptor {
    /// Media type of the content
    pub media_type: String,

    /// Content digest (e.g., "sha256:abc123...")
    pub digest: String,

    /// Size in bytes
    pub size: u64,

    /// Optional annotations
    #[serde(default)]
    pub annotations: Option<std::collections::HashMap<String, String>>,

    /// Platform (for manifest lists/indexes)
    #[serde(default)]
    pub platform: Option<Platform>,
}

/// Platform specification
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
    #[serde(default)]
    pub variant: Option<String>,
}

/// OCI Image Manifest
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ImageManifest {
    /// Schema version (should be 2)
    pub schema_version: u32,

    /// Media type
    #[serde(default)]
    pub media_type: Option<String>,

    /// Artifact type (OCI v1.1) - indicates the primary content type
    #[serde(default)]
    pub artifact_type: Option<String>,

    /// Config blob descriptor
    pub config: Descriptor,

    /// Layer descriptors
    pub layers: Vec<Descriptor>,

    /// Optional annotations
    #[serde(default)]
    pub annotations: Option<std::collections::HashMap<String, String>>,
}

/// OCI Image Index (manifest list)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ImageIndex {
    /// Schema version (should be 2)
    pub schema_version: u32,

    /// Media type
    #[serde(default)]
    pub media_type: Option<String>,

    /// Manifest descriptors
    pub manifests: Vec<Descriptor>,
}

/// Parsed manifest - can be either a single manifest or an index
#[derive(Debug)]
pub enum Manifest {
    Image(Box<ImageManifest>),
    Index(ImageIndex),
}

impl Manifest {
    /// Parse manifest from JSON bytes
    pub fn parse(data: &[u8], content_type: Option<&str>) -> Result<Self, String> {
        // Try to determine type from content-type header or media_type field
        let data_str =
            std::str::from_utf8(data).map_err(|e| format!("Invalid UTF-8 in manifest: {}", e))?;

        // First, try to detect based on content
        if let Ok(index) = serde_json::from_str::<ImageIndex>(data_str) {
            if !index.manifests.is_empty() {
                return Ok(Manifest::Index(index));
            }
        }

        // Try as image manifest
        if let Ok(manifest) = serde_json::from_str::<ImageManifest>(data_str) {
            return Ok(Manifest::Image(Box::new(manifest)));
        }

        // Try based on content-type
        if let Some(ct) = content_type {
            if ct.contains("index") || ct.contains("list") {
                let index: ImageIndex = serde_json::from_str(data_str)
                    .map_err(|e| format!("Failed to parse manifest index: {}", e))?;
                return Ok(Manifest::Index(index));
            }
        }

        Err("Unable to parse manifest as image or index".to_string())
    }

    /// Get the single layer from an image manifest
    ///
    /// Returns:
    /// - the only layer for single-layer manifests
    /// - for multi-layer manifests: artifactType match first, otherwise first automotive disk layer
    /// - error when no suitable layer is found
    pub fn get_single_layer(&self) -> Result<&Descriptor, String> {
        match self {
            Manifest::Image(ref m) => {
                if m.layers.is_empty() {
                    return Err("Manifest has no layers".to_string());
                }

                if m.layers.len() == 1 {
                    return Ok(&m.layers[0]);
                }

                // If artifactType is set, find the layer matching it
                if let Some(ref artifact_type) = m.artifact_type {
                    let expected_base = split_media_type(artifact_type).0;
                    if let Some(layer) = m.layers.iter().find(|l| {
                        FlashableArtifact::is_flashable(&l.media_type)
                            && (l.media_type == *artifact_type
                                || split_media_type(&l.media_type).0 == expected_base)
                    }) {
                        return Ok(layer);
                    }
                }

                // Fall back to the first disk image layer
                if let Some(layer) = m
                    .layers
                    .iter()
                    .find(|l| FlashableArtifact::is_flashable(&l.media_type))
                {
                    return Ok(layer);
                }

                Err(format!(
                    "No disk image layer found among {} layers",
                    m.layers.len()
                ))
            }
            Manifest::Index(_) => Err(
                "Cannot get layer from manifest index - need to resolve platform first".to_string(),
            ),
        }
    }

    /// Get all layers from an image manifest
    #[allow(dead_code)]
    pub fn get_layers(&self) -> Result<&[Descriptor], String> {
        match self {
            Manifest::Image(ref m) => Ok(&m.layers),
            Manifest::Index(_) => Err("Cannot get layers from manifest index".to_string()),
        }
    }
}

impl ImageIndex {
    /// Find a manifest for a specific platform
    pub fn find_platform(&self, os: &str, arch: &str) -> Option<&Descriptor> {
        self.manifests.iter().find(|m| {
            if let Some(platform) = &m.platform {
                platform.os == os && platform.architecture == arch
            } else {
                false
            }
        })
    }

    /// Find a manifest for linux/amd64 or linux/arm64
    pub fn find_linux_manifest(&self) -> Option<&Descriptor> {
        // Try arm64 first (common for embedded), then amd64
        self.find_platform("linux", "arm64")
            .or_else(|| self.find_platform("linux", "amd64"))
    }
}

/// Split a media type into its base media type and optional structured syntax compression suffix.
fn split_media_type(media_type: &str) -> (&str, Option<&str>) {
    if media_type == media_types::DOCKER_LAYER {
        (media_type, Some("gzip"))
    } else if let Some((base, suffix)) = media_type.split_once('+') {
        (base, Some(suffix))
    } else {
        (media_type, None)
    }
}

impl Descriptor {
    /// Check if this is a gzip-compressed layer
    pub fn is_gzip_layer(&self) -> bool {
        self.compression() == LayerCompression::Gzip
    }

    /// Check if this is a zstd-compressed layer
    pub fn is_zstd_layer(&self) -> bool {
        self.compression() == LayerCompression::Zstd
    }

    /// Check if this is an xz-compressed layer
    pub fn is_xz_layer(&self) -> bool {
        self.compression() == LayerCompression::Xz
    }

    #[allow(dead_code)]
    pub fn flashable_artifact(&self) -> Option<FlashableArtifact> {
        FlashableArtifact::from_media_type(&self.media_type)
    }

    /// Get compression type
    pub fn compression(&self) -> LayerCompression {
        match split_media_type(&self.media_type).1 {
            Some("gzip") => LayerCompression::Gzip,
            Some("zstd") => LayerCompression::Zstd,
            Some("xz") => LayerCompression::Xz,
            _ => LayerCompression::None,
        }
    }
}

/// Layer compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerCompression {
    None,
    Gzip,
    Xz,
    Zstd,
}

/// Flashable disk image artifact types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashableArtifact {
    DiskRaw,
    DiskQcow2,
    DiskSimg,
}

impl FlashableArtifact {
    const MEDIA_TYPE_PREFIXES: &[&str] = &[
        "application/vnd.automotive.disk",
        "application/vnd.embedded.disk",
    ];

    /// Parse a [`FlashableArtifact`] from an OCI layer media type string, validating supported format and compression.
    pub fn from_media_type(media_type: &str) -> Option<Self> {
        let (base, suffix) = split_media_type(media_type);
        if let Some(s) = suffix {
            if !matches!(s, "gzip" | "zstd" | "xz") {
                return None;
            }
        }
        let format = Self::MEDIA_TYPE_PREFIXES
            .iter()
            .find_map(|prefix| base.strip_prefix(prefix))?;
        match format {
            ".raw" => Some(Self::DiskRaw),
            ".qcow2" => Some(Self::DiskQcow2),
            ".simg" => Some(Self::DiskSimg),
            _ => None,
        }
    }

    /// Return the standard file extension / format suffix for this artifact type (e.g., ".raw").
    pub fn format_suffix(&self) -> &'static str {
        match self {
            Self::DiskRaw => ".raw",
            Self::DiskQcow2 => ".qcow2",
            Self::DiskSimg => ".simg",
        }
    }

    /// Check whether the given media type corresponds to a supported flashable artifact.
    pub fn is_flashable(media_type: &str) -> bool {
        Self::from_media_type(media_type).is_some()
    }

    /// Return all supported uncompressed media type strings for flashable artifacts.
    pub fn supported_types() -> Vec<String> {
        let mut types = Vec::new();
        for prefix in Self::MEDIA_TYPE_PREFIXES {
            for artifact in [Self::DiskRaw, Self::DiskQcow2, Self::DiskSimg] {
                types.push(format!("{}{}", prefix, artifact.format_suffix()));
            }
        }
        types
    }
}

impl From<LayerCompression> for crate::fls::compression::Compression {
    fn from(layer_compression: LayerCompression) -> Self {
        match layer_compression {
            LayerCompression::None => crate::fls::compression::Compression::None,
            LayerCompression::Gzip => crate::fls::compression::Compression::Gzip,
            LayerCompression::Xz => crate::fls::compression::Compression::Xz,
            LayerCompression::Zstd => crate::fls::compression::Compression::Zstd,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_image_manifest() {
        let json = r#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 1234
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:layer123",
                    "size": 5678
                }
            ]
        }"#;

        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        match manifest {
            Manifest::Image(ref m) => {
                assert_eq!(m.schema_version, 2);
                assert_eq!(m.layers.len(), 1);
                assert_eq!(m.layers[0].digest, "sha256:layer123");
            }
            _ => panic!("Expected image manifest"),
        }
    }

    #[test]
    fn test_single_flashable_layer() {
        let json = r#"{
            "schemaVersion": 2,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 100
            },
            "layers": [
                {
                    "mediaType": "application/vnd.automotive.disk.raw",
                    "digest": "sha256:disk123",
                    "size": 9999
                }
            ]
        }"#;
        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        let layer = manifest.get_single_layer().unwrap();
        assert_eq!(layer.digest, "sha256:disk123");
    }

    #[test]
    fn test_single_non_disk_layer_returned() {
        // Single-layer manifests return the layer regardless of media type;
        // callers (e.g. flash_from_oci) are responsible for validating
        // flashable media types when appropriate.
        let json = r#"{
            "schemaVersion": 2,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 100
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:layer123",
                    "size": 5678
                }
            ]
        }"#;
        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        let layer = manifest.get_single_layer().unwrap();
        assert_eq!(layer.digest, "sha256:layer123");
    }

    #[test]
    fn test_artifact_type_selection() {
        // Two flashable layers: artifactType must pick the qcow2 layer,
        // NOT the raw layer which appears first (and would be chosen by the fallback).
        let json = r#"{
            "schemaVersion": 2,
            "artifactType": "application/vnd.automotive.disk.qcow2",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 100
            },
            "layers": [
                {
                    "mediaType": "application/vnd.automotive.disk.raw",
                    "digest": "sha256:disk_raw",
                    "size": 1000
                },
                {
                    "mediaType": "application/vnd.automotive.disk.qcow2",
                    "digest": "sha256:disk_qcow2",
                    "size": 9999
                }
            ]
        }"#;
        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        let layer = manifest.get_single_layer().unwrap();
        assert_eq!(layer.digest, "sha256:disk_qcow2");
    }

    #[test]
    fn test_artifact_type_no_match_falls_back_to_disk_layer() {
        // artifactType doesn't match any layer — should fall back to the first disk layer
        let json = r#"{
            "schemaVersion": 2,
            "artifactType": "application/vnd.unknown.type",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 100
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:tar_layer",
                    "size": 1000
                },
                {
                    "mediaType": "application/vnd.automotive.disk.raw",
                    "digest": "sha256:disk1",
                    "size": 9999
                }
            ]
        }"#;
        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        let layer = manifest.get_single_layer().unwrap();
        assert_eq!(layer.digest, "sha256:disk1");
    }

    #[test]
    fn test_no_flashable_layer_error() {
        let json = r#"{
            "schemaVersion": 2,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 100
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:layer1",
                    "size": 1000
                },
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+zstd",
                    "digest": "sha256:layer2",
                    "size": 2000
                }
            ]
        }"#;
        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        let err = manifest.get_single_layer().unwrap_err();
        assert!(
            err.contains("No disk image layer found"),
            "Expected no-match error, got: {}",
            err
        );
    }

    #[test]
    fn test_parse_index() {
        let json = r#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:manifest123",
                    "size": 1000,
                    "platform": {
                        "architecture": "arm64",
                        "os": "linux"
                    }
                }
            ]
        }"#;

        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        match manifest {
            Manifest::Index(idx) => {
                assert_eq!(idx.manifests.len(), 1);
                let linux = idx.find_linux_manifest().unwrap();
                assert_eq!(linux.digest, "sha256:manifest123");
            }
            _ => panic!("Expected manifest index"),
        }
    }

    #[test]
    fn test_flashable_artifact_from_media_type() {
        // Automotive prefix
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.raw"),
            Some(FlashableArtifact::DiskRaw)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.qcow2"),
            Some(FlashableArtifact::DiskQcow2)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.simg"),
            Some(FlashableArtifact::DiskSimg)
        );

        // Embedded prefix — same formats, different domain
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.embedded.disk.raw"),
            Some(FlashableArtifact::DiskRaw)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.embedded.disk.qcow2"),
            Some(FlashableArtifact::DiskQcow2)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.embedded.disk.simg"),
            Some(FlashableArtifact::DiskSimg)
        );

        // Unrecognized types
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.oci.image.layer.v1.tar+gzip"),
            None
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.vhdx"),
            None
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.unknown.disk.raw"),
            None
        );

        // Round-trip: each format suffix resolves back from both prefixes
        for artifact in [
            FlashableArtifact::DiskRaw,
            FlashableArtifact::DiskQcow2,
            FlashableArtifact::DiskSimg,
        ] {
            for prefix in FlashableArtifact::MEDIA_TYPE_PREFIXES {
                let media_type = format!("{}{}", prefix, artifact.format_suffix());
                assert_eq!(
                    FlashableArtifact::from_media_type(&media_type),
                    Some(artifact)
                );
            }
        }

        // Compressed variants
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.raw+gzip"),
            Some(FlashableArtifact::DiskRaw)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.raw+zstd"),
            Some(FlashableArtifact::DiskRaw)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.raw+xz"),
            Some(FlashableArtifact::DiskRaw)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.embedded.disk.qcow2+gzip"),
            Some(FlashableArtifact::DiskQcow2)
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.embedded.disk.simg+zstd"),
            Some(FlashableArtifact::DiskSimg)
        );

        // Unrecognized compression suffixes are rejected
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.raw+unknown"),
            None
        );
        assert_eq!(
            FlashableArtifact::from_media_type("application/vnd.automotive.disk.raw+tar"),
            None
        );
    }

    #[test]
    fn test_descriptor_compression() {
        let make_desc = |media_type: &str| Descriptor {
            media_type: media_type.to_string(),
            digest: "sha256:123".to_string(),
            size: 100,
            annotations: None,
            platform: None,
        };

        let raw = make_desc("application/vnd.automotive.disk.raw");
        assert_eq!(raw.compression(), LayerCompression::None);
        assert!(!raw.is_gzip_layer());
        assert!(!raw.is_zstd_layer());

        let raw_gz = make_desc("application/vnd.automotive.disk.raw+gzip");
        assert_eq!(raw_gz.compression(), LayerCompression::Gzip);
        assert!(raw_gz.is_gzip_layer());
        assert!(!raw_gz.is_zstd_layer());

        let raw_zstd = make_desc("application/vnd.automotive.disk.raw+zstd");
        assert_eq!(raw_zstd.compression(), LayerCompression::Zstd);
        assert!(!raw_zstd.is_gzip_layer());
        assert!(raw_zstd.is_zstd_layer());
        assert!(!raw_zstd.is_xz_layer());

        let raw_xz = make_desc("application/vnd.automotive.disk.raw+xz");
        assert_eq!(raw_xz.compression(), LayerCompression::Xz);
        assert!(!raw_xz.is_gzip_layer());
        assert!(!raw_xz.is_zstd_layer());
        assert!(raw_xz.is_xz_layer());

        let oci_gz = make_desc(media_types::OCI_LAYER_GZIP);
        assert_eq!(oci_gz.compression(), LayerCompression::Gzip);
        assert!(oci_gz.is_gzip_layer());
        assert!(!oci_gz.is_zstd_layer());

        let oci_zstd = make_desc(media_types::OCI_LAYER_ZSTD);
        assert_eq!(oci_zstd.compression(), LayerCompression::Zstd);
        assert!(!oci_zstd.is_gzip_layer());
        assert!(oci_zstd.is_zstd_layer());

        let docker = make_desc(media_types::DOCKER_LAYER);
        assert_eq!(docker.compression(), LayerCompression::Gzip);
        assert!(docker.is_gzip_layer());
        assert!(!docker.is_zstd_layer());
    }

    #[test]
    fn test_artifact_type_selection_with_compression() {
        let json = r#"{
            "schemaVersion": 2,
            "artifactType": "application/vnd.automotive.disk.qcow2",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 100
            },
            "layers": [
                {
                    "mediaType": "application/vnd.automotive.disk.raw+gzip",
                    "digest": "sha256:disk_raw",
                    "size": 1000
                },
                {
                    "mediaType": "application/vnd.automotive.disk.qcow2+gzip",
                    "digest": "sha256:disk_qcow2",
                    "size": 9999
                }
            ]
        }"#;
        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        let layer = manifest.get_single_layer().unwrap();
        assert_eq!(layer.digest, "sha256:disk_qcow2");
        assert_eq!(layer.compression(), LayerCompression::Gzip);
    }

    #[test]
    fn test_unsupported_layer_preceding_supported_layer_with_same_base() {
        let json = r#"{
            "schemaVersion": 2,
            "artifactType": "application/vnd.automotive.disk.raw",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 100
            },
            "layers": [
                {
                    "mediaType": "application/vnd.automotive.disk.raw+unknown",
                    "digest": "sha256:unsupported_raw",
                    "size": 1000
                },
                {
                    "mediaType": "application/vnd.automotive.disk.raw+gzip",
                    "digest": "sha256:supported_raw",
                    "size": 2000
                }
            ]
        }"#;
        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        let layer = manifest.get_single_layer().unwrap();
        assert_eq!(layer.digest, "sha256:supported_raw");
        assert_eq!(layer.compression(), LayerCompression::Gzip);
    }
}
