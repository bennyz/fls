use super::oci::manifest::Descriptor;

#[derive(Debug, Clone)]
pub struct AnnotationSchema {
    pub name: &'static str,
    pub partition_key: &'static str,
    pub default_partitions_key: &'static str,
}

impl AnnotationSchema {
    pub const AUTOMOTIVE: Self = Self {
        name: "automotive",
        partition_key: "automotive.sdv.cloud.redhat.com/partition",
        default_partitions_key: "automotive.sdv.cloud.redhat.com/default-partitions",
    };

    pub const GENERIC: Self = Self {
        name: "generic",
        partition_key: "dev.jumpstarter.fls/partition",
        default_partitions_key: "dev.jumpstarter.fls/default-partitions",
    };

    pub fn default_search_order() -> &'static [Self] {
        static ORDER: [AnnotationSchema; 2] =
            [AnnotationSchema::GENERIC, AnnotationSchema::AUTOMOTIVE];
        &ORDER
    }
}

pub fn resolve_annotation_schema<'a>(
    layers: &[Descriptor],
    schemas: &'a [AnnotationSchema],
) -> Option<&'a AnnotationSchema> {
    schemas.iter().find(|schema| {
        layers.iter().any(|layer| {
            layer
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(schema.partition_key))
        })
    })
}

pub fn effective_schemas(custom: &[AnnotationSchema]) -> &[AnnotationSchema] {
    if custom.is_empty() {
        AnnotationSchema::default_search_order()
    } else {
        custom
    }
}

pub fn searched_keys_display(schemas: &[AnnotationSchema]) -> String {
    schemas
        .iter()
        .map(|s| format!("'{}' ({})", s.partition_key, s.name))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn descriptor_with_annotation(key: &str, value: &str) -> Descriptor {
        let mut annotations = HashMap::new();
        annotations.insert(key.to_string(), value.to_string());
        Descriptor {
            media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
            digest: "sha256:abc123".to_string(),
            size: 1000,
            annotations: Some(annotations),
            platform: None,
        }
    }

    fn descriptor_without_annotations() -> Descriptor {
        Descriptor {
            media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
            digest: "sha256:abc123".to_string(),
            size: 1000,
            annotations: None,
            platform: None,
        }
    }

    #[test]
    fn test_presets() {
        assert_eq!(
            AnnotationSchema::AUTOMOTIVE.partition_key,
            "automotive.sdv.cloud.redhat.com/partition"
        );
        assert_eq!(
            AnnotationSchema::GENERIC.partition_key,
            "dev.jumpstarter.fls/partition"
        );
    }

    #[test]
    fn test_default_search_order_prefers_generic() {
        let order = AnnotationSchema::default_search_order();
        assert_eq!(order[0].name, "generic");
        assert_eq!(order[1].name, "automotive");
    }

    #[test]
    fn test_resolve_finds_generic() {
        let layers = vec![descriptor_with_annotation(
            "dev.jumpstarter.fls/partition",
            "boot",
        )];
        let schemas = AnnotationSchema::default_search_order();
        let resolved = resolve_annotation_schema(&layers, schemas).unwrap();
        assert_eq!(resolved.name, "generic");
    }

    #[test]
    fn test_resolve_finds_automotive() {
        let layers = vec![descriptor_with_annotation(
            "automotive.sdv.cloud.redhat.com/partition",
            "root",
        )];
        let schemas = AnnotationSchema::default_search_order();
        let resolved = resolve_annotation_schema(&layers, schemas).unwrap();
        assert_eq!(resolved.name, "automotive");
    }

    #[test]
    fn test_resolve_prefers_generic_when_both_present() {
        let layers = vec![
            descriptor_with_annotation("dev.jumpstarter.fls/partition", "boot"),
            descriptor_with_annotation("automotive.sdv.cloud.redhat.com/partition", "root"),
        ];
        let schemas = AnnotationSchema::default_search_order();
        let resolved = resolve_annotation_schema(&layers, schemas).unwrap();
        assert_eq!(resolved.name, "generic");
    }

    #[test]
    fn test_resolve_returns_none_for_no_match() {
        let layers = vec![descriptor_without_annotations()];
        let schemas = AnnotationSchema::default_search_order();
        assert!(resolve_annotation_schema(&layers, schemas).is_none());
    }

    #[test]
    fn test_effective_schemas_uses_default_when_empty() {
        let schemas = effective_schemas(&[]);
        assert_eq!(schemas.len(), 2);
    }

    #[test]
    fn test_effective_schemas_uses_custom_when_provided() {
        let custom = vec![AnnotationSchema::AUTOMOTIVE];
        let schemas = effective_schemas(&custom);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "automotive");
    }
}
