#![forbid(unsafe_code)]

use std::collections::{btree_map::Entry, BTreeMap};

use serde::{Deserialize, Serialize};

/// Supported manifest schema version.
pub const MANIFEST_SCHEMA_V1: u16 = 1;
/// Supported delta schema version.
pub const DELTA_SCHEMA_V1: u16 = 1;

/// Errors for manifest/delta schema parsing and validation.
#[derive(Debug, PartialEq, Eq)]
pub enum MetadataError {
    Json(String),
    UnsupportedSchemaVersion { expected: u16, found: u16 },
    EmptyField(&'static str),
    MissingField(&'static str),
    WrongFieldType(&'static str),
}

impl From<serde_json::Error> for MetadataError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

/// Manifest snapshot v1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSnapshotV1 {
    pub schema_version: u16,
    pub epoch_ms: u64,
    pub checksum32: u32,
    pub entries: Vec<ManifestEntryV1>,
}

/// Segment metadata entry in a snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntryV1 {
    pub object_id: String,
    pub key_range_start: String,
    pub key_range_end: String,
}

/// Delta fact v1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaFactV1 {
    pub schema_version: u16,
    pub epoch_ms: u64,
    pub seq: u64,
    pub tenant_id: String,
    pub fingerprint: String,
    pub prefix_hash: String,
    pub payload_digest: String,
    pub write_id: String,
}

/// Version-dispatched manifest snapshot enum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestSnapshot {
    V1(ManifestSnapshotV1),
}

/// Version-dispatched delta fact enum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeltaFact {
    V1(DeltaFactV1),
}

/// Parse and validate manifest snapshot v1.
pub fn parse_manifest_snapshot_v1(json: &str) -> Result<ManifestSnapshotV1, MetadataError> {
    let manifest: ManifestSnapshotV1 = serde_json::from_str(json)?;
    if manifest.schema_version != MANIFEST_SCHEMA_V1 {
        return Err(MetadataError::UnsupportedSchemaVersion {
            expected: MANIFEST_SCHEMA_V1,
            found: manifest.schema_version,
        });
    }

    for entry in &manifest.entries {
        if entry.object_id.is_empty() {
            return Err(MetadataError::EmptyField("object_id"));
        }
        if entry.key_range_start.is_empty() {
            return Err(MetadataError::EmptyField("key_range_start"));
        }
        if entry.key_range_end.is_empty() {
            return Err(MetadataError::EmptyField("key_range_end"));
        }
    }

    Ok(manifest)
}

/// Parse and validate delta fact v1.
pub fn parse_delta_fact_v1(json: &str) -> Result<DeltaFactV1, MetadataError> {
    let fact: DeltaFactV1 = serde_json::from_str(json)?;
    if fact.schema_version != DELTA_SCHEMA_V1 {
        return Err(MetadataError::UnsupportedSchemaVersion {
            expected: DELTA_SCHEMA_V1,
            found: fact.schema_version,
        });
    }

    if fact.tenant_id.is_empty() {
        return Err(MetadataError::EmptyField("tenant_id"));
    }
    if fact.fingerprint.is_empty() {
        return Err(MetadataError::EmptyField("fingerprint"));
    }
    if fact.prefix_hash.is_empty() {
        return Err(MetadataError::EmptyField("prefix_hash"));
    }
    if fact.payload_digest.is_empty() {
        return Err(MetadataError::EmptyField("payload_digest"));
    }
    if fact.write_id.is_empty() {
        return Err(MetadataError::EmptyField("write_id"));
    }

    Ok(fact)
}

/// Parse manifest snapshot with schema-version dispatch.
pub fn parse_manifest_snapshot(json: &str) -> Result<ManifestSnapshot, MetadataError> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    let schema_version = extract_schema_version(&value)?;
    if schema_version != MANIFEST_SCHEMA_V1 {
        return Err(MetadataError::UnsupportedSchemaVersion {
            expected: MANIFEST_SCHEMA_V1,
            found: schema_version,
        });
    }

    let manifest = parse_manifest_snapshot_v1(json)?;
    Ok(ManifestSnapshot::V1(manifest))
}

/// Parse delta fact with schema-version dispatch.
pub fn parse_delta_fact(json: &str) -> Result<DeltaFact, MetadataError> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    let schema_version = extract_schema_version(&value)?;
    if schema_version != DELTA_SCHEMA_V1 {
        return Err(MetadataError::UnsupportedSchemaVersion {
            expected: DELTA_SCHEMA_V1,
            found: schema_version,
        });
    }

    let fact = parse_delta_fact_v1(json)?;
    Ok(DeltaFact::V1(fact))
}

fn extract_schema_version(value: &serde_json::Value) -> Result<u16, MetadataError> {
    let schema =
        value.get("schema_version").ok_or(MetadataError::MissingField("schema_version"))?;
    let schema_u64 = schema.as_u64().ok_or(MetadataError::WrongFieldType("schema_version"))?;
    let schema_u16 =
        u16::try_from(schema_u64).map_err(|_| MetadataError::WrongFieldType("schema_version"))?;
    Ok(schema_u16)
}

/// Canonical key used for first-writer-wins replay.
pub type DeltaLogicalKey = (String, String, String);

/// Conflict row emitted when a later fact loses due to different payload digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaConflictRow {
    pub tenant_id: String,
    pub fingerprint: String,
    pub prefix_hash: String,
    pub winner_payload_digest: String,
    pub loser_payload_digest: String,
    pub winner_write_id: String,
    pub loser_write_id: String,
    pub winner_epoch_ms: u64,
    pub loser_epoch_ms: u64,
    pub winner_seq: u64,
    pub loser_seq: u64,
}

/// Deterministic replay observability counters for duplicate handling.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaReplayStats {
    pub winners: usize,
    pub duplicates_idempotent: usize,
    pub duplicates_conflict: usize,
    pub conflict_rows: Vec<DeltaConflictRow>,
}

/// Deterministic replay output containing winners and observability stats.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaReplayOutcome {
    pub winners: Vec<DeltaFactV1>,
    pub stats: DeltaReplayStats,
}

/// Deterministic replay for delta facts with observability counters.
///
/// Rules:
/// - order candidates by `(epoch_ms, seq, write_id)`
/// - first fact per logical key wins
/// - same-digest duplicates are dropped as idempotent
/// - different-digest later facts are ignored (conflict losers)
#[must_use]
pub fn replay_delta_facts_with_observability(deltas: &[DeltaFactV1]) -> DeltaReplayOutcome {
    let mut ordered = deltas.to_vec();
    ordered.sort_by(|lhs, rhs| {
        (lhs.epoch_ms, lhs.seq, &lhs.write_id).cmp(&(rhs.epoch_ms, rhs.seq, &rhs.write_id))
    });

    let mut winners: BTreeMap<DeltaLogicalKey, DeltaFactV1> = BTreeMap::new();
    let mut stats = DeltaReplayStats::default();

    for delta in ordered {
        let key = (delta.tenant_id.clone(), delta.fingerprint.clone(), delta.prefix_hash.clone());
        match winners.entry(key.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(delta);
            }
            Entry::Occupied(slot) => {
                let winner = slot.get();
                if winner.payload_digest == delta.payload_digest {
                    stats.duplicates_idempotent = stats.duplicates_idempotent.saturating_add(1);
                } else {
                    stats.duplicates_conflict = stats.duplicates_conflict.saturating_add(1);
                    stats.conflict_rows.push(DeltaConflictRow {
                        tenant_id: key.0,
                        fingerprint: key.1,
                        prefix_hash: key.2,
                        winner_payload_digest: winner.payload_digest.clone(),
                        loser_payload_digest: delta.payload_digest,
                        winner_write_id: winner.write_id.clone(),
                        loser_write_id: delta.write_id,
                        winner_epoch_ms: winner.epoch_ms,
                        loser_epoch_ms: delta.epoch_ms,
                        winner_seq: winner.seq,
                        loser_seq: delta.seq,
                    });
                }
            }
        }
    }

    let winners = winners.into_values().collect::<Vec<_>>();
    stats.winners = winners.len();
    DeltaReplayOutcome { winners, stats }
}

/// Deterministic replay for delta facts with idempotent dedupe.
///
/// Rules:
/// - order candidates by `(epoch_ms, seq, write_id)`
/// - first fact per logical key wins
/// - same-digest duplicates are dropped as idempotent
/// - different-digest later facts are ignored (conflict losers)
#[must_use]
pub fn replay_delta_facts_deterministic(deltas: &[DeltaFactV1]) -> Vec<DeltaFactV1> {
    replay_delta_facts_with_observability(deltas).winners
}

#[cfg(test)]
mod tests {
    use super::{
        parse_delta_fact, parse_delta_fact_v1, parse_manifest_snapshot, parse_manifest_snapshot_v1,
        replay_delta_facts_deterministic, replay_delta_facts_with_observability, DeltaFact,
        DeltaFactV1, ManifestSnapshot, MetadataError,
    };

    #[test]
    fn manifest_missing_required_field_fails() {
        let json = r#"{"epoch_ms":1700,"checksum32":1,"entries":[]}"#;
        let error = parse_manifest_snapshot_v1(json).expect_err("missing schema_version");
        assert!(matches!(error, MetadataError::Json(_)));
    }

    #[test]
    fn delta_missing_required_field_fails() {
        let json = r#"{"schema_version":1,"epoch_ms":1700,"seq":1,"fingerprint":"f","prefix_hash":"h","payload_digest":"d","write_id":"w"}"#;
        let error = parse_delta_fact_v1(json).expect_err("missing tenant_id");
        assert!(matches!(error, MetadataError::Json(_)));
    }

    #[test]
    fn valid_manifest_and_delta_parse() {
        let manifest_json = r#"{
            "schema_version":1,
            "epoch_ms":1700,
            "checksum32":42,
            "entries":[{"object_id":"seg-1","key_range_start":"a","key_range_end":"z"}]
        }"#;
        let delta_json = r#"{
            "schema_version":1,
            "epoch_ms":1700,
            "seq":1,
            "tenant_id":"t1",
            "fingerprint":"f1",
            "prefix_hash":"h1",
            "payload_digest":"d1",
            "write_id":"w1"
        }"#;

        assert!(parse_manifest_snapshot_v1(manifest_json).is_ok());
        assert!(parse_delta_fact_v1(delta_json).is_ok());
    }

    #[test]
    fn manifest_with_additive_unknown_fields_is_accepted() {
        let manifest_json = r#"{
            "schema_version":1,
            "epoch_ms":1700,
            "checksum32":42,
            "producer":"wombatkv-node",
            "entries":[{"object_id":"seg-1","key_range_start":"a","key_range_end":"z","extra":"ignored"}],
            "future_extension":{"mode":"v2"}
        }"#;
        assert!(parse_manifest_snapshot_v1(manifest_json).is_ok());
    }

    #[test]
    fn delta_with_additive_unknown_fields_is_accepted() {
        let delta_json = r#"{
            "schema_version":1,
            "epoch_ms":1700,
            "seq":1,
            "tenant_id":"t1",
            "fingerprint":"f1",
            "prefix_hash":"h1",
            "payload_digest":"d1",
            "write_id":"w1",
            "trace_id":"abc123",
            "extra_meta":{"producer":"wombatkv-node"}
        }"#;
        assert!(parse_delta_fact_v1(delta_json).is_ok());
    }

    #[test]
    fn replay_order_and_dedupe_is_deterministic() {
        let a_conflict_winner = DeltaFactV1 {
            schema_version: 1,
            epoch_ms: 100,
            seq: 1,
            tenant_id: "t1".to_string(),
            fingerprint: "f1".to_string(),
            prefix_hash: "h1".to_string(),
            payload_digest: "digest-a".to_string(),
            write_id: "w1".to_string(),
        };
        let a_conflict_loser = DeltaFactV1 {
            schema_version: 1,
            epoch_ms: 100,
            seq: 2,
            tenant_id: "t1".to_string(),
            fingerprint: "f1".to_string(),
            prefix_hash: "h1".to_string(),
            payload_digest: "digest-b".to_string(),
            write_id: "w2".to_string(),
        };
        let a_idempotent_duplicate = DeltaFactV1 {
            schema_version: 1,
            epoch_ms: 101,
            seq: 1,
            tenant_id: "t1".to_string(),
            fingerprint: "f1".to_string(),
            prefix_hash: "h1".to_string(),
            payload_digest: "digest-a".to_string(),
            write_id: "w3".to_string(),
        };
        let b_unique = DeltaFactV1 {
            schema_version: 1,
            epoch_ms: 90,
            seq: 1,
            tenant_id: "t2".to_string(),
            fingerprint: "f2".to_string(),
            prefix_hash: "h2".to_string(),
            payload_digest: "digest-c".to_string(),
            write_id: "w4".to_string(),
        };

        let first_input = vec![
            a_conflict_loser.clone(),
            b_unique.clone(),
            a_idempotent_duplicate.clone(),
            a_conflict_winner.clone(),
        ];
        let second_input = vec![
            a_conflict_winner.clone(),
            a_idempotent_duplicate,
            b_unique.clone(),
            a_conflict_loser,
        ];

        let first = replay_delta_facts_deterministic(&first_input);
        let second = replay_delta_facts_deterministic(&second_input);
        let first_observed = replay_delta_facts_with_observability(&first_input);
        let second_observed = replay_delta_facts_with_observability(&second_input);

        assert_eq!(first, second);
        assert_eq!(first, vec![a_conflict_winner, b_unique]);
        assert_eq!(first, first_observed.winners);
        assert_eq!(first_observed, second_observed);
        assert_eq!(first_observed.stats.winners, 2);
        assert_eq!(first_observed.stats.duplicates_idempotent, 1);
        assert_eq!(first_observed.stats.duplicates_conflict, 1);
        assert_eq!(first_observed.stats.conflict_rows.len(), 1);
    }

    #[test]
    fn replay_idempotent_duplicates_do_not_emit_conflict_rows() {
        let winner = DeltaFactV1 {
            schema_version: 1,
            epoch_ms: 100,
            seq: 1,
            tenant_id: "t1".to_string(),
            fingerprint: "f1".to_string(),
            prefix_hash: "h1".to_string(),
            payload_digest: "digest-a".to_string(),
            write_id: "w1".to_string(),
        };
        let duplicate = DeltaFactV1 {
            schema_version: 1,
            epoch_ms: 101,
            seq: 1,
            tenant_id: "t1".to_string(),
            fingerprint: "f1".to_string(),
            prefix_hash: "h1".to_string(),
            payload_digest: "digest-a".to_string(),
            write_id: "w2".to_string(),
        };

        let observed = replay_delta_facts_with_observability(&[winner.clone(), duplicate]);
        assert_eq!(observed.winners, vec![winner]);
        assert_eq!(observed.stats.winners, 1);
        assert_eq!(observed.stats.duplicates_idempotent, 1);
        assert_eq!(observed.stats.duplicates_conflict, 0);
        assert!(observed.stats.conflict_rows.is_empty());
    }

    #[test]
    fn version_dispatch_accepts_v1_payloads() {
        let manifest_json = r#"{
            "schema_version":1,
            "epoch_ms":1700,
            "checksum32":42,
            "entries":[{"object_id":"seg-1","key_range_start":"a","key_range_end":"z"}]
        }"#;
        let delta_json = r#"{
            "schema_version":1,
            "epoch_ms":1700,
            "seq":1,
            "tenant_id":"t1",
            "fingerprint":"f1",
            "prefix_hash":"h1",
            "payload_digest":"d1",
            "write_id":"w1"
        }"#;

        let manifest = parse_manifest_snapshot(manifest_json).expect("manifest");
        let delta = parse_delta_fact(delta_json).expect("delta");
        assert!(matches!(manifest, ManifestSnapshot::V1(_)));
        assert!(matches!(delta, DeltaFact::V1(_)));
    }

    #[test]
    fn version_dispatch_rejects_unsupported_versions() {
        let manifest_json = r#"{
            "schema_version":2,
            "epoch_ms":1700,
            "checksum32":42,
            "entries":[]
        }"#;
        let delta_json = r#"{
            "schema_version":2,
            "epoch_ms":1700,
            "seq":1,
            "tenant_id":"t1",
            "fingerprint":"f1",
            "prefix_hash":"h1",
            "payload_digest":"d1",
            "write_id":"w1"
        }"#;

        let manifest_error = parse_manifest_snapshot(manifest_json).expect_err("unsupported");
        let delta_error = parse_delta_fact(delta_json).expect_err("unsupported");
        assert_eq!(
            manifest_error,
            MetadataError::UnsupportedSchemaVersion { expected: 1, found: 2 }
        );
        assert_eq!(delta_error, MetadataError::UnsupportedSchemaVersion { expected: 1, found: 2 });
    }
}
