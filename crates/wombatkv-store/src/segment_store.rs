#![forbid(unsafe_code)]

use wombatkv_format::{decode_segment_index, FormatError};

use crate::wal_store::{InMemoryObjectStore, ObjectStore, WalStoreError};

/// Errors for segment range-read operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SegmentStoreError {
    ObjectStore(WalStoreError),
    Format(FormatError),
    InvalidRange,
}

impl From<WalStoreError> for SegmentStoreError {
    fn from(error: WalStoreError) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<FormatError> for SegmentStoreError {
    fn from(error: FormatError) -> Self {
        Self::Format(error)
    }
}

/// Range-read response with bounded object-store round-trip accounting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeReadResult {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub round_trips: u8,
}

/// Zero-copy entry view over a shared payload window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowEntry {
    pub key: Vec<u8>,
    window_offset: usize,
    payload_len: usize,
}

/// Range-read window that avoids per-entry payload cloning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeReadWindow {
    payload_window: Vec<u8>,
    entries: Vec<WindowEntry>,
    pub round_trips: u8,
}

impl RangeReadWindow {
    #[must_use]
    pub fn entries(&self) -> &[WindowEntry] {
        &self.entries
    }

    #[must_use]
    pub fn payload(&self, index: usize) -> Option<&[u8]> {
        let entry = self.entries.get(index)?;
        let end = entry.window_offset.checked_add(entry.payload_len)?;
        self.payload_window.get(entry.window_offset..end)
    }

    #[must_use]
    pub fn payload_window(&self) -> &[u8] {
        &self.payload_window
    }
}

/// Segment-store helper for range extraction from compacted segment objects.
pub struct SegmentStore<S: ObjectStore = InMemoryObjectStore> {
    object_store: S,
}

impl<S: ObjectStore> SegmentStore<S> {
    #[must_use]
    pub fn new(object_store: S) -> Self {
        Self { object_store }
    }

    #[must_use]
    pub const fn round_trip_budget_bound() -> u8 {
        2
    }

    pub fn range_read(
        &self,
        object_key: &str,
        key_start: &[u8],
        key_end: &[u8],
    ) -> Result<RangeReadResult, SegmentStoreError> {
        let window = self.range_read_window(object_key, key_start, key_end)?;
        let mut entries = Vec::with_capacity(window.entries.len());
        for (idx, entry) in window.entries.iter().enumerate() {
            let payload =
                window.payload(idx).ok_or(SegmentStoreError::Format(FormatError::UnexpectedEof))?;
            entries.push((entry.key.clone(), payload.to_vec()));
        }
        Ok(RangeReadResult { entries, round_trips: window.round_trips })
    }

    pub fn range_read_window(
        &self,
        object_key: &str,
        key_start: &[u8],
        key_end: &[u8],
    ) -> Result<RangeReadWindow, SegmentStoreError> {
        if key_start > key_end {
            return Err(SegmentStoreError::InvalidRange);
        }

        let object = self.object_store.get_object(object_key)?;
        let index = decode_segment_index(&object)?;
        let selected = index
            .into_iter()
            .filter(|entry| entry.key.as_slice() >= key_start && entry.key.as_slice() <= key_end)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Ok(RangeReadWindow {
                payload_window: Vec::new(),
                entries: Vec::new(),
                round_trips: 1,
            });
        }

        let min_offset = selected
            .iter()
            .map(|entry| usize::try_from(entry.payload_offset).unwrap_or(usize::MAX))
            .min()
            .ok_or(SegmentStoreError::Format(FormatError::UnexpectedEof))?;
        let max_end = selected
            .iter()
            .map(|entry| {
                usize::try_from(entry.payload_offset)
                    .unwrap_or(usize::MAX)
                    .saturating_add(usize::try_from(entry.payload_len).unwrap_or(usize::MAX))
            })
            .max()
            .ok_or(SegmentStoreError::Format(FormatError::UnexpectedEof))?;

        let payload_window = self.object_store.get_object_range(object_key, min_offset, max_end)?;
        let mut entries = Vec::with_capacity(selected.len());
        for entry in selected {
            let start = usize::try_from(entry.payload_offset)
                .unwrap_or(usize::MAX)
                .saturating_sub(min_offset);
            let end =
                start.saturating_add(usize::try_from(entry.payload_len).unwrap_or(usize::MAX));
            if end > payload_window.len() {
                return Err(SegmentStoreError::Format(FormatError::UnexpectedEof));
            }
            entries.push(WindowEntry {
                key: entry.key,
                window_offset: start,
                payload_len: usize::try_from(entry.payload_len).unwrap_or(usize::MAX),
            });
        }

        Ok(RangeReadWindow { payload_window, entries, round_trips: 2 })
    }
}

#[cfg(test)]
mod tests {
    use super::{SegmentStore, SegmentStoreError};
    use crate::wal_store::{InMemoryObjectStore, ObjectStore, S3ObjectStore, S3ObjectStoreConfig};
    use wombatkv_format::encode_segment_v1;

    #[test]
    fn range_read_extracts_expected_keys_and_payloads() {
        let store = InMemoryObjectStore::default();
        let segment = encode_segment_v1(&[
            (b"a".to_vec(), b"payload-a".to_vec()),
            (b"b".to_vec(), b"payload-b".to_vec()),
            (b"c".to_vec(), b"payload-c".to_vec()),
        ])
        .expect("encode segment");
        store.put_object("segments/tenant-a/seg-1.bin", &segment).expect("put segment");

        let segment_store = SegmentStore::new(store);
        let result = segment_store
            .range_read("segments/tenant-a/seg-1.bin", b"b", b"c")
            .expect("range read");
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].0, b"b".to_vec());
        assert_eq!(result.entries[0].1, b"payload-b".to_vec());
        assert_eq!(result.entries[1].0, b"c".to_vec());
        assert_eq!(result.entries[1].1, b"payload-c".to_vec());
    }

    #[test]
    fn round_trip_budget_is_bounded_for_compacted_segment() {
        let store = InMemoryObjectStore::default();
        let segment = encode_segment_v1(&[
            (b"a".to_vec(), b"payload-a".to_vec()),
            (b"b".to_vec(), b"payload-b".to_vec()),
            (b"c".to_vec(), b"payload-c".to_vec()),
        ])
        .expect("encode segment");
        store.put_object("segments/tenant-a/seg-2.bin", &segment).expect("put segment");

        let segment_store = SegmentStore::new(store);
        let result = segment_store
            .range_read("segments/tenant-a/seg-2.bin", b"a", b"c")
            .expect("range read");
        assert!(
            result.round_trips <= SegmentStore::<InMemoryObjectStore>::round_trip_budget_bound()
        );
    }

    #[test]
    fn corrupt_footer_or_index_is_rejected() {
        let store = InMemoryObjectStore::default();
        let mut segment =
            encode_segment_v1(&[(b"a".to_vec(), b"payload-a".to_vec())]).expect("encode segment");
        let footer_start = segment.len() - 16;
        segment[footer_start] ^= 0xFF;
        store.put_object("segments/tenant-a/seg-corrupt.bin", &segment).expect("put segment");

        let segment_store = SegmentStore::new(store);
        let result = segment_store.range_read("segments/tenant-a/seg-corrupt.bin", b"a", b"a");
        assert!(matches!(result, Err(SegmentStoreError::Format(_))));
    }

    #[test]
    fn range_read_window_reuses_shared_payload_buffer() {
        let store = InMemoryObjectStore::default();
        let segment = encode_segment_v1(&[
            (b"a".to_vec(), b"payload-a".to_vec()),
            (b"b".to_vec(), b"payload-b".to_vec()),
            (b"c".to_vec(), b"payload-c".to_vec()),
        ])
        .expect("encode segment");
        store.put_object("segments/tenant-a/seg-window.bin", &segment).expect("put segment");

        let segment_store = SegmentStore::new(store);
        let window = segment_store
            .range_read_window("segments/tenant-a/seg-window.bin", b"a", b"c")
            .expect("range read window");
        assert_eq!(window.entries().len(), 3);
        assert_eq!(window.payload(0), Some("payload-a".as_bytes()));
        assert_eq!(window.payload(1), Some("payload-b".as_bytes()));
        assert_eq!(window.payload(2), Some("payload-c".as_bytes()));

        let base = window.payload_window().as_ptr() as usize;
        let limit = base.saturating_add(window.payload_window().len());
        for idx in 0..window.entries().len() {
            let payload = window.payload(idx).expect("payload");
            let payload_ptr = payload.as_ptr() as usize;
            assert!(payload_ptr >= base && payload_ptr < limit);
        }
    }

    fn run_live_minio_tests() -> bool {
        matches!(std::env::var("WMBT_KV_TEST_MINIO_INTEGRATION"), Ok(v) if v == "1")
    }

    fn live_s3_store() -> S3ObjectStore {
        let cfg = S3ObjectStoreConfig::from_env().expect("env config");
        let store = S3ObjectStore::new(cfg).expect("s3 store init");
        store.ensure_bucket().expect("ensure bucket");
        store
    }

    #[test]
    fn segment_range_read_with_s3_minio_live() {
        if !run_live_minio_tests() {
            return;
        }

        let store = live_s3_store();
        let key = "object-store/live/segments/seg-range.bin";
        let segment = encode_segment_v1(&[
            (b"a".to_vec(), b"payload-a".to_vec()),
            (b"b".to_vec(), b"payload-b".to_vec()),
            (b"c".to_vec(), b"payload-c".to_vec()),
        ])
        .expect("encode segment");
        store.put_object(key, &segment).expect("put segment");

        let segment_store = SegmentStore::new(store.clone());
        let result = segment_store.range_read(key, b"b", b"c").expect("range read");
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].0, b"b".to_vec());
        assert_eq!(result.entries[0].1, b"payload-b".to_vec());
        assert_eq!(result.entries[1].0, b"c".to_vec());
        assert_eq!(result.entries[1].1, b"payload-c".to_vec());

        let _ = store.delete_object(key);
    }
}
