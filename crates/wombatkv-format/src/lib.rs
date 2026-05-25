#![forbid(unsafe_code)]

/// Returns crate identity for smoke tests.
#[must_use]
pub fn crate_id() -> &'static str {
    "wombatkv-format"
}

const WAL_MAGIC: &[u8; 12] = b"WMBT_KV_WAL1";
const WAL_HEADER_SIZE: usize = 24;
const SEGMENT_FOOTER_MAGIC: &[u8; 12] = b"WMBT_KV_SEG1";
const SEGMENT_FOOTER_SIZE: usize = 20;
const SEGMENT_MAGIC: &[u8; 12] = b"WMBT_KV_SGV1";
const SEGMENT_HEADER_SIZE: usize = 16;

/// WAL record v1 used for append-only chunk payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecord {
    pub key: Vec<u8>,
    pub meta: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Binary format parsing and encoding errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatError {
    InvalidMagic,
    UnexpectedEof,
    ChecksumMismatch,
    FieldTooLarge,
}

/// Encodes a single `WalRecord` using `WMBT_KV_WAL1` layout.
pub fn encode_wal_record(record: &WalRecord) -> Result<Vec<u8>, FormatError> {
    let key_len = u16::try_from(record.key.len()).map_err(|_| FormatError::FieldTooLarge)?;
    let meta_len = u16::try_from(record.meta.len()).map_err(|_| FormatError::FieldTooLarge)?;
    let payload_len =
        u32::try_from(record.payload.len()).map_err(|_| FormatError::FieldTooLarge)?;

    let mut body = Vec::with_capacity(
        usize::from(key_len) + usize::from(meta_len) + usize::try_from(payload_len).unwrap_or(0),
    );
    body.extend_from_slice(&record.key);
    body.extend_from_slice(&record.meta);
    body.extend_from_slice(&record.payload);

    let checksum = checksum32(&body);

    let mut out = Vec::with_capacity(WAL_HEADER_SIZE + body.len());
    out.extend_from_slice(WAL_MAGIC);
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&meta_len.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decodes a single `WalRecord` from binary bytes.
pub fn decode_wal_record(data: &[u8]) -> Result<WalRecord, FormatError> {
    if data.len() < WAL_HEADER_SIZE {
        return Err(FormatError::UnexpectedEof);
    }

    if &data[..WAL_MAGIC.len()] != WAL_MAGIC {
        return Err(FormatError::InvalidMagic);
    }

    let key_len = usize::from(u16::from_le_bytes([data[12], data[13]]));
    let meta_len = usize::from(u16::from_le_bytes([data[14], data[15]]));
    let payload_len_u32 = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
    let payload_len = usize::try_from(payload_len_u32).map_err(|_| FormatError::FieldTooLarge)?;
    let checksum = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);

    let body_len = key_len
        .checked_add(meta_len)
        .and_then(|x| x.checked_add(payload_len))
        .ok_or(FormatError::FieldTooLarge)?;

    let total_len = WAL_HEADER_SIZE.checked_add(body_len).ok_or(FormatError::FieldTooLarge)?;
    if data.len() < total_len {
        return Err(FormatError::UnexpectedEof);
    }

    let body = &data[WAL_HEADER_SIZE..total_len];
    if checksum32(body) != checksum {
        return Err(FormatError::ChecksumMismatch);
    }

    let key_end = key_len;
    let meta_end = key_end + meta_len;
    Ok(WalRecord {
        key: body[..key_end].to_vec(),
        meta: body[key_end..meta_end].to_vec(),
        payload: body[meta_end..].to_vec(),
    })
}

/// Lightweight deterministic checksum for v1 scaffolding tests.
#[must_use]
pub fn checksum32(bytes: &[u8]) -> u32 {
    let mut state = 0x811c_9dc5_u32;
    for byte in bytes {
        state ^= u32::from(*byte);
        state = state.wrapping_mul(16_777_619);
    }
    state
}

/// Segment footer v1 for validating index footer integrity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentFooter {
    pub index_len: u32,
    pub index_checksum: u32,
}

/// Encodes a v1 segment footer.
#[must_use]
pub fn encode_segment_footer(footer: SegmentFooter) -> [u8; SEGMENT_FOOTER_SIZE] {
    let mut out = [0_u8; SEGMENT_FOOTER_SIZE];
    out[..12].copy_from_slice(SEGMENT_FOOTER_MAGIC);
    out[12..16].copy_from_slice(&footer.index_len.to_le_bytes());
    out[16..20].copy_from_slice(&footer.index_checksum.to_le_bytes());
    out
}

/// Parses and validates a v1 segment footer and index payload.
pub fn decode_segment_footer(
    footer_bytes: &[u8],
    index_bytes: &[u8],
) -> Result<SegmentFooter, FormatError> {
    if footer_bytes.len() < SEGMENT_FOOTER_SIZE {
        return Err(FormatError::UnexpectedEof);
    }

    if &footer_bytes[..12] != SEGMENT_FOOTER_MAGIC {
        return Err(FormatError::InvalidMagic);
    }

    let index_len = u32::from_le_bytes([
        footer_bytes[12],
        footer_bytes[13],
        footer_bytes[14],
        footer_bytes[15],
    ]);
    let index_checksum = u32::from_le_bytes([
        footer_bytes[16],
        footer_bytes[17],
        footer_bytes[18],
        footer_bytes[19],
    ]);

    if usize::try_from(index_len).map_err(|_| FormatError::FieldTooLarge)? != index_bytes.len() {
        return Err(FormatError::UnexpectedEof);
    }

    if checksum32(index_bytes) != index_checksum {
        return Err(FormatError::ChecksumMismatch);
    }

    Ok(SegmentFooter { index_len, index_checksum })
}

/// Segment index entry mapping key -> payload byte range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentIndexEntry {
    pub key: Vec<u8>,
    pub payload_offset: u32,
    pub payload_len: u32,
}

/// Encode compact segment object with sparse index + footer.
pub fn encode_segment_v1(blocks: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<u8>, FormatError> {
    let block_count = u32::try_from(blocks.len()).map_err(|_| FormatError::FieldTooLarge)?;
    let mut out = Vec::new();
    out.extend_from_slice(SEGMENT_MAGIC);
    out.extend_from_slice(&block_count.to_le_bytes());

    let mut index_entries = Vec::with_capacity(blocks.len());
    for (key, payload) in blocks {
        let key_len = u16::try_from(key.len()).map_err(|_| FormatError::FieldTooLarge)?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| FormatError::FieldTooLarge)?;
        let payload_offset = u32::try_from(out.len() + 2 + 4 + usize::from(key_len))
            .map_err(|_| FormatError::FieldTooLarge)?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(payload);
        index_entries.push(SegmentIndexEntry { key: key.clone(), payload_offset, payload_len });
    }

    let index_bytes = encode_segment_index(&index_entries)?;
    let footer = encode_segment_footer(SegmentFooter {
        index_len: u32::try_from(index_bytes.len()).map_err(|_| FormatError::FieldTooLarge)?,
        index_checksum: checksum32(&index_bytes),
    });
    out.extend_from_slice(&index_bytes);
    out.extend_from_slice(&footer);
    Ok(out)
}

/// Decode segment index from a full segment object.
pub fn decode_segment_index(segment: &[u8]) -> Result<Vec<SegmentIndexEntry>, FormatError> {
    if segment.len() < SEGMENT_HEADER_SIZE + SEGMENT_FOOTER_SIZE {
        return Err(FormatError::UnexpectedEof);
    }
    if &segment[..SEGMENT_MAGIC.len()] != SEGMENT_MAGIC {
        return Err(FormatError::InvalidMagic);
    }

    let footer_start =
        segment.len().checked_sub(SEGMENT_FOOTER_SIZE).ok_or(FormatError::UnexpectedEof)?;
    let footer_bytes = &segment[footer_start..];
    let index_len_u32 = u32::from_le_bytes([
        footer_bytes[12],
        footer_bytes[13],
        footer_bytes[14],
        footer_bytes[15],
    ]);
    let index_len = usize::try_from(index_len_u32).map_err(|_| FormatError::FieldTooLarge)?;
    let index_start = footer_start.checked_sub(index_len).ok_or(FormatError::UnexpectedEof)?;
    let index_bytes = &segment[index_start..footer_start];
    let _ = decode_segment_footer(footer_bytes, index_bytes)?;
    parse_segment_index(index_bytes)
}

fn encode_segment_index(entries: &[SegmentIndexEntry]) -> Result<Vec<u8>, FormatError> {
    let mut out = Vec::new();
    for entry in entries {
        let key_len = u16::try_from(entry.key.len()).map_err(|_| FormatError::FieldTooLarge)?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&entry.key);
        out.extend_from_slice(&entry.payload_offset.to_le_bytes());
        out.extend_from_slice(&entry.payload_len.to_le_bytes());
    }
    Ok(out)
}

fn parse_segment_index(index_bytes: &[u8]) -> Result<Vec<SegmentIndexEntry>, FormatError> {
    let mut cursor = 0_usize;
    let mut entries = Vec::new();
    while cursor < index_bytes.len() {
        if index_bytes.len().saturating_sub(cursor) < 2 {
            return Err(FormatError::UnexpectedEof);
        }
        let key_len =
            usize::from(u16::from_le_bytes([index_bytes[cursor], index_bytes[cursor + 1]]));
        cursor = cursor.saturating_add(2);
        if index_bytes.len().saturating_sub(cursor) < key_len + 8 {
            return Err(FormatError::UnexpectedEof);
        }
        let key = index_bytes[cursor..cursor + key_len].to_vec();
        cursor = cursor.saturating_add(key_len);
        let payload_offset = u32::from_le_bytes([
            index_bytes[cursor],
            index_bytes[cursor + 1],
            index_bytes[cursor + 2],
            index_bytes[cursor + 3],
        ]);
        cursor = cursor.saturating_add(4);
        let payload_len = u32::from_le_bytes([
            index_bytes[cursor],
            index_bytes[cursor + 1],
            index_bytes[cursor + 2],
            index_bytes[cursor + 3],
        ]);
        cursor = cursor.saturating_add(4);
        entries.push(SegmentIndexEntry { key, payload_offset, payload_len });
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::{
        checksum32, decode_segment_footer, decode_segment_index, decode_wal_record,
        encode_segment_footer, encode_segment_v1, encode_wal_record, FormatError, SegmentFooter,
        WalRecord,
    };

    #[test]
    fn crate_id_is_stable() {
        assert_eq!(super::crate_id(), "wombatkv-format");
    }

    #[test]
    fn wal_record_round_trip_is_lossless() {
        let record = WalRecord {
            key: b"k/tenant/f/hash".to_vec(),
            meta: b"{\"epoch\":1700}".to_vec(),
            payload: vec![1, 2, 3, 4, 5, 6],
        };
        let encoded = encode_wal_record(&record).expect("encode");
        let decoded = decode_wal_record(&encoded).expect("decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn wal_checksum_mismatch_is_rejected() {
        let record = WalRecord {
            key: b"key".to_vec(),
            meta: b"meta".to_vec(),
            payload: b"payload".to_vec(),
        };
        let mut encoded = encode_wal_record(&record).expect("encode");
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        assert_eq!(decode_wal_record(&encoded), Err(FormatError::ChecksumMismatch));
    }

    #[test]
    fn wal_golden_vector_v1_stable() {
        let record = WalRecord {
            key: vec![0x6B],                 // "k"
            meta: vec![0x6D],                // "m"
            payload: vec![0x70, 0x71, 0x72], // "pqr"
        };
        let encoded = encode_wal_record(&record).expect("encode");
        let expected = include_bytes!("../tests/golden/wal_v1_record.bin");
        assert_eq!(encoded, expected);
    }

    #[test]
    fn segment_footer_round_trip_with_checksum_validation() {
        let index_bytes = b"index-v1".to_vec();
        let index_len = u32::try_from(index_bytes.len()).expect("small test index");
        let footer = SegmentFooter { index_len, index_checksum: checksum32(&index_bytes) };
        let encoded = encode_segment_footer(footer);
        let decoded = decode_segment_footer(&encoded, &index_bytes).expect("decode footer");
        assert_eq!(decoded, footer);
    }

    #[test]
    fn segment_footer_rejects_corrupt_index_checksum() {
        let index_bytes = b"index-v1".to_vec();
        let index_len = u32::try_from(index_bytes.len()).expect("small test index");
        let footer = SegmentFooter { index_len, index_checksum: checksum32(&index_bytes) };
        let encoded = encode_segment_footer(footer);
        let mut corrupted_index = index_bytes.clone();
        corrupted_index[0] ^= 0xFF;
        assert_eq!(
            decode_segment_footer(&encoded, &corrupted_index),
            Err(FormatError::ChecksumMismatch)
        );
    }

    #[test]
    fn segment_footer_rejects_unknown_magic_for_backward_compat() {
        let index_bytes = b"index-v1".to_vec();
        let index_len = u32::try_from(index_bytes.len()).expect("small test index");
        let mut encoded = encode_segment_footer(SegmentFooter {
            index_len,
            index_checksum: checksum32(&index_bytes),
        });
        encoded[0] = b'X';
        assert_eq!(decode_segment_footer(&encoded, &index_bytes), Err(FormatError::InvalidMagic));
    }

    #[test]
    fn segment_v1_encode_decode_index_round_trip() {
        let segment = encode_segment_v1(&[
            (b"a".to_vec(), b"payload-a".to_vec()),
            (b"b".to_vec(), b"payload-b".to_vec()),
            (b"c".to_vec(), b"payload-c".to_vec()),
        ])
        .expect("encode segment");
        let index = decode_segment_index(&segment).expect("decode index");
        assert_eq!(index.len(), 3);
        assert_eq!(index[0].key, b"a".to_vec());
        assert_eq!(index[1].key, b"b".to_vec());
        assert_eq!(index[2].key, b"c".to_vec());
    }
}
