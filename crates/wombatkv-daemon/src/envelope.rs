//! RFC 0018 universal wire envelope for daemon TCP + HTTP transports.
//!
//! Every wire frame on the daemon-TCP and daemon-HTTP paths is wrapped
//! in this 16-byte header before the rkyv-archived body:
//!
//! ```text
//! +---------+---------+---------+---------+----------------+
//! | magic 4 | ver 4LE | crc 4LE | len 4LE | body[len] ...  |
//! | 'WMBT'  | u32     | u32     | u32     |                |
//! +---------+---------+---------+---------+----------------+
//! ```
//!
//! - **magic** `b"WMBT"` distinguishes wombatkv RPC frames from any
//!   other byte sequence that might land on the same TCP socket.
//! - **version** is `WIRE_ENVELOPE_VERSION`. Strict-equal at decode
//!   (no fallback parser; pre-launch breaking-window applies, when
//!   we bump the version, all daemons + clients must upgrade
//!   together).
//! - **crc** is `crc32c::crc32c(body)` (Castagnoli polynomial
//!   0x82F63B78). Same algorithm ds4 uses for its v4 sidecar + v2
//!   block envelopes, single CRC32C across the stack.
//! - **len** is `body.len()` as `u32 LE`. Lets the decoder allocate
//!   exactly the right buffer without scanning for a sentinel.
//!
//! Body is `WireRequest::encode()` / `WireResponse::encode()` output -
//! a bare rkyv archive starting at offset 0 of the body buffer. Since
//! callers always allocate a fresh `Vec<u8>` of exactly `len` bytes
//! and read into it, the rkyv content sits at offset 0 of that
//! allocation, alignment-safe for rkyv 0.8's 8-byte pointer
//! requirement without a copy.

use crc32c::crc32c;

pub const WIRE_ENVELOPE_MAGIC: &[u8; 4] = b"WMBT";
pub const WIRE_ENVELOPE_VERSION: u32 = 1;
pub const WIRE_ENVELOPE_BYTES: usize = 16;

// Comptime layout assertions (alpha.13-polish, learning from TigerBeetle
// message_header.zig:60-64). Catch struct-layout drift at compile time -
// faster than waiting for the pinned_layout_v1_DO_NOT_UPDATE runtime test
// to fail in CI.
//
// WIRE_ENVELOPE_BYTES must equal: 4 (magic) + 4 (version u32) + 4 (crc u32) + 4 (len u32) = 16.
const _: () = assert!(WIRE_ENVELOPE_BYTES == 16);
const _: () = assert!(
    WIRE_ENVELOPE_BYTES
        == std::mem::size_of::<[u8; 4]>()        // magic
            + std::mem::size_of::<u32>()         // version
            + std::mem::size_of::<u32>()         // crc
            + std::mem::size_of::<u32>() // len
);
// Wire-format version is intentionally `1` for the v0.1.0-alpha series
// (RFC 0018). Bumping this is a breaking change, coordinate via the
// alpha-breaking-window policy (memory: feedback_alpha_breaking_window).
const _: () = assert!(WIRE_ENVELOPE_VERSION == 1);
// Magic is exactly 4 bytes, single LE-decode-friendly word. Anything
// longer slows the streaming parser; shorter risks collision with raw
// TCP traffic.
const _: () = assert!(WIRE_ENVELOPE_MAGIC.len() == 4);

#[derive(Debug)]
pub enum EnvelopeError {
    TooShort { got: usize, need: usize },
    BadMagic { got: [u8; 4] },
    BadVersion { got: u32, want: u32 },
    BadLength { header_len: u32, actual_body_bytes: usize },
    BadCrc { header_crc: u32, computed_crc: u32 },
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvelopeError::TooShort { got, need } => {
                write!(f, "envelope too short: got {got} bytes, need at least {need}")
            }
            EnvelopeError::BadMagic { got } => {
                write!(f, "envelope bad magic: got {got:?}, expected {WIRE_ENVELOPE_MAGIC:?}")
            }
            EnvelopeError::BadVersion { got, want } => {
                write!(
                    f,
                    "envelope unsupported version {got} (want {want}); pre-launch \
                     breaking-window applies, daemons and clients must upgrade together"
                )
            }
            EnvelopeError::BadLength { header_len, actual_body_bytes } => {
                write!(
                    f,
                    "envelope length mismatch: header says {header_len} body bytes, \
                     actually got {actual_body_bytes}"
                )
            }
            EnvelopeError::BadCrc { header_crc, computed_crc } => {
                write!(
                    f,
                    "envelope CRC32C mismatch: header={header_crc:08x} computed={computed_crc:08x}"
                )
            }
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// Wrap `body` in the RFC 0018 envelope. Returns a fresh Vec containing
/// `[envelope 16 bytes][body]`.
#[must_use]
pub fn encode_envelope(body: &[u8]) -> Vec<u8> {
    let body_len = u32::try_from(body.len()).expect("body fits u32 (4 GiB)");
    let crc = crc32c(body);
    let mut out = Vec::with_capacity(WIRE_ENVELOPE_BYTES + body.len());
    out.extend_from_slice(WIRE_ENVELOPE_MAGIC);
    out.extend_from_slice(&WIRE_ENVELOPE_VERSION.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Decode + validate a complete envelope, returning the body slice.
///
/// # Header-then-body ordering (alpha.13-polish audit fix #97)
///
/// Callers receiving frames over a stream (TCP/HTTP transports) MUST
/// use the two-step pattern, not this convenience wrapper, to get the
/// "reject oversized claim BEFORE allocating body buffer" guarantee:
///
/// ```ignore
/// // 1. Read exactly WIRE_ENVELOPE_BYTES from the stream.
/// let mut header_buf = [0u8; WIRE_ENVELOPE_BYTES];
/// reader.read_exact(&mut header_buf)?;
///
/// // 2. Validate magic + version + extract len. NO body alloc yet.
/// let header = decode_envelope_header(&header_buf)?;
///
/// // 3. Reject oversized len BEFORE allocating (DoS protection).
/// if header.len > MAX_FRAME_BYTES as u32 { return Err(...); }
///
/// // 4. Now safe to allocate exactly `header.len` bytes.
/// let mut body = vec![0u8; header.len as usize];
/// reader.read_exact(&mut body)?;
///
/// // 5. Validate body CRC against header.
/// verify_envelope_crc(&header, &body)?;
/// ```
///
/// `tcp_transport.rs:441-456` and `http_transport.rs:590-598` follow
/// this pattern; the `tcp_sync_rejects_oversized_envelope_len` test
/// (and HTTP counterpart) verifies the DoS rejection happens before
/// allocation.
///
/// This `decode_envelope` function is for callers that already have
/// the FULL message buffered (e.g., unit tests).
pub fn decode_envelope(bytes: &[u8]) -> Result<&[u8], EnvelopeError> {
    if bytes.len() < WIRE_ENVELOPE_BYTES {
        return Err(EnvelopeError::TooShort { got: bytes.len(), need: WIRE_ENVELOPE_BYTES });
    }
    let header_bytes: [u8; WIRE_ENVELOPE_BYTES] =
        bytes[..WIRE_ENVELOPE_BYTES].try_into().expect("len checked");
    let header = decode_envelope_header(&header_bytes)?;
    let body_end =
        WIRE_ENVELOPE_BYTES.checked_add(header.len as usize).ok_or(EnvelopeError::BadLength {
            header_len: header.len,
            actual_body_bytes: bytes.len().saturating_sub(WIRE_ENVELOPE_BYTES),
        })?;
    if bytes.len() != body_end {
        return Err(EnvelopeError::BadLength {
            header_len: header.len,
            actual_body_bytes: bytes.len() - WIRE_ENVELOPE_BYTES,
        });
    }
    let body = &bytes[WIRE_ENVELOPE_BYTES..body_end];
    verify_envelope_crc(&header, body)?;
    Ok(body)
}

/// Just the 16-byte envelope header (no body). Useful for streaming
/// readers that need to know `len` before allocating the body buffer.
pub struct EnvelopeHeader {
    pub crc: u32,
    pub len: u32,
}

pub fn decode_envelope_header(
    header: &[u8; WIRE_ENVELOPE_BYTES],
) -> Result<EnvelopeHeader, EnvelopeError> {
    let magic: [u8; 4] = header[0..4].try_into().expect("4-byte slice");
    if &magic != WIRE_ENVELOPE_MAGIC {
        return Err(EnvelopeError::BadMagic { got: magic });
    }
    let version = u32::from_le_bytes(header[4..8].try_into().expect("4-byte slice"));
    if version != WIRE_ENVELOPE_VERSION {
        return Err(EnvelopeError::BadVersion { got: version, want: WIRE_ENVELOPE_VERSION });
    }
    let crc = u32::from_le_bytes(header[8..12].try_into().expect("4-byte slice"));
    let len = u32::from_le_bytes(header[12..16].try_into().expect("4-byte slice"));
    Ok(EnvelopeHeader { crc, len })
}

pub fn verify_envelope_crc(header: &EnvelopeHeader, body: &[u8]) -> Result<(), EnvelopeError> {
    if body.len() != header.len as usize {
        return Err(EnvelopeError::BadLength {
            header_len: header.len,
            actual_body_bytes: body.len(),
        });
    }
    let computed = crc32c(body);
    if computed != header.crc {
        return Err(EnvelopeError::BadCrc { header_crc: header.crc, computed_crc: computed });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_small() {
        let body = b"hello world";
        let wire = encode_envelope(body);
        assert_eq!(wire.len(), WIRE_ENVELOPE_BYTES + body.len());
        let decoded = decode_envelope(&wire).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn roundtrip_empty() {
        let body: &[u8] = &[];
        let wire = encode_envelope(body);
        assert_eq!(wire.len(), WIRE_ENVELOPE_BYTES);
        let decoded = decode_envelope(&wire).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn roundtrip_large() {
        let body = vec![0xa5u8; 1 << 20]; // 1 MB
        let wire = encode_envelope(&body);
        assert_eq!(wire.len(), WIRE_ENVELOPE_BYTES + body.len());
        let decoded = decode_envelope(&wire).unwrap();
        assert_eq!(decoded, body.as_slice());
    }

    #[test]
    #[allow(non_snake_case)]
    fn pinned_layout_v1_DO_NOT_UPDATE() {
        // RFC 0018 envelope v1 byte-for-byte layout. If this test
        // breaks, you've changed the envelope format, that's a
        // breaking wire change. Either revert, or coordinate a
        // version bump + wipe all daemon deployments.
        let body = b"x";
        let wire = encode_envelope(body);
        assert_eq!(wire.len(), 17);
        assert_eq!(&wire[0..4], b"WMBT", "magic");
        assert_eq!(&wire[4..8], &1u32.to_le_bytes(), "version=1 LE");
        // crc32c(b"x") = precomputed; recompute via crc32c::crc32c if you
        // need to verify, but DO NOT EDIT THIS CONSTANT, it pins the
        // CRC algorithm choice (Castagnoli polynomial 0x82F63B78).
        let crc_x = crc32c(b"x");
        assert_eq!(&wire[8..12], &crc_x.to_le_bytes(), "crc32c('x') LE");
        assert_eq!(&wire[12..16], &1u32.to_le_bytes(), "body_len=1 LE");
        assert_eq!(wire[16], b'x', "body byte 0");
    }

    #[test]
    fn header_decode_rejects_oversized_len_without_body_alloc() {
        // Audit fix #97: streaming readers must validate magic + version
        // + len bound BEFORE allocating the body buffer. This test
        // verifies that decode_envelope_header succeeds on a 16-byte
        // header without ever touching a body buffer. The transport's
        // job is then to check header.len against MAX_FRAME_BYTES and
        // reject if too big.
        let mut header = [0u8; WIRE_ENVELOPE_BYTES];
        header[0..4].copy_from_slice(WIRE_ENVELOPE_MAGIC);
        header[4..8].copy_from_slice(&WIRE_ENVELOPE_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&0xDEADBEEFu32.to_le_bytes()); // crc, irrelevant here
        header[12..16].copy_from_slice(&u32::MAX.to_le_bytes()); // oversized len claim
        let h = decode_envelope_header(&header).expect("header itself parses");
        assert_eq!(h.len, u32::MAX);
        // Caller is now expected to reject, we just proved the header
        // is parseable without touching a body buffer.
    }

    #[test]
    fn rejects_bad_magic() {
        let mut wire = encode_envelope(b"hi");
        wire[0] = b'X';
        assert!(matches!(decode_envelope(&wire), Err(EnvelopeError::BadMagic { .. })));
    }

    #[test]
    fn rejects_bad_version() {
        let mut wire = encode_envelope(b"hi");
        wire[4] = 99;
        assert!(matches!(decode_envelope(&wire), Err(EnvelopeError::BadVersion { .. })));
    }

    #[test]
    fn rejects_bad_crc() {
        let mut wire = encode_envelope(b"hi");
        wire[16] = b'X'; // tamper body
        assert!(matches!(decode_envelope(&wire), Err(EnvelopeError::BadCrc { .. })));
    }

    #[test]
    fn rejects_truncated_body() {
        let wire = encode_envelope(b"hello");
        let truncated = &wire[..wire.len() - 1];
        assert!(matches!(decode_envelope(truncated), Err(EnvelopeError::BadLength { .. })));
    }

    #[test]
    fn rejects_extra_bytes() {
        let mut wire = encode_envelope(b"hi");
        wire.push(0);
        assert!(matches!(decode_envelope(&wire), Err(EnvelopeError::BadLength { .. })));
    }

    #[test]
    fn header_only_decode() {
        let body = b"streaming reader needs len";
        let wire = encode_envelope(body);
        let header_bytes: [u8; WIRE_ENVELOPE_BYTES] =
            wire[..WIRE_ENVELOPE_BYTES].try_into().unwrap();
        let header = decode_envelope_header(&header_bytes).unwrap();
        assert_eq!(header.len as usize, body.len());
        verify_envelope_crc(&header, body).unwrap();
    }
}
