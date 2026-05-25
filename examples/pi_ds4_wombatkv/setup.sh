#!/usr/bin/env bash
# Idempotent setup for pi-ds4 + WombatKV.
#
#   - Pulls the wombatkv/ds4 Docker image (or skips if already present).
#   - Extracts the ds4-server binary to ~/.wombatkv/bin/ds4-server.
#   - Prints the 5 environment variables you should export.
#
# Does NOT modify your shell rc files. Does NOT start any processes.
# You wire it into your shell or pi invocation yourself, see README.md.

set -euo pipefail

IMAGE="${WOMBATKV_DS4_IMAGE:-wombatkv/ds4:latest}"
DEST_DIR="${WOMBATKV_BIN_DIR:-$HOME/.wombatkv/bin}"
DEST_BIN="$DEST_DIR/ds4-server"

info() { printf '  \033[36m%s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m%s\033[0m\n' "$*"; }
warn() { printf '  \033[33m%s\033[0m\n' "$*" >&2; }
err()  { printf '  \033[31m%s\033[0m\n' "$*" >&2; }

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "missing required command: $1"
    exit 1
  fi
}

require_cmd docker
require_cmd uname

info "ensuring ${DEST_DIR} exists"
mkdir -p "$DEST_DIR"

# Pull the image if not already present.
if docker image inspect "$IMAGE" >/dev/null 2>&1; then
  ok "image ${IMAGE} already present"
else
  info "pulling ${IMAGE} (this may take a minute)"
  docker pull "$IMAGE"
fi

# Extract the binary. The image's working dir contains ds4-server.
info "extracting ds4-server -> ${DEST_BIN}"
CID="$(docker create "$IMAGE")"
trap 'docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT
docker cp "$CID:/ds4/ds4-server" "$DEST_BIN"
chmod +x "$DEST_BIN"
docker rm -f "$CID" >/dev/null 2>&1 || true
trap - EXIT

if [[ ! -x "$DEST_BIN" ]]; then
  err "extracted binary is not executable at ${DEST_BIN}"
  exit 1
fi

# Sanity check the binary loads.
if "$DEST_BIN" --help >/dev/null 2>&1 || "$DEST_BIN" -h >/dev/null 2>&1; then
  ok "ds4-server binary ready at ${DEST_BIN}"
else
  warn "ds4-server extracted but --help failed, may still work; check architecture"
  warn "  host: $(uname -sm)"
  warn "  image platform: $(docker inspect "$IMAGE" --format '{{.Architecture}}/{{.Os}}' 2>/dev/null || echo unknown)"
fi

cat <<EOF

  ✓ Setup complete. Add these to your shell (or ~/.zshrc):

      export DS4_SERVER_BINARY="${DEST_BIN}"
      export DS4_WOMBATKV_ENABLE=1
      export WMBT_KV_BUCKET="wombatkv-\${USER}"
      export WMBT_KV_S3_ENDPOINT="http://localhost:9000"
      export WMBT_KV_S3_ACCESS_KEY="minio"
      export WMBT_KV_S3_SECRET_KEY="minio123"

  Then start MinIO if you don't already have it running, and launch \`pi\`.
  See README.md for the verification commands.

EOF
