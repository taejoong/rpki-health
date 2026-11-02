#!/bin/bash
# rpki_cycle.sh — RPKI ETL pipeline based on rpki-client, run every 20 minutes from cron
# Usage: ./rpki_cycle.sh
# Cron:  */20 * * * * cd /path/to/rpki-parser && ./rpki_cycle.sh >> ~/cron_rpki_cycle.log 2>&1

set -euo pipefail

# ── flock: exit at once if the previous cycle is still running ──
exec 9>/tmp/rpki_cycle.lock
flock -n 9 || { echo "$(date -u +'%Y-%m-%d %H:%M:%S') Previous cycle still running, skipping"; exit 0; }

# ── Settings ──
DATASET_TS=$(date -u +'%Y-%m-%d %H:%M:%S')
CACHE_DIR="/ssd-data/rpki-validator/cache"
VRP_OUTPUT="/ssd-data/rpki-validator/output"
# Keep rpki-client's per-file validation stderr (input to the validity-map builder)
RC_STDERR_LOG="/ssd-data/rpki-validator/rpki-client.stderr.log"
WORK_DIR="$(dirname "$(readlink -f "$0")")"   # this checkout
# rpki-client: the system binary (rpki-client-portable 9.7 on the production host); set RPKI_CLIENT
# in the environment to run another build. A portable build's compiled-in default TAL path points to
# its build prefix, so the TAL + constraints files in /etc/tals are passed explicitly with -t.
RPKI_CLIENT="${RPKI_CLIENT:-/usr/sbin/rpki-client}"
TAL_DIR="${TAL_DIR:-/etc/tals}"
KV_PATH="warehouse-shared/kv.sqlite"

PYTHON="${PYTHON:-${WORK_DIR}/.venv/bin/python}"
# One line per finished cycle, read by the cycle monitor (not part of this repository)
STATUS_LOG="${STATUS_LOG:-$HOME/rpki_cycle_status.log}"
IO_WORKERS="${IO_WORKERS:-24}"
PARSE_WORKERS="${PARSE_WORKERS:-32}"
ZSTD_LEVEL="${ZSTD_LEVEL:-1}"
FLUSH_ROWS="${FLUSH_ROWS:-250000}"

# Publication point hostname → TAL ID mapping
# Matches the .rrdp/<hash>/<hostname>/ and rsync/<hostname>/ layout of the rpki-client cache
declare -A TAL_MAP=(
  # RIPENCC
  ["rpki.ripe.net"]="RIPENCC"
  ["rsync.paas.rpki.ripe.net"]="RIPENCC"
  # APNIC (+ delegated NIRs)
  ["rpki.apnic.net"]="APNIC"
  ["rpki.sub.apnic.net"]="APNIC"
  ["rpki-repository.nic.ad.jp"]="APNIC"
  ["akane.maru.co.jp"]="APNIC"
  ["repo-rpki.idnic.net"]="APNIC"
  ["rpkica.twnic.tw"]="APNIC"
  ["rpki-rps.cnnic.cn"]="APNIC"
  # CERNET FITI sub-CA (rsync://rsync.fiti.net.cn/repo/fiti-sub002/, ~4,180 ROAs, ~12,480 VRPs) since 2026-05-14.
  # Its manifest's AIA is rsync.cernet.net, which is itself only resolved via AIA, so the one-hop lookup
  # never found it and the cycle dropped the whole repository.
  ["rsync.fiti.net.cn"]="APNIC"
  # ARIN (+ delegated)
  ["rpki.arin.net"]="ARIN"
  ["rpki-rps.arin.net"]="ARIN"
  ["rpki-rsync.us-east-2.amazonaws.com"]="ARIN"
  ["rpki.multacom.com"]="ARIN"
  ["rpki-01.pdxnet.uk"]="ARIN"
  # LACNIC
  ["repository.lacnic.net"]="LACNIC"
  ["rpki.lacnic.net"]="LACNIC"
  ["rpki-repo.registro.br"]="LACNIC"
  # AFRINIC
  ["rpki.afrinic.net"]="AFRINIC"
  # Delegated / third-party publication points
  # (AIA verified: repo.rpki.space→ripe, cloudie-repo→arin)
  ["repo.rpki.space"]="RIPENCC"
  ["cloudie-repo.rpki.app"]="ARIN"
  ["rpki.roa.net"]="RIPENCC"
  ["rpki.admin.freerangecloud.com"]="RIPENCC"
  ["rpki-01.pdxnet.uk"]="RIPENCC"
  ["rpki.pudu.be"]="RIPENCC"
  ["rpki.nellicus.net"]="RIPENCC"
  ["rpki.sunoaki.net"]="RIPENCC"
  ["rpki.cc"]="RIPENCC"
)

echo "=== Cycle start: ${DATASET_TS} ==="

# ── Step 1: run rpki-client (all TALs, delta update) ──
# -d: cache directory (stores the DER files); positional arg: VRP output
echo "Running rpki-client..."
# 9.8 needs the output directory to exist beforehand (9.0 creates it). Create it as a safety net.
mkdir -p "${VRP_OUTPUT}"
# Name the five RIR TALs explicitly (the adjacent .constraints files load automatically, as in 9.0's default behavior).
RC_TAL_ARGS=()
for _tal in afrinic apnic arin lacnic ripe; do
  RC_TAL_ARGS+=(-t "${TAL_DIR}/${_tal}.tal")
done
# Keep the full stderr in a log file (it includes the per-file validation results); print only a 5-line summary.
# A redirect instead of a pipe lets rpki-client's exit code reach set -e unchanged.
"${RPKI_CLIENT}" -v "${RC_TAL_ARGS[@]}" -d "${CACHE_DIR}" "${VRP_OUTPUT}" 2> "${RC_STDERR_LOG}"
tail -5 "${RC_STDERR_LOG}"
echo "rpki-client done: $(date -u +'%Y-%m-%d %H:%M:%S')"

# ── Step 1.5: build the validity map (RP-rejected objects → object_hash sidecar) ──
# Parse the per-file rejection lines in rpki-client's stderr into a SHA-256 map of the invalid objects.
# Each cycle, the ETL uses this map to write only the invalid objects (present in this cycle) to the object_validity table.
# Fail-safe: if the builder fails, write an empty map, so no object_validity rows are written (all valid) → collection never stops.
VALIDITY_MAP="/ssd-data/rpki-validator/validity-map.json"
echo "Building validity map..."
if ! "$PYTHON" "${WORK_DIR}/build_validity_map.py" \
      --stderr-log "${RC_STDERR_LOG}" \
      --cache-dir "${CACHE_DIR}" \
      --output "${VALIDITY_MAP}"; then
  echo "WARN: validity-map build failed; writing empty map (ETL falls back to all-valid)"
  echo "{}" > "${VALIDITY_MAP}"
fi

# ── Step 2: create per-TAL staging directories (symlinks) ──
# rpki-client cache layout:
#   .rrdp/<hash>/<hostname>/...  (objects fetched over RRDP)
#   <hostname>/...               (objects fetched over rsync)
#   ta/                          (trust anchor certificates)
STAGING="/tmp/rpki-tal-staging-$$"
mkdir -p "$STAGING"
trap 'rm -rf "$STAGING"' EXIT

# Find the TAL of an unknown hostname from AIA at run time
resolve_tal_from_aia() {
  local host_dir="$1"
  # $2: AIA hops still allowed when the parent host is not in TAL_MAP (default 0 = one-hop lookup only).
  local hops="${2:-0}"
  local aia_host=""
  local sample

  # 1) Parse a .cer file directly
  sample=$(find -L "$host_dir" -name "*.cer" -type f 2>/dev/null | head -1)
  if [ -n "$sample" ]; then
    aia_host=$(openssl x509 -in "$sample" -inform DER -noout -text 2>/dev/null \
      | grep -A1 "Authority Information" \
      | grep -oP '(?<=rsync://)[^/]+' | head -1)
  fi

  # 2) No .cer: extract the EE cert from a .mft, then parse its AIA
  if [ -z "$aia_host" ]; then
    sample=$(find -L "$host_dir" -name "*.mft" -type f 2>/dev/null | head -1)
    if [ -n "$sample" ]; then
      local tmp_cer
      tmp_cer=$(mktemp)
      openssl cms -verify -noverify -inform DER -in "$sample" \
        -certsout "$tmp_cer" -out /dev/null 2>/dev/null
      aia_host=$(openssl x509 -in "$tmp_cer" -noout -text 2>/dev/null \
        | grep -A1 "Authority Information" \
        | grep -oP '(?<=rsync://)[^/]+' | head -1)
      rm -f "$tmp_cer"
    fi
  fi

  # 3) If the .mft fails too, try a .roa
  if [ -z "$aia_host" ]; then
    sample=$(find -L "$host_dir" -name "*.roa" -type f 2>/dev/null | head -1)
    if [ -n "$sample" ]; then
      local tmp_cer
      tmp_cer=$(mktemp)
      openssl cms -verify -noverify -inform DER -in "$sample" \
        -certsout "$tmp_cer" -out /dev/null 2>/dev/null
      aia_host=$(openssl x509 -in "$tmp_cer" -noout -text 2>/dev/null \
        | grep -A1 "Authority Information" \
        | grep -oP '(?<=rsync://)[^/]+' | head -1)
      rm -f "$tmp_cer"
    fi
  fi

  if [ -z "$aia_host" ]; then
    echo ""
    return 0
  fi
  # Look up the TAL by the AIA parent hostname
  local tal="${TAL_MAP[$aia_host]:-}"
  # If the parent host is not in TAL_MAP, go up one more level, to the parent's cache directory (the validated tree).
  # Example: rsync.fiti.net.cn -> rsync.cernet.net -> rpki.apnic.net (APNIC)
  if [ -z "$tal" ] && [ "$hops" -gt 0 ] \
     && [[ "$aia_host" =~ ^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$ ]] \
     && [ "$aia_host" != "$(basename "$host_dir")" ] \
     && [ -d "${CACHE_DIR}/${aia_host}" ]; then
    tal=$(resolve_tal_from_aia "${CACHE_DIR}/${aia_host}" $((hops - 1)))
  fi
  echo "$tal"
}

# Classify the hostnames in the RRDP directories → TAL
for hash_dir in "${CACHE_DIR}/.rrdp"/*/; do
  [ -d "$hash_dir" ] || continue
  for host_dir in "${hash_dir}"*/; do
    [ -d "$host_dir" ] || continue
    hostname=$(basename "$host_dir")
    TAL_ID="${TAL_MAP[$hostname]:-}"

    # Not in the map: find it from AIA
    if [ -z "$TAL_ID" ]; then
      TAL_ID=$(resolve_tal_from_aia "$host_dir")
      if [ -n "$TAL_ID" ]; then
        echo "  Resolved ${hostname} → ${TAL_ID} (via AIA)"
      fi
    fi

    if [ -n "$TAL_ID" ]; then
      tal_lower=$(echo "$TAL_ID" | tr '[:upper:]' '[:lower:]')
      hash_short=$(basename "$hash_dir" | head -c 8)
      mkdir -p "$STAGING/$tal_lower"
      ln -sf "$host_dir" "$STAGING/$tal_lower/rrdp-${hostname}-${hash_short}"
    else
      echo "  Unresolved publication point: ${hostname}"
    fi
  done
done

# Classify the rsync directories too (fallback when RRDP fails)
for rsync_dir in "${CACHE_DIR}"/*/; do
  [ -d "$rsync_dir" ] || continue
  hostname=$(basename "$rsync_dir")
  [[ "$hostname" == ".rrdp" || "$hostname" == "ta" ]] && continue
  TAL_ID="${TAL_MAP[$hostname]:-}"
  if [ -z "$TAL_ID" ]; then
    # Only in the validated tree (<cache>/<host>/), follow the AIA chain up to 2 more hops (.rrdp leftovers keep the one-hop lookup).
    TAL_ID=$(resolve_tal_from_aia "$rsync_dir" 2)
    if [ -n "$TAL_ID" ]; then
      echo "  Resolved rsync ${hostname} → ${TAL_ID} (via AIA)"
    fi
  fi
  if [ -n "$TAL_ID" ]; then
    tal_lower=$(echo "$TAL_ID" | tr '[:upper:]' '[:lower:]')
    mkdir -p "$STAGING/$tal_lower"
    ln -sf "$rsync_dir" "$STAGING/$tal_lower/rsync-${hostname}"
  else
    echo "  Unresolved rsync publication point: ${hostname}"
  fi
done

# ── Step 3: Per-TAL ETL ──
cd "${WORK_DIR}"

PROCESSED=0
SKIPPED_HOSTS=0

for tal_dir in "$STAGING"/*/; do
  [ -d "$tal_dir" ] || continue

  TAL_LOWER=$(basename "$tal_dir")
  TAL_UPPER=$(echo "$TAL_LOWER" | tr '[:lower:]' '[:upper:]')
  OUT_ROOT="warehouse-${TAL_LOWER}"
  CAS_ROOT="${OUT_ROOT}/cas-sqlite"

  # Count the files (skip empty directories)
  FILE_COUNT=$(find -L "$tal_dir" -type f 2>/dev/null | wc -l)
  if [ "$FILE_COUNT" -eq 0 ]; then
    echo "  Skip ${TAL_UPPER}: no files"
    continue
  fi

  echo "  ETL: ${TAL_UPPER} (${FILE_COUNT} files)"
  "$PYTHON" rpki_etl.py single \
    --input "${tal_dir}" \
    --dataset-ts "${DATASET_TS}" \
    --repo-id "rpki-client" \
    --tal-id "${TAL_UPPER}" \
    --transport rrdp \
    --session-id 0 --serial 0 \
    --out-root "${OUT_ROOT}" \
    --kv "${KV_PATH}" \
    --io-workers "${IO_WORKERS}" --parse-workers "${PARSE_WORKERS}" \
    --zstd-level "${ZSTD_LEVEL}" \
    --cas-backend sqlite --cas-mode bytes \
    --cas-sqlite-root "${CAS_ROOT}" --cas-sqlite-shards 256 \
    --validity-map "${VALIDITY_MAP}" \
    --flush-rows "${FLUSH_ROWS}" --skip-first-last --verbose

  PROCESSED=$((PROCESSED + 1))
done

DONE_TS=$(date -u +'%Y-%m-%d %H:%M:%S')
echo "=== Cycle done: ${DONE_TS} | tals=${PROCESSED} ==="

# Append to the status file (read by the cycle monitor, not part of this repository)
echo "${DONE_TS} ok tals=${PROCESSED}" >> "${STATUS_LOG}"
