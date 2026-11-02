#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Config you may tweak
PYTHON="${PYTHON:-python}"
DATA_ROOT="${DATA_ROOT:-/ssd-data/rpki-objects/unpacked}"
KV_PATH="${KV_PATH:-warehouse-shared/kv.sqlite}"
REPO_ID="${REPO_ID:-ftp.ripe.net}"
IO_WORKERS="${IO_WORKERS:-24}"
PARSE_WORKERS="${PARSE_WORKERS:-32}"
ZSTD_LEVEL="${ZSTD_LEVEL:-1}"
ZSTD_THREADS="${ZSTD_THREADS:-0}"
FORCE_TIME="${FORCE_TIME:-00:00:00}"
EXCLUDE_DIR="${EXCLUDE_DIR:-unvalidated}"

usage() {
  echo "Usage: $0 TAL PERIOD"
  echo "  TAL: ARIN|APNIC|AFRINIC|LACNIC|RIPENCC (case-insensitive)"
  echo "  PERIOD: YYYY or YYYY/MM or YYYY-MM or YYYY-MM-DD"
  echo
  echo "Env overrides: DATA_ROOT, KV_PATH, REPO_ID, IO_WORKERS, PARSE_WORKERS, ZSTD_LEVEL, ZSTD_THREADS, FORCE_TIME, EXCLUDE_DIR"
  exit 1
}

[[ $# -ne 2 ]] && usage

# Normalize TAL
TAL_RAW="$1"
TAL_UPPER="$(echo "$TAL_RAW" | tr '[:lower:]' '[:upper:]')"
TAL_LOWER="$(echo "$TAL_RAW" | tr '[:upper:]' '[:lower:]')"

# Normalize period
PERIOD="$2"
DAY=""  # Initialize DAY variable

if [[ "$PERIOD" =~ ^([0-9]{4})$ ]]; then
  YEAR="${BASH_REMATCH[1]}"
  MONTHS=(01 02 03 04 05 06 07 08 09 10 11 12)
elif [[ "$PERIOD" =~ ^([0-9]{4})/([0-9]{2})$ ]]; then
  YEAR="${BASH_REMATCH[1]}"
  MONTHS=("${BASH_REMATCH[2]}")
elif [[ "$PERIOD" =~ ^([0-9]{4})-([0-9]{2})$ ]]; then
  YEAR="${BASH_REMATCH[1]}"
  MONTHS=("${BASH_REMATCH[2]}")
# --- [Added] Support for YYYY/MM/DD ---
elif [[ "$PERIOD" =~ ^([0-9]{4})/([0-9]{2})/([0-9]{2})$ ]]; then
  YEAR="${BASH_REMATCH[1]}"
  MONTHS=("${BASH_REMATCH[2]}")
  DAY="${BASH_REMATCH[3]}"
# --- [Added] Support for YYYY-MM-DD ---
elif [[ "$PERIOD" =~ ^([0-9]{4})-([0-9]{2})-([0-9]{2})$ ]]; then
  YEAR="${BASH_REMATCH[1]}"
  MONTHS=("${BASH_REMATCH[2]}")
  DAY="${BASH_REMATCH[3]}"
else
  echo "Error: PERIOD must be YYYY, YYYY-MM, or YYYY-MM-DD" >&2
  exit 2
fi

# Paths
OUT_ROOT="warehouse-${TAL_LOWER}"
CAS_SQLITE_ROOT="${OUT_ROOT}/cas-sqlite"

# Make sure rpki_etl.py is available
if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "Error: python not found" >&2
  exit 3
fi
if [[ ! -f "rpki_etl.py" ]]; then
  echo "Error: rpki_etl.py not found in current dir" >&2
  exit 3
fi

echo "TAL=${TAL_UPPER} period=${YEAR}/${MONTHS[*]} day=${DAY:-ALL} out_root=${OUT_ROOT}"
echo "DATA_ROOT=${DATA_ROOT} KV_PATH=${KV_PATH}"


for MON in "${MONTHS[@]}"; do
  INPUT_ROOT="${DATA_ROOT}/${TAL_LOWER}/${TAL_LOWER}.tal/${YEAR}/${MON}"
  
  # --- [Added] If specific day is requested, append it to the path ---
  if [[ -n "$DAY" ]]; then
    INPUT_ROOT="${INPUT_ROOT}/${DAY}"
  fi

  if [[ ! -d "${INPUT_ROOT}" ]]; then
    echo "Skip: no such input dir ${INPUT_ROOT}"
    continue
  fi

  echo "Running ETL: TAL=${TAL_UPPER} ${YEAR}/${MON}${DAY:+/$DAY}"
  set -x
  "$PYTHON" rpki_etl.py bulk \
    --input-root "${INPUT_ROOT}" \
    --repo-id "${REPO_ID}" --tal-id "${TAL_UPPER}" \
    --transport rrdp --session-id 0 --serial 0 \
    --out-root "${OUT_ROOT}" --kv "${KV_PATH}" \
    --io-workers "${IO_WORKERS}" --parse-workers "${PARSE_WORKERS}" \
    --zstd-level "${ZSTD_LEVEL}" --zstd-threads "${ZSTD_THREADS}" \
    --force-time "${FORCE_TIME}" --exclude-dir "${EXCLUDE_DIR}" \
    --skip-existing-membership \
    --cas-backend sqlite --cas-mode path \
    --cas-sqlite-root "${CAS_SQLITE_ROOT}" --cas-sqlite-shards 256 \
    --flush-rows 250000 --verbose
  set +x
done

echo "Done."