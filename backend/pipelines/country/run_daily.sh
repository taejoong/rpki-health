#!/usr/bin/env bash
# Country ROA deployment pipeline: daily run (cron entry point; see README.md).
#
#   usage: run_daily.sh [--root DIR] [--jobs N] [--trailing N]
#
#   0. remove leftovers of killed runs (this pipeline's own temp names only)
#   1. refresh inputs   vrp_subtal (only if missing), vrp_export, ripe_archive, vrp_merge, deleg, asnames
#   2. BGP pairs        every snapshot day not fetched yet (snapshot_pairs --catch-up)
#   3. per-date data    the trailing 32 grid dates up to the latest pairs day + every missing grid date
#                       (VRP bridging closes gaps of up to 31 days once the VRPs come back)
#   4. build            assemble -> builds/<stamp>, `current` swapped atomically
#   5. one summary line on stdout "<time> country-rpki daily: <STATUS> in <s>s; ...":
#        OK       exit 0  published; every step succeeded and assemble reported no warnings
#        PARTIAL  exit 1  published, but an input refresh or snapshot_pairs failed (the previous
#                         files were used) or assemble reported freshness warnings (as_of older
#                         than yesterday, a VRP source that ends before as_of, missing dates, ...)
#        FAILED   exit 2  compute_day or assemble failed, or the script stopped; `current` unchanged
#      Another run holding ROOT/.lock: one line on stderr, exit 75, nothing done.
#
# Environment (the options win): COUNTRY_RPKI_ROOT (default /ssd-data/country-rpki), PY (pipeline
# python, default this checkout's .venv), SUBTAL_PY (python with pyroaring for the one-off
# vrp_subtal step, default the .venv of the parser next to this checkout: an rpki-parser checkout,
# or parser/ in the rpki-health repository), JOBS (parallel dates, 2), TRAILING (grid dates
# recomputed every day, 32 = the longest bridged VRP gap + 1).
set -euo pipefail

HERE=$(dirname "$(readlink -f "$0")")   # pipelines/country of this checkout
ROOT=${COUNTRY_RPKI_ROOT:-/ssd-data/country-rpki}
PY=${PY:-$HERE/../../.venv/bin/python}
PARSER_DIR=$HERE/../../../rpki-parser   # the first of the two that exists, as in app/config.py
if [[ ! -d $PARSER_DIR && -d $HERE/../../../parser ]]; then PARSER_DIR=$HERE/../../../parser; fi
SUBTAL_PY=${SUBTAL_PY:-$PARSER_DIR/.venv/bin/python}
JOBS=${JOBS:-2}
TRAILING=${TRAILING:-32}
usage() { echo "usage: $0 [--root DIR] [--jobs N] [--trailing N]" >&2; exit 64; }
while (( $# )); do
    case $1 in
        --root) (( $# >= 2 )) || usage; ROOT=$2; shift 2 ;;
        --root=*) ROOT=${1#*=}; shift ;;
        --jobs) (( $# >= 2 )) || usage; JOBS=$2; shift 2 ;;
        --trailing) (( $# >= 2 )) || usage; TRAILING=$2; shift 2 ;;
        -h|--help) usage ;;
        *) echo "run_daily: unknown argument: $1" >&2; usage ;;
    esac
done
[[ $JOBS =~ ^[1-9][0-9]*$ && $TRAILING =~ ^[0-9]+$ ]] || usage
# absolute paths before the cd below (the modules run from pipelines/country): a relative root
# would otherwise land inside the code checkout. -s keeps symlinks: a venv's bin/python is a
# symlink, and the resolved interpreter would not see the venv's packages.
ROOT=$(realpath -ms -- "$ROOT")
if [[ $PY == */* ]]; then PY=$(realpath -ms -- "$PY"); fi
if [[ $SUBTAL_PY == */* ]]; then SUBTAL_PY=$(realpath -ms -- "$SUBTAL_PY"); fi
export COUNTRY_RPKI_ROOT=$ROOT
cd "$HERE"

mkdir -p "$ROOT"
exec 9>"$ROOT/.lock"
if ! flock -n 9; then
    echo "$(date -u +%FT%TZ) country-rpki daily: another run holds $ROOT/.lock; skipped" >&2
    exit 75
fi
renice -n 10 -p $$ >/dev/null 2>&1 || true   # children inherit nice 10

log() { echo "$(date -u +%FT%TZ) run_daily: $*" >&2; }
START=$SECONDS
STEP=setup
DONE=0
FAILED=()
finish() {   # EXIT trap: a run that stops anywhere still ends with one summary line
    local rc=$? also=''
    (( DONE )) && return
    (( ${#FAILED[@]} )) && also="; soft failures before: ${FAILED[*]}"
    echo "$(date -u +%FT%TZ) country-rpki daily: FAILED in $((SECONDS - START))s at $STEP (exit $rc);" \
         "current build unchanged$also"
    exit 2
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
soft() {   # soft <name> <command...>: a failed input refresh is reported, the run goes on
    local name=$1 rc=0; shift
    STEP=$name
    log "start $name"
    "$@" >&2 || rc=$?          # step output goes to the log; stdout carries only the summary line
    if (( rc == 0 )); then
        log "done $name"
    else
        FAILED+=("$name")
        log "WARNING $name failed (exit $rc); continuing with the previous files"
    fi
}
hard() {   # hard <name> <command...>: a failure ends the run (FAILED, exit 2)
    STEP=$1; shift
    log "start $STEP"
    "$@" >&2
    log "done $STEP"
}

command -v "$PY" >/dev/null || { log "ERROR: PY=$PY is not an executable python"; exit 2; }
mkdir -p "$ROOT"/{inputs,pairs,daily,builds,logs,tmp}

# 0. leftovers of runs that were killed: only this pipeline's own temp names, nothing else in ROOT/tmp
STEP=cleanup
find "$ROOT/daily" "$ROOT/pairs" -mindepth 2 -maxdepth 2 \( -name '.tmp-*' -o -name '.old-*' \) \
     -mmin +360 -prune -exec rm -rf -- {} + 2>/dev/null || true
if [[ -d $ROOT/tmp/snap_raw ]]; then   # raw snapshot downloads of days that never completed
    find "$ROOT/tmp/snap_raw" -mindepth 1 -maxdepth 1 -type d -mtime +2 -prune -exec rm -rf -- {} + || true
fi
for d in "$ROOT"/tmp/duckdb-*; do      # DuckDB spill directories of processes that are gone
    [[ -d $d ]] || continue
    pid=${d##*/duckdb-}; pid=${pid%%-*}
    [[ $pid =~ ^[0-9]+$ && -e /proc/$pid ]] && continue
    rm -rf -- "$d"
done

# 1. inputs (vrp_export holds the shared presence lock itself and refuses 00:45-01:20 UTC)
if [[ ! -f "$ROOT/inputs/vrp_subtal.parquet" ]]; then
    soft vrp_subtal "$SUBTAL_PY" -m crc.vrp_subtal --root "$ROOT"
fi
soft vrp_export   "$PY" -m crc.vrp_export   --root "$ROOT"
soft ripe_archive "$PY" -m crc.ripe_archive --root "$ROOT"
soft vrp_merge    "$PY" -m crc.vrp_merge    --root "$ROOT"
soft deleg        "$PY" -m crc.deleg        --root "$ROOT"
soft asnames      "$PY" -m crc.asnames      --root "$ROOT"

# 2. BGP pairs for new snapshot days
soft snapshot_pairs "$PY" -m crc.snapshot_pairs --root "$ROOT" --catch-up

# 3. per-date aggregates (hard failure)
hard compute_day "$PY" -m crc.compute_day --root "$ROOT" --auto --trailing "$TRAILING" --jobs "$JOBS"

# 4. build + atomic publish (hard failure); stdout = one JSON line
STEP=assemble
log "start assemble"
SUMMARY=$("$PY" -m crc.assemble --root "$ROOT")
log "done assemble"
NWARN=$("$PY" -c 'import json, sys; print(len(json.loads(sys.argv[1]).get("warnings") or []))' "$SUMMARY") \
    || NWARN=1

# 5. one-line summary
DONE=1
STATUS=OK
RC=0
DETAIL=''
if (( ${#FAILED[@]} )); then
    STATUS=PARTIAL; RC=1; DETAIL+="; failed: ${FAILED[*]}"
fi
if (( NWARN > 0 )); then
    STATUS=PARTIAL; RC=1; DETAIL+="; $NWARN warning(s), see 'assemble: WARNING' above"
fi
echo "$(date -u +%FT%TZ) country-rpki daily: $STATUS in $((SECONDS - START))s$DETAIL; build: $SUMMARY"
exit $RC
