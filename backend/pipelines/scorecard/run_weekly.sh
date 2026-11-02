#!/usr/bin/env bash
# Weekly RPKI operations scorecard: the weekly cron entry point (see README.md).
#
#   usage: run_weekly.sh [--root DIR] [--weeks N] [--backfill FROM TO] [--metrics LIST]
#
#   0. remove leftovers of killed runs (this pipeline's own temp names only)
#   1. observation ledger  scs.ledger (a failure ends the run: scs.run refuses a ledger that ends before
#                          as_of, whose later days would count as gaps and freeze the closed weeks as masked)
#   2. compute + build     scs.run (at most 3 hours): as_of from the presence tables, the trailing N weeks
#                          (default 6) and the open weeks of every metric (plus FROM..TO with --backfill),
#                          statuses, merge with state/, publish builds/<stamp> and `current`, the new state/,
#                          and (not with --backfill) releases/<YYYY-Www>/
#   3. one summary line on stdout "<time> scorecard weekly: <STATUS> in <s>s; ...":
#        OK       exit 0  published; every step succeeded and scs.run reported no warning
#        PARTIAL  exit 1  published, but scs.run reported warnings (a RIR's presence build is late, as_of
#                         before this week's Monday, state or release not written, ...)
#        FAILED   exit 2  scs.ledger or scs.run failed (or ran out of time), or the script stopped; the line
#                         names the step and says whether `current` is unchanged
#      Another run holding ROOT/.lock: one line on stderr, exit 75, nothing done. Bad option: exit 64.
#
# Environment (the options win): SCORECARD_ROOT (default /ssd-data/scorecard), SCORECARD_WAREHOUSE_BASE
# (default /ssd-data), PY (pipeline python), WEEKS (trailing weeks, 6: ROA-EE revocation with the 35-day
# horizon is final at W+6), METRICS (comma list of metric_ids; default every metric); the modules also read
# SCORECARD_MAX_SPILL (DuckDB spill per connection, default 300GB).
set -euo pipefail

ROOT=${SCORECARD_ROOT:-/ssd-data/scorecard}
PY=${PY:-$(dirname "$(readlink -f "$0")")/../../.venv/bin/python}   # rpki-backend/.venv
WEEKS=${WEEKS:-6}
METRICS=${METRICS:-}
BACKFILL=()
usage() { echo "usage: $0 [--root DIR] [--weeks N] [--backfill FROM TO] [--metrics LIST]" >&2; exit 64; }
while (( $# )); do
    case $1 in
        --root) (( $# >= 2 )) || usage; ROOT=$2; shift 2 ;;
        --root=*) ROOT=${1#*=}; shift ;;
        --weeks) (( $# >= 2 )) || usage; WEEKS=$2; shift 2 ;;
        --backfill) (( $# >= 3 )) || usage; BACKFILL=("$2" "$3"); shift 3 ;;
        --metrics) (( $# >= 2 )) || usage; METRICS=$2; shift 2 ;;
        -h|--help) usage ;;
        *) echo "run_weekly: unknown argument: $1" >&2; usage ;;
    esac
done
[[ $WEEKS =~ ^[0-9]+$ ]] || usage
for d in "${BACKFILL[@]}"; do
    [[ $d =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || usage
done
[[ -z $METRICS || $METRICS =~ ^[a-z0-9_,]+$ ]] || usage
# absolute paths before the cd below (the modules run from pipelines/scorecard): a relative root would
# otherwise land inside the code checkout. -s keeps symlinks: a venv's bin/python is a symlink, and the
# resolved interpreter would not see the venv's packages.
ROOT=$(realpath -ms -- "$ROOT")
if [[ $PY == */* ]]; then PY=$(realpath -ms -- "$PY"); fi
export SCORECARD_ROOT=$ROOT
cd "$(dirname "$(readlink -f "$0")")"

log() { echo "$(date -u +%FT%TZ) run_weekly: $*" >&2; }
START=$SECONDS
STEP=setup
DONE=0
CUR0=-                         # `current` before scs.run ('-': scs.run has not started)
finish() {   # EXIT trap: a run that stops anywhere still ends with one summary line (a busy lock: exit 75)
    local rc=$? cur what
    (( DONE || rc == 75 )) && return
    cur=$(readlink "$ROOT/current" || true)
    if [[ $CUR0 == - || $cur == "$CUR0" ]]; then
        what='current build unchanged'
    else
        what="current CHANGED to $cur; state or release may be missing"
    fi
    echo "$(date -u +%FT%TZ) scorecard weekly: FAILED in $((SECONDS - START))s at $STEP (exit $rc); $what"
    exit 2
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

mkdir -p "$ROOT"
exec 9>"$ROOT/.lock"
if ! flock -n 9; then
    echo "$(date -u +%FT%TZ) scorecard weekly: another run holds $ROOT/.lock; skipped" >&2
    exit 75
fi
renice -n 10 -p $$ >/dev/null 2>&1 || true   # children inherit nice 10

hard() {   # hard <name> <command...>: a failure ends the run (FAILED, exit 2)
    STEP=$1; shift
    log "start $STEP"
    "$@" >&2                   # step output goes to the log; stdout carries only the summary line
    log "done $STEP"
}

command -v "$PY" >/dev/null || { log "ERROR: PY=$PY is not an executable python"; exit 2; }
mkdir -p "$ROOT"/{builds,states,releases,ledger,logs,tmp}

# 0. leftovers of runs that were killed: DuckDB spill and run work directories of processes that are gone,
#    release temp directories and ledger CSVs older than 6 h (builds/ and states/ *.tmp: scs.assemble.prune)
STEP=cleanup
for d in "$ROOT"/tmp/duckdb-* "$ROOT"/tmp/run-*; do
    [[ -d $d ]] || continue
    pid=${d##*/}; pid=${pid#duckdb-}; pid=${pid#run-}; pid=${pid%%-*}
    [[ $pid =~ ^[0-9]+$ && -e /proc/$pid ]] && continue
    rm -rf -- "$d"
done
find "$ROOT/releases" -mindepth 1 -maxdepth 1 -name '.tmp-*' -mmin +360 -prune -exec rm -rf -- {} + 2>/dev/null || true
find "$ROOT/tmp" -maxdepth 1 -name '.ledger-*.csv' -mmin +360 -delete 2>/dev/null || true

# 1. observation ledger (reads snapshot metadata and object_validity, no presence table)
hard ledger "$PY" -m scs.ledger --root "$ROOT"

# 2. metrics, build and publish (hard failure, at most 3 hours: a hang would hold ROOT/.lock); stdout = one
#    JSON line. --foreground keeps scs.run in this process group, so Ctrl-C still stops a run started by hand
ARGS=(--root "$ROOT" --weeks "$WEEKS")
if (( ${#BACKFILL[@]} )); then ARGS+=(--backfill "${BACKFILL[@]}"); fi
if [[ -n $METRICS ]]; then ARGS+=(--metrics "$METRICS"); fi
STEP=run
CUR0=$(readlink "$ROOT/current" || true)
log "start run ${ARGS[*]}"
run_rc=0
SUMMARY=$(timeout --foreground --signal=TERM 3h "$PY" -m scs.run "${ARGS[@]}") || run_rc=$?
if (( run_rc == 124 )); then STEP='run (timed out after 3h)'; fi
if (( run_rc )); then exit "$run_rc"; fi
log "done run"
NWARN=$("$PY" -c 'import json, sys; print(len(json.loads(sys.argv[1]).get("warnings") or []))' "$SUMMARY") \
    || NWARN=1

# 3. one-line summary
DONE=1
STATUS=OK
RC=0
DETAIL=''
if (( NWARN > 0 )); then
    STATUS=PARTIAL; RC=1; DETAIL+="; $NWARN warning(s), see 'assemble: WARNING' above"
fi
echo "$(date -u +%FT%TZ) scorecard weekly: $STATUS in $((SECONDS - START))s$DETAIL; build: $SUMMARY"
exit $RC
