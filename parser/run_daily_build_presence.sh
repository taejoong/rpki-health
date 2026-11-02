#!/bin/bash
# Daily 01:00 cron job: rebuild object_presence_runs / object_presence_stats from the membership bitmaps.
# rpki-backend's last_seen (= object_presence_stats.last_ts) depends on this output.
set -u

# Take the same lock as the weekly off-host backup (operations repository), so Parquet files are not rewritten while it takes its snapshot.
exec 8>/tmp/rpki_presence.lock
flock 8

# 1. Change to the working directory (exit at once on failure)
WH_ROOT="$(dirname "$(readlink -f "$0")")"   # this checkout, which holds warehouse-<tal>/
cd "${WH_ROOT}/analysis" || exit 1

# cron's PATH has no `python` (Ubuntu 26.04 has only python3, and the dependencies are installed only in the venv).
# From 2026-10-12 the job failed silently every day with `python: command not found`, so use the venv's absolute path.
PYTHON="${PYTHON:-${WH_ROOT}/.venv/bin/python}"

# 2. Run the TALs one after another. If any fails, exit non-zero → the cron heartbeat is not sent, and the monitor flags it.
FAILED=()
for tal in afrinic arin apnic ripencc lacnic; do
  echo "$(date -u +'%F %T') build_presence_runs: ${tal}"
  if ! "$PYTHON" build_presence_runs.py --warehouse "${WH_ROOT}/warehouse-${tal}" --batch-rows 200000; then
    FAILED+=("$tal")
  fi
done

if [ ${#FAILED[@]} -gt 0 ]; then
  echo "$(date -u +'%F %T') FAILED: ${FAILED[*]}"
  exit 1
fi
echo "$(date -u +'%F %T') done"
