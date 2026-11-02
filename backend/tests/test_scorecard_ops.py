"""run_weekly.sh of the scorecard pipeline with a stub interpreter (as tests/test_country_pipeline_ops.py
does for run_daily.sh): exit codes 0 OK / 1 PARTIAL / 2 FAILED / 75 lock busy / 64 bad option, the one
summary line (also for a timeout, a `current` that scs.run changed before it failed, and failures before
the lock is taken), the arguments passed to the modules (--weeks, --backfill, --metrics), a relative root,
the venv interpreter symlink, and the removal of leftovers of killed runs.
"""
import fcntl
import os
import subprocess
import sys
import time

import pytest

PIPE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pipelines', 'scorecard')
RUN_WEEKLY = os.path.join(PIPE, 'run_weekly.sh')

STUB = """#!/usr/bin/env bash
# stub interpreter: `stub -m scs.<module> ...` (STUB_FAIL: modules that exit STUB_RC, default 3; STUB_WARN:
# run warnings; STUB_SWAP: scs.run points `current` at builds/<STUB_SWAP> first, as after its publish)
if [[ $1 == -c ]]; then exec "$REAL_PY" "$@"; fi
mod=${2#scs.}
echo "$mod exe=$0 cwd=$PWD ${*:3}" >> "$STUB_LOG"
if [[ $mod == run && -n ${STUB_SWAP:-} ]]; then ln -sfn "builds/$STUB_SWAP" "$SCORECARD_ROOT/current"; fi
case ",${STUB_FAIL:-}," in *",$mod,"*) echo "stub $mod failed" >&2; exit "${STUB_RC:-3}" ;; esac
if [[ $mod == ledger ]]; then
  mkdir -p "$SCORECARD_ROOT/ledger" && touch "$SCORECARD_ROOT/ledger/ledger.parquet"
  echo "stdout of ledger"
fi
if [[ $mod == run ]]; then
  echo '{"as_of": "2026-11-02T00:40:01Z", "build": "20261102T050000Z", "mode": "weekly", "warnings": ['"${STUB_WARN:-}"']}'
fi
"""


@pytest.fixture
def stub(tmp_path):
    p = tmp_path / 'stubpy'
    p.write_text(STUB)
    p.chmod(0o755)
    return str(p)


def run_weekly(stub, tmp_path, *args, cwd=None, **env):
    e = {'PATH': os.environ['PATH'], 'HOME': str(tmp_path), 'PY': stub, 'REAL_PY': sys.executable,
         'STUB_LOG': str(tmp_path / 'stub.log'), **env}
    p = subprocess.run(['bash', RUN_WEEKLY, *args], env=e, cwd=cwd or str(tmp_path), capture_output=True,
                       text=True, timeout=60)
    return p.returncode, p.stdout.strip(), p.stderr


def calls(tmp_path):
    log = tmp_path / 'stub.log'
    out = log.read_text().splitlines() if log.exists() else []
    if log.exists():
        log.unlink()
    return out


def test_run_weekly_outcomes(stub, tmp_path):
    """OK 0, PARTIAL 1 (warnings), FAILED 2 with one summary line naming the step and whether `current`
    changed (the ledger failed, also with an older ledger: scs.run would refuse it; scs.run failed or timed
    out), busy 75, bad option 64."""
    root = str(tmp_path / 'root')
    rc, out, err = run_weekly(stub, tmp_path, '--root', 'root', STUB_FAIL='ledger')
    assert rc == 2 and ' scorecard weekly: FAILED in ' in out and 'at ledger (exit 3); current build unchanged' in out
    assert [c.split()[0] for c in calls(tmp_path)] == ['ledger']
    rc, out, err = run_weekly(stub, tmp_path, '--root', root)
    assert rc == 0 and ' scorecard weekly: OK in ' in out and '"build": "20261102T050000Z"' in out
    assert '\n' not in out and 'stdout of ledger' in err            # stdout carries only the summary line
    assert 'run_weekly: done ledger' in err and 'run_weekly: done run' in err
    assert [c.split()[0] for c in calls(tmp_path)] == ['ledger', 'run']
    rc, out, _ = run_weekly(stub, tmp_path, '--root', root, STUB_FAIL='ledger')    # an older ledger exists
    assert rc == 2 and 'at ledger (exit 3); current build unchanged' in out
    assert [c.split()[0] for c in calls(tmp_path)] == ['ledger']                     # scs.run not started
    rc, out, _ = run_weekly(stub, tmp_path, '--root', root, STUB_WARN='"warehouse-arin presence ends late"')
    assert rc == 1 and ' PARTIAL in ' in out and '1 warning(s)' in out
    rc, out, _ = run_weekly(stub, tmp_path, '--root', root, STUB_FAIL='run')
    assert rc == 2 and 'at run (exit 3); current build unchanged' in out
    rc, out, _ = run_weekly(stub, tmp_path, '--root', root, STUB_FAIL='run', STUB_RC='124')   # timeout's exit code
    assert rc == 2 and 'at run (timed out after 3h) (exit 124); current build unchanged' in out
    rc, out, _ = run_weekly(stub, tmp_path, '--root', root, STUB_FAIL='run', STUB_SWAP='20261109T050000Z')
    assert rc == 2 and 'at run (exit 3); current CHANGED to builds/20261109T050000Z; state or release may be ' \
        'missing' in out
    with open(os.path.join(root, '.lock')) as f:                     # a second run while one holds the lock
        fcntl.flock(f, fcntl.LOCK_EX)
        rc, out, err = run_weekly(stub, tmp_path, '--root', root)
    assert rc == 75 and out == '' and 'another run holds' in err
    for bad in (['--weeks', 'x'], ['--weeks'], ['--backfill', '2012-01-02'], ['--backfill', '2012-1-2', '2026-10-26'],
                ['--metrics', 'crl_window;rm'], ['--frobnicate']):
        rc, out, err = run_weekly(stub, tmp_path, '--root', root, *bad)
        assert rc == 64 and 'usage:' in err and out == '', bad


def test_run_weekly_setup_failures_end_with_the_summary_line(stub, tmp_path):
    """A root that cannot be created and a .lock that cannot be opened stop the run before the lock is
    taken: the FAILED line and exit 2 as for every other stop (exit 1 would read as PARTIAL)."""
    if os.geteuid() == 0:
        pytest.skip('root ignores file permissions')
    ro = tmp_path / 'ro'
    (ro / 'root').mkdir(parents=True)
    (ro / 'root' / '.lock').write_text('')
    os.chmod(ro / 'root' / '.lock', 0o444)
    os.chmod(ro / 'root', 0o555)
    os.chmod(ro, 0o555)
    try:
        rc, out, err = run_weekly(stub, tmp_path, '--root', str(ro / 'new'))
        assert rc == 2 and ' scorecard weekly: FAILED in ' in out and 'at setup (exit 1); current build unchanged' in out
        rc, out, err = run_weekly(stub, tmp_path, '--root', str(ro / 'root'))
        assert rc == 2 and 'at setup (exit 1)' in out and '.lock: Permission denied' in err
    finally:
        os.chmod(ro, 0o755)
        os.chmod(ro / 'root', 0o755)
    assert calls(tmp_path) == []


def test_run_weekly_passes_weeks_backfill_and_metrics(stub, tmp_path):
    """--weeks defaults to 6 (WEEKS overrides it, the option wins), --backfill FROM TO and --metrics go
    to scs.run, the ledger always runs first with the same root."""
    root = str(tmp_path / 'root')

    def run_args(*args, **env):
        rc, out, _ = run_weekly(stub, tmp_path, '--root', root, *args, **env)
        assert rc == 0 and ' OK in ' in out
        c = calls(tmp_path)
        assert c[0].startswith('ledger ') and c[0].endswith(f' --root {root}')
        return c[1][c[1].index(' --root '):]
    assert run_args() == f' --root {root} --weeks 6'
    assert run_args(WEEKS='3') == f' --root {root} --weeks 3'
    assert run_args('--weeks', '8', WEEKS='3') == f' --root {root} --weeks 8'
    assert run_args('--backfill', '2011-12-26', '2026-10-26') == \
        f' --root {root} --weeks 6 --backfill 2011-12-26 2026-10-26'
    assert run_args('--metrics', 'stub_crl_count') == f' --root {root} --weeks 6 --metrics stub_crl_count'
    assert run_args(METRICS='crl_window,mft_window') == f' --root {root} --weeks 6 --metrics crl_window,mft_window'


def test_run_weekly_relative_root_and_venv_symlink(stub, tmp_path):
    """A relative root resolves against the caller's directory before the cd into the pipeline directory
    (SCORECARD_ROOT is exported to the modules), and PY keeps the venv's bin/python symlink."""
    work = tmp_path / 'work'
    (work / 'venv' / 'bin').mkdir(parents=True)
    os.symlink(stub, work / 'venv' / 'bin' / 'python')
    rc, out, _ = run_weekly(stub, tmp_path, '--root=rel', cwd=str(work), PY='venv/bin/python',
                            SCORECARD_ROOT='/nonexistent/unused')
    assert rc == 0 and ' OK in ' in out
    assert os.path.isdir(work / 'rel' / 'builds') and os.path.exists(work / 'rel' / '.lock')
    assert os.path.exists(work / 'rel' / 'ledger' / 'ledger.parquet')
    assert not os.path.exists(os.path.join(PIPE, 'rel'))
    lines = calls(tmp_path)
    assert len(lines) == 2 and all(f' exe={work}/venv/bin/python ' in ln for ln in lines), lines
    assert all(f' cwd={os.path.realpath(PIPE)} ' in ln for ln in lines) and '/nonexistent' not in ''.join(lines)
    rc, out, err = run_weekly(stub, tmp_path, '--root', 'r', cwd=str(work), PY='/nonexistent/python')
    assert rc == 2 and 'at setup' in out and 'is not an executable python' in err


def test_run_weekly_removes_only_stale_leftovers(stub, tmp_path):
    """DuckDB spill and run work directories of processes that are gone are removed, so are release temp
    directories and ledger CSVs older than 6 h; a live process's directories and anything else stay."""
    root = tmp_path / 'root'
    stale = [root / 'tmp/duckdb-999999999-0', root / 'tmp/run-999999999-1793260000', root / 'releases/.tmp-2026-W44-1',
             root / 'tmp/.ledger-999999999.csv']
    keep = [root / f'tmp/duckdb-{os.getpid()}-0', root / f'tmp/run-{os.getpid()}-1', root / 'releases/2026-W43',
            root / 'tmp/other-tool', root / 'releases/.tmp-2026-W45-2', root / 'tmp/.ledger-1.csv']
    for p in stale + keep:
        if p.suffix == '.csv':
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('x')
        else:
            p.mkdir(parents=True)
    old = time.time() - 7 * 3600
    for p in stale + [root / 'releases/2026-W43', root / 'tmp/other-tool']:
        os.utime(p, (old, old))
    rc, _, _ = run_weekly(stub, tmp_path, '--root', str(root))
    assert rc == 0
    assert [str(p) for p in stale if os.path.exists(p)] == []
    assert [str(p) for p in keep if not os.path.exists(p)] == []
