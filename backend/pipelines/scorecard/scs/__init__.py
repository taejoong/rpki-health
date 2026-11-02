"""Weekly RPKI operations scorecard: per-RIR operational metrics of the RPKI CA and repository services,
one row per (week, window, RIR, population, operator, metric, statistic). Supported by the ARIN Community
Grant Program.

Run modules from pipelines/scorecard, e.g. `python -m scs.run --weeks 6` (see README.md). Only duckdb and
the standard library are required. The metric-module interface (what a module in scs/m_*.py receives
and must emit) is documented in scs/metrics.py; the metric definitions are registered in scs/defs.py.
"""

# Version of the shared framework (output layout, status rules, populations, ledger), written to the
# manifest and to state/state.json. A bump discards the frozen state, so the next run recomputes the
# whole history it had closed (like a def_version change of every metric). Metric definitions carry
# their own def_version (scs/defs.py). 1 (2026-10-29): first version.
VERSION = 1
