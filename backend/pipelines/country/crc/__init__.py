"""Country ROA deployment pipeline (rpki.netsecurelab.org /analysis/countries).

Run modules from pipelines/country, e.g. `python -m crc.compute_day --date 2026-10-26`.
Only duckdb and the standard library are required, so the same code runs on the API host and the BGP host.
"""

# Version of the per-date computation, written to done.json and the manifest. A bump makes compute_day
# --auto/--only-missing recompute every date. 2 (2026-10-28): prefix-length floor (IPv4 /8, IPv6 /16)
# and per-repository VRP bridging. 3 (2026-10-28): per-repository bridging only for publication points
# of >= 150 VRPs (crc.intervals.HOST_MIN_REF) and not across ROA validity lapses (validity guard).
VERSION = 3
