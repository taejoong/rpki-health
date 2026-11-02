from pathlib import Path

# This checkout. The defaults below expect the parser next to it, with its
# warehouse-{rir}/ directories inside: an rpki-parser checkout, or parser/ in
# the rpki-health repository (the first of the two that exists).
BACKEND_DIR = Path(__file__).resolve().parent.parent

_PARSER_DIRS = (BACKEND_DIR.parent / "rpki-parser", BACKEND_DIR.parent / "parser")
WAREHOUSE_BASE = next((p for p in _PARSER_DIRS if p.is_dir()), _PARSER_DIRS[0])

RIRS = ["afrinic", "apnic", "arin", "lacnic", "ripencc"]

WAREHOUSE_DIRS = {rir: WAREHOUSE_BASE / f"warehouse-{rir}" for rir in RIRS}


# rpki-parser's daily build_presence_runs.py rewrites object_presence_stats in
# place while holding this lock; init/reload hold it shared so they never read
# half-written parts. The weekly off-host backup takes it as well.
PRESENCE_LOCK_FILE = Path("/tmp/rpki_presence.lock")


# BGP Warehouse API for prefix announcement data
BGP_WAREHOUSE_URL = "https://rib.netsecurelab.org"
BGP_WAREHOUSE_KEY_FILE = Path.home() / ".config" / "bgp-warehouse.key"
# Coverage counts a prefix as announced when any of these collectors saw the
# ASN originate it during the last BGP_COVERAGE_WINDOW_DAYS days of RIBs the
# BGP warehouse holds. route-views2 has no current IPv6 feed (its IPv6 data
# stops in 2021), hence route-views6; RIS rrc00 adds a second, multihop view.
BGP_COVERAGE_COLLECTORS = ("route-views2", "route-views6", "rrc00")
BGP_COVERAGE_WINDOW_DAYS = 7

# ASINT API for org-to-ASN mapping
ASINT_API_URL = "https://api.asint.netsecurelab.org"

# Country analytics (/analytics/country/*): pipelines/country writes a daily
# build to builds/<stamp>/ here and points `current` at it; the API reads
# `current` lazily (no warehouse load, no reload needed).
COUNTRY_RPKI_DIR = Path("/ssd-data/country-rpki")

# ── Visitor analytics / admin ──
# SQLite store for visitor tracking (created on startup if missing)
ANALYTICS_DB = BACKEND_DIR / "data" / "analytics.db"
# View token for GET /stats (X-Stats-Token, app/routers/stats.py), one line.
STATS_TOKEN_FILE = Path.home() / ".config" / "rpki-stats.token"

# Admin token file (X-Admin-Token for /admin/stats), one line. Create it with:
#   umask 077; openssl rand -hex 32 > ~/.config/rpki-admin.token
ADMIN_TOKEN_FILE = Path.home() / ".config" / "rpki-admin.token"

# Machine token for POST /admin/reload (X-Reload-Token), used by the reload
# cron job (every 30 minutes). Kept apart from the admin token so the
# cron holds only reload rights. Create it with:
#   umask 077; openssl rand -hex 32 > ~/.config/rpki-reload.token
RELOAD_TOKEN_FILE = Path.home() / ".config" / "rpki-reload.token"

# Shared secret the Cloudflare Pages Function (frontend functions/api/) sends as
# X-Proxy-Token. X-Visitor-IP is trusted only alongside it (app/client_ip.py),
# since any client can send that header. If the
# file is missing or empty, all requests through the Function share one
# rate-limit key (its egress IP). Create it with:
#   umask 077; openssl rand -hex 32 > ~/.config/rpki-proxy.token
# and set the same value as the Pages secret PROXY_TOKEN (README: rotation).
PROXY_TOKEN_FILE = Path.home() / ".config" / "rpki-proxy.token"
