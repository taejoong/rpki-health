# RPKI Health

RPKI Health is the code behind RPKI Warehouse, a longitudinal observatory for the RPKI objects
published under all five Regional Internet Registries (RIRs), from 2012 to today. It collects the
RPKI repositories with rpki-client every 20 minutes, keeps every object it has seen in per-RIR
Parquet warehouses, and serves the history through a public API and a web dashboard. History
before April 2026 comes from the RIPE NCC RPKI archive. You can look up ROAs, follow object
lifecycles, measure how much of an AS's or an organization's BGP announcements RPKI covers, and
compare ROA deployment across countries and RIRs. The NetSecure Lab at Virginia Tech runs it.

- Live site: <https://rpki.netsecurelab.org> (also <https://rpki-health.netsecurelab.org>)
- Public API: `https://rpki.netsecurelab.org/api/<endpoint>`. It returns JSON, needs no key and
  allows 60 requests per minute per client. For example,
  `https://rpki.netsecurelab.org/api/roa/export?format=csv` returns the current VRPs. The API is a
  research service without availability guarantees: do not use it, or its exports, for routing
  decisions. Data derived from ARIN's repository is also subject to ARIN's
  [Relying Party Agreement](https://www.arin.net/resources/manage/rpki/rpa.pdf).

## What is in this repository

| Directory | Component | What it does |
|---|---|---|
| [`parser/`](parser/) | rpki-parser | Runs rpki-client every 20 minutes and loads what it fetches into per-RIR Parquet warehouses: the parsed objects, a membership bitmap per snapshot, and the RP-validity verdicts. Rebuilds the presence tables (first seen, last seen) daily. Also holds research scripts. |
| [`backend/`](backend/) | rpki-backend | The read-only API over the warehouses (FastAPI, in-memory DuckDB), the daily country analytics pipeline and the weekly RPKI operations scorecard. |
| [`frontend/`](frontend/) | rpki-frontend | The web UI (React, Vite) on Cloudflare Pages, and the Pages Function that forwards `/api` to the API. |

The top-level files cover the whole system: [`ARCHITECTURE.md`](ARCHITECTURE.md),
[`LICENSE`](LICENSE), [`NOTICE`](NOTICE), [`SECURITY.md`](SECURITY.md),
[`CITATION.cff`](CITATION.cff) and `VERSIONS.json`.

This repository receives release snapshots only. We develop in private repositories. For each
release, a script exports each component with `git archive`, which leaves out internal files,
and commits the result here.

## Where to start

- [`ARCHITECTURE.md`](ARCHITECTURE.md): how the parts fit together, the job schedule, the data
  sources and the security boundaries.
- The README in each component directory: how to install, configure, run and test it.

## Versions

`VERSIONS.json` records what a release contains. The release script writes it. For each
component, it gives the source ref and the commit that the snapshot was exported from. Those
commits are in our private development repositories, so you cannot browse them here. Please quote
them in bug reports.

## How to cite

[`CITATION.cff`](CITATION.cff) holds the citation for this software. GitHub shows it under "Cite
this repository". Please cite the release you used.

## Feedback

Please use [GitHub issues](https://github.com/taejoong/rpki-health/issues). There are two
templates:

- **Operator feedback**: data that looks wrong, something you miss, or something that would help
  you run your network.
- **Bug report**: something on the site, in the API or in the code that does not work.

Issues are public, so include only public data. This repository only receives release snapshots,
so we cannot merge pull requests here. Please open an issue instead. Accepted changes appear in a
later release.

## Security

Do not report vulnerabilities in public issues. See [`SECURITY.md`](SECURITY.md).

## Acknowledgments

RPKI Health is developed by the NetSecure Lab at Virginia Tech. The RPKI warehouse and its ETL
(`parser/`) were built with support from Virginia Tech and the U.S. National Science Foundation.
The public release, the weekly refresh pipeline, the dashboard and the operator-facing work are
supported by the ARIN Community Grant Program (<https://www.arin.net/about/community_grants/>).

Any opinions, findings, and conclusions or recommendations expressed in this material are those
of the authors and do not necessarily reflect the views of the National Science Foundation or
ARIN.

## License

- Code: BSD 3-Clause License. See [`LICENSE`](LICENSE). Each component directory has its own
  `LICENSE` with the same terms.
- Trademarks: the NSF and ARIN logos in `frontend/public/` are trademarks of their owners. They
  appear only to acknowledge funding and are not covered by the BSD license. See
  [`NOTICE`](NOTICE).
- Data: we plan dated data releases on Zenodo under the Creative Commons Attribution 4.0
  International license (CC-BY-4.0). The third-party data that the code reads stays under its
  providers' terms (see [`NOTICE`](NOTICE)).
