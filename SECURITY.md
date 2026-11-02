# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub. On this repository's
**Security** tab, choose **Report a vulnerability**. Only you and the maintainers can see the
report.

- Do not open a public issue, pull request or discussion about a vulnerability.
- Do not run load tests, brute-force tests or automated scanners against rpki.netsecurelab.org or
  rpki-health.netsecurelab.org. A small lab runs the service, and heavy traffic slows it down for
  everyone. A few requests that show the problem are enough.
- Tell us what you found, where (a URL, or a file and line), how to reproduce it, and which
  release you looked at (see `VERSIONS.json`).

We aim to reply within five business days. This is best effort: a small research lab runs the
service. Fixes go into the next release.

## Scope

In scope:

- the code in this repository: `parser/`, `backend/`, `frontend/` and the top-level files;
- the deployment at <https://rpki.netsecurelab.org> (also <https://rpki-health.netsecurelab.org>),
  including the API under `/api/`.

Out of scope:

- problems in RPKI data published by an RIR or a CA. Report those to the publisher. If our site
  shows the data wrongly, open an "Operator feedback" issue;
- third-party services, such as Cloudflare, GitHub, the RIR repositories, RouteViews and RIPE RIS;
- denial of service by traffic volume, and scanner output without a demonstrated impact.

The service serves public RPKI data. There is no bug bounty.
