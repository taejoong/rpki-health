# RPKI Warehouse frontend

This is the web UI of RPKI Warehouse, at <https://rpki.netsecurelab.org>.

RPKI Warehouse is a longitudinal observatory for RPKI objects across all five Regional
Internet Registries (RIRs), from 2012 to today. The UI lets you look up ROAs, follow object
lifecycles, measure RPKI signing coverage by AS and organization, and compare ROA
deployment across countries and RIRs. Our rpki-client collection runs every 20 minutes.
History before April 2026 comes from the RIPE NCC RPKI archive.

The UI is a React single-page app, built with Vite, Tailwind CSS and Recharts. It is hosted
on Cloudflare Pages. A Pages Function passes its API calls to the backend.

This code is published as the `frontend/` directory of the `rpki-health` repository,
<https://github.com/taejoong/rpki-health>. That repository also holds the collection and ETL
code (`parser/`) and the API (`backend/`). Its `ARCHITECTURE.md` shows how the parts fit
together.

## Pages

| Path | Page |
|---|---|
| `/` | Dashboard: warehouse overview and health, live VRP downloads (CSV, JSON) |
| `/analysis/countries` | ROA deployment per RIR and per registered country |
| `/roa/lookup` | Live ROAs for a prefix, with lifecycle history |
| `/roa/by-asn` | Current ROA prefixes of an ASN |
| `/roa/org` | Organization lookup by name or ASN |
| `/roa/coverage` | Signing coverage of an ASN: ROA-covered vs. BGP-announced prefixes |
| `/roa/org-coverage` | Signing coverage across the ASNs of an organization |
| `/aspa` | ASPA deployment and adoption trends |
| `/ca` | CA hierarchy explorer |
| `/validity` | Objects that rpki-client rejected in the latest cycle, with the reason |
| `/alerts` | New and withdrawn ROAs within a time window |
| `/admin` | Visitor statistics; needs the admin token |

## Layout

| Path | Contents |
|---|---|
| `src/pages/`, `src/components/` | Pages and shared components |
| `src/api.js` | API client |
| `functions/api/[[path]].js` | Pages Function: the proxy to the backend |
| `public/` | Static files copied into the build: icon, logos, `_headers`, `_redirects` |
| `vite.config.js` | Vite configuration, with the development proxy |

## How the UI reaches the API

The app requests paths under `/api/` on its own origin, such as `/api/roa/lookup`. On
Cloudflare Pages, the Function in `functions/api/[[path]].js` answers them:

- It removes `/api` and sends the rest of the path, with the query string, to the backend at
  `BACKEND_URL`. For example, `/api/roa/lookup?prefix=...` goes to
  `<BACKEND_URL>/roa/lookup?prefix=...`.
- It sends only these headers to the backend:
  - `X-Visitor-IP`: the visitor's address, from `CF-Connecting-IP`.
  - `X-Visitor-Country`: the visitor's country, from Cloudflare.
  - `User-Agent`, `Referer`, and `Accept: application/json`.
  - `X-Admin-Token`, if the request has one. The admin page sends it.
  - `X-Proxy-Token`, if the Pages secret `PROXY_TOKEN` is set. Its value is that secret.
- The backend rate-limits each visitor address and keeps usage statistics. Any client can send
  `X-Visitor-IP`, so the backend trusts that header only when `X-Proxy-Token` carries the right
  token.
- It keeps the request method but does not forward a request body; the UI sends only GET
  requests.
- It streams the response back with the backend's status, `Content-Type` and
  `Content-Disposition`, and adds `Cache-Control: no-store`.
- If the backend cannot be reached, it logs the error and returns status 502 with
  `{"error": "Backend unavailable"}`.

The same API is public at `https://rpki.netsecurelab.org/api/<endpoint>`.

## Local development

You need Node.js 20 or later. We build with Node.js 22. The development server sends API
calls to a backend at `http://localhost:8000`, the backend's default address (see
`backend/`).

```sh
npm ci
npm run dev
```

Vite prints the local URL. The Pages Function does not run in development. Instead, the
Vite proxy in `vite.config.js` sends `/api/*` to `http://localhost:8000` and removes `/api`,
as the Function does. To use another backend, change `target` there.

`src/api.js` reads one optional build-time variable, `VITE_API_URL`, and puts it in front of
its API paths. Production leaves it unset. Only the path part of the result is used, so the
variable cannot send requests to another host.

To try the Function itself, serve a build with Wrangler. Wrangler reads the Function's
variables from `.dev.vars`, which git ignores. Create it with this line, and add a
`PROXY_TOKEN=...` line if your backend has a proxy token:

```
BACKEND_URL=http://localhost:8000
```

Then run:

```sh
npm run build
npx wrangler pages dev dist
```

## Build

```sh
npm run build
```

The output goes to `dist/`. `npm run preview` serves it with the same `/api` proxy as
`npm run dev`.

The build copies two files from `public/` that Cloudflare Pages reads. `_redirects` serves
`index.html` for every path, so the app's routes work when a page is reloaded. `_headers`
sets `X-Frame-Options: DENY` and `X-Content-Type-Options: nosniff`.

## Deploy

The site is a Cloudflare Pages project, and its production branch is `main`. Build from a
clean checkout of the commit you want to ship, then upload `dist/` with Wrangler:

```sh
npm ci
npm run build
npx wrangler pages deploy dist --project-name <project> --branch main
```

`--branch main` makes the upload a production deployment, whatever is checked out. Without
it, as in `npm run deploy`, Wrangler takes the branch from git, and an upload from any branch
other than `main` becomes a preview deployment.

The Function reads two Pages secrets. Set them with Wrangler or in the Cloudflare dashboard.
Never commit them.

| Secret | Value |
|---|---|
| `BACKEND_URL` | The backend's base URL, with no trailing slash |
| `PROXY_TOKEN` | The token for `X-Proxy-Token`: the same value the backend reads from its `PROXY_TOKEN_FILE` (`app/config.py` in `backend/`) |

```sh
npx wrangler pages secret put BACKEND_URL --project-name <project>
npx wrangler pages secret put PROXY_TOKEN --project-name <project>
```

Without `PROXY_TOKEN`, the backend ignores `X-Visitor-IP`, and all visitors of the site
share one rate limit. A secret applies only to deployments made after it is set, so deploy
again after you set or change one.

## License

BSD 3-Clause License. See `LICENSE`.

## Trademarks

The NSF and ARIN logos in `public/` are trademarks of their owners. They are shown only to
acknowledge funding, and the BSD license does not cover them. See `NOTICE`.

## Security

Please report vulnerabilities privately, with the "Report a vulnerability" button on the
Security tab of <https://github.com/taejoong/rpki-health>. Do not open a public issue.

## Acknowledgments

RPKI Warehouse is developed by the NetSecure Lab at Virginia Tech. The longitudinal RPKI
warehouse and its ETL (rpki-parser) were built with support from Virginia Tech and from the
U.S. National Science Foundation. The public release, the weekly refresh pipeline, the
operational dashboard and the operator-facing views are supported by the ARIN Community
Grant Program (<https://www.arin.net/about/community_grants/>).

Any opinions, findings, and conclusions or recommendations expressed in this material are
those of the author(s) and do not necessarily reflect the views of the National Science
Foundation or ARIN.
