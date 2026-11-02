# sample-crawl

Test fixtures for `tests/test_sample_crawl.py` and the quick start in the [parser README](../README.md) (`parser/README.md` in the rpki-health repository).

The five files are public RPKI objects from ARIN's RPKI repository
(`rsync://rpki.arin.net/repository/`), captured in September 2025. They are kept
unchanged, byte for byte. All five come from one CA:

| File | Object |
|---|---|
| `00621ba6-99e5-4a17-9087-51ef82e92263.crl` | the CA's CRL (thisUpdate 2025-09-26) |
| `00621ba6-99e5-4a17-9087-51ef82e92263.mft` | the CA's manifest (thisUpdate 2025-09-26) |
| `55fdfb6e-...`, `92e9ca67-...`, `cc137786-...` (`.roa`) | three ROAs |

The objects have expired: the manifest and CRL on 2025-09-29, and the ROAs' EE
certificates in November 2025. A relying party would reject them today. The ETL
does not validate signatures or dates, so the fixtures still work for parsing tests.

These objects were published by ARIN, not written by this project.
