# analysis2

Repo-only analyses for:
- Hosted vs delegated CA classification
- VRP stability versus crypto churn
- Who controls ROAs (publication-domain heuristic)

## Install

```bash
pip install duckdb pandas pyarrow tqdm pyroaring
```

Run examples

# 1) CA deployment mode
python analysis2/deployment_mode.py --warehouse warehouse-ripencc

# 2) VRP stability versus crypto churn (plain)
python analysis2/vrp_stability_vs_crypto_churn.py --warehouse warehouse-apnic

# 2b) Split by hosted vs delegated
python analysis2/vrp_stability_vs_crypto_churn.py --warehouse warehouse-apnic --by-mode

# 3) Who controls ROAs
python analysis2/who_controls_roa.py --warehouse warehouse-arin

Outputs land in analysis2/out/.

Notes
- Scripts prefer membership bitmaps at indexes/membership. If pyroaring is not installed, the scripts will exit with a clear message.
- Domain classification is an operational heuristic, not legal identity. See RFC 9255 for identity caveats.

---
