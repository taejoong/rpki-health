# ASCII-only punctuation.
"""
Classify CA deployment mode: hosted vs delegated.

What
- Classify each CA certificate as hosted or delegated using SIA publication hosts.
- Produce per-CA classification and daily counts by mode.

Why
- Hosted CAs are managed via RIR portals; delegated CAs are managed by the holder or a provider.

How
- Read table=certs; infer CA rows using basic_constraints and key usage hints.
- Extract hostnames from 'sia' string; if all hosts are RIR domains, mark hosted; else delegated.
- For daily counts, intersect CA object_ids with membership bitmaps.

Outputs
- analysis2/out/ca_classification.csv: per CA object with mode and provider
- analysis2/out/daily_ca_mode_counts.csv: date, mode, active_ca_count
"""

from __future__ import annotations
import argparse, ast, os
from pathlib import Path
import duckdb, pandas as pd
from tqdm import tqdm

from common import (
    connect_duckdb, list_latest_membership, load_bitmap,
    ensure_out_dir, extract_hosts_from_sia, registrable_domain
)

# Known RIR registrable domains
RIR_DOMAINS = {
    "AFRINIC": {"afrinic.net"},
    "APNIC":   {"apnic.net"},
    "ARIN":    {"arin.net"},
    "LACNIC":  {"lacnic.net"},
    "RIPENCC": {"ripe.net"},
}
ALL_RIR = set().union(*RIR_DOMAINS.values())


def infer_is_ca(basic_constraints: str | None,
                key_usage: str | None,
                ext_key_usage: str | None) -> bool:
    """
    Infer CA status using basic constraints or key-usage hints.
    """
    bc_text = (basic_constraints or "").strip()
    if bc_text:
        try:
            parsed = ast.literal_eval(bc_text)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            flag = parsed.get("ca")
            if isinstance(flag, bool):
                return flag
            if isinstance(flag, str):
                lowered = flag.strip().lower()
                if lowered in {"true", "1", "yes"}:
                    return True
                if lowered in {"false", "0", "no"}:
                    return False
            if isinstance(flag, (int, float)):
                return bool(flag)
        low = bc_text.lower()
        if "ca" in low and "true" in low:
            return True
        if "ca" in low and ":1" in low:
            return True

    usage = ",".join([x for x in [key_usage, ext_key_usage] if x])
    if usage and "keycertsign" in usage.lower():
        return True
    return False


def classify_mode(hosts: list[str], tal_id: str | None) -> tuple[str, str]:
    """
    Return (mode, provider). mode in {"hosted","delegated"}.
    provider is "RIR:<name>" or the registrable domain of a non-RIR host.
    """
    if not hosts:
        return ("delegated", "unknown")
    domains = {registrable_domain(h) for h in hosts}
    rir_set = RIR_DOMAINS.get((tal_id or "").upper(), ALL_RIR)
    if domains.issubset(rir_set):
        label = (tal_id or "RIR").upper()
        return ("hosted", f"RIR:{label}")
    for h in hosts:
        d = registrable_domain(h)
        if d not in rir_set:
            return ("delegated", d)
    return ("delegated", "unknown")


EXAMPLE = """Example:
  python analysis2/deployment_mode.py --warehouse warehouse-ripencc
"""


def main():
    ap = argparse.ArgumentParser(
        description="Classify CA certificates as hosted or delegated using SIA hosts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLE,
    )
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("analysis2/out"))
    ap.add_argument("--start", type=str, default=None)
    ap.add_argument("--end", type=str, default=None)
    ap.add_argument("--threads", type=int, default=max(1, (os.cpu_count() or 8) - 1))
    args = ap.parse_args()

    ensure_out_dir(args.out_dir)
    con = connect_duckdb(args.threads)

    cert_glob = str(args.warehouse / "tables" / "table=certs" / "**" / "*.parquet")
    prs_glob  = str(args.warehouse / "tables" / "table=object_presence_runs" / "**" / "*.parquet")

    certs = con.execute(f"""
        SELECT object_id::INT AS object_id,
               basic_constraints::VARCHAR AS basic_constraints,
               key_usage::VARCHAR AS key_usage,
               ext_key_usage::VARCHAR AS ext_key_usage,
               tal_id::VARCHAR AS tal_id,
               sia::VARCHAR AS sia
        FROM read_parquet('{cert_glob}')
    """).fetch_df()

    certs["is_ca_inferred"] = certs.apply(
        lambda r: infer_is_ca(r.get("basic_constraints"),
                              r.get("key_usage"),
                              r.get("ext_key_usage")),
        axis=1
    )

    cas = certs[certs["is_ca_inferred"] == True].copy()
    if cas.empty:
        raise SystemExit("No CA certificates found")

    cas["hosts"] = cas["sia"].apply(extract_hosts_from_sia)
    cas[["mode","provider"]] = cas.apply(
        lambda r: pd.Series(classify_mode(r["hosts"], r.get("tal_id"))),
        axis=1
    )

    # Add first_seen and last_seen for context
    prs = con.execute(f"""
        SELECT object_id::INT AS object_id,
               CAST(start_ts AS TIMESTAMP) s,
               CAST(end_ts   AS TIMESTAMP) e
        FROM read_parquet('{prs_glob}')
    """).fetch_df()
    if not prs.empty:
        prs["first_seen"] = pd.to_datetime(prs["s"]).dt.date
        prs["last_seen"]  = pd.to_datetime(prs["e"]).dt.date
        span = prs.groupby("object_id").agg(first_seen=("first_seen","min"),
                                            last_seen=("last_seen","max")).reset_index()
        cas = cas.merge(span, on="object_id", how="left")

    cas.to_csv(args.out_dir / "ca_classification.csv", index=False)

    # Daily counts by mode using membership
    latest = list_latest_membership(args.warehouse, args.start, args.end)
    if not latest:
        print("No membership bitmaps found; skip daily_ca_mode_counts")
        return

    ca_ids = set(int(x) for x in cas["object_id"].tolist())
    id_to_mode = cas.set_index("object_id")["mode"].to_dict()
    rows = []
    for day, roar in tqdm(latest, desc="daily CA mode"):
        bm = load_bitmap(roar)
        if bm is None:
            print("pyroaring not available; cannot load membership bitmaps")
            break
        active = [int(x) for x in bm if int(x) in ca_ids]
        if not active:
            rows.append((day, "hosted", 0))
            rows.append((day, "delegated", 0))
            continue
        df = pd.DataFrame({"object_id": active})
        df["mode"] = df["object_id"].map(lambda i: id_to_mode.get(i, "delegated"))
        rows.append((day, "hosted", int((df["mode"] == "hosted").sum())))
        rows.append((day, "delegated", int((df["mode"] == "delegated").sum())))

    if rows:
        pd.DataFrame(rows, columns=["date","mode","active_ca_count"]).to_csv(
            args.out_dir / "daily_ca_mode_counts.csv", index=False
        )


if __name__ == "__main__":
    main()
