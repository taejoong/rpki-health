"""RIPE NCC AS names -> inputs/asnames.parquet.

Source: https://ftp.ripe.net/ripe/asnames/asn.txt, one line per ASN: 'ASN NAME - org, CC'
(e.g. '1 LVLT-1 - Level 3 Parent, LLC, US'). name = everything between the ASN and the final
', CC'; cc = the trailing two-letter code, NULL when a line has none (e.g. AS23456).
Refreshed on every run (about 6 MB); the latest raw copy is kept as inputs/asnames_raw/asn.txt.<zst|gz>.
A download that parses to fewer than 90% of the previous row count (or < 50,000 rows) is rejected
and the previous parquet is kept (exit 1).

Output: asn UBIGINT, name VARCHAR, cc VARCHAR (sorted by asn) + inputs/asnames.json.
CLI: python -m crc.asnames [--root R]
"""
import argparse
import datetime as dt
import json
import os
import re
import sys
import time

from crc import common, fetch

URL = 'https://ftp.ripe.net/ripe/asnames/asn.txt'
LINE_RE = re.compile(r'^(\d+) (.*), ([A-Z]{2})\s*$')
BARE_RE = re.compile(r'^(\d+)\s+(.*?)\s*$')
MIN_ROWS = 50_000
MIN_SHARE_OF_PREVIOUS = 0.9


def parse(text):
    """-> (rows [(asn, name, cc|None)], skipped_lines). Later duplicates of an ASN are ignored."""
    rows, seen, skipped = [], set(), 0
    for line in text.splitlines():
        if not line.strip():
            continue
        m = LINE_RE.match(line)
        if m:
            asn, name, cc = int(m.group(1)), m.group(2).strip(), m.group(3)
        else:
            m = BARE_RE.match(line)
            if not m:
                skipped += 1
                continue
            asn, name, cc = int(m.group(1)), m.group(2), None
        if asn >= 1 << 32 or asn in seen:
            skipped += 1
            continue
        seen.add(asn)
        rows.append((asn, name, cc))
    return rows, skipped


def write_raw(root, data):
    d = os.path.join(common.inputs_dir(root), 'asnames_raw')
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f'asn.txt.{fetch.RAW_COMPRESS}')
    tmp = path + '.tmp'
    with open(tmp, 'wb') as raw:
        out = fetch._compressor(fetch.RAW_COMPRESS, raw)
        out.write(data)
        out.close()
    os.replace(tmp, path)
    return path


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--url', default=URL)
    a = ap.parse_args(argv)
    t0 = time.time()
    data = fetch.fetch_bytes(a.url)
    rows, skipped = parse(data.decode('utf-8', 'replace'))
    out = os.path.join(common.inputs_dir(a.root), 'asnames.parquet')
    meta_path = os.path.join(common.inputs_dir(a.root), 'asnames.json')
    prev = None
    try:
        with open(meta_path) as fh:
            prev = json.load(fh).get('rows')
    except (FileNotFoundError, ValueError):
        pass
    floor = max(MIN_ROWS, int(MIN_SHARE_OF_PREVIOUS * prev) if prev else 0)
    if len(rows) < floor:
        common.log(f'asnames: only {len(rows):,} rows parsed (need >= {floor:,}); keeping the previous file')
        return 1
    write_raw(a.root, data)
    con = common.connect(threads=4, memory_limit='4GB', root=a.root)
    con.execute("CREATE TABLE a AS SELECT unnest($1::UBIGINT[]) AS asn, unnest($2::VARCHAR[]) AS name, "
                "unnest($3::VARCHAR[]) AS cc", [[r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows]])
    common.copy_to_parquet(con, "SELECT asn, name, cc FROM a", out, order_by='asn')
    n_cc = sum(1 for r in rows if r[2])
    meta = {'generated_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'), 'url': a.url,
            'bytes_downloaded': len(data), 'rows': len(rows), 'rows_with_cc': n_cc, 'skipped_lines': skipped,
            'distinct_cc': len({r[2] for r in rows if r[2]}), 'seconds': round(time.time() - t0, 1)}
    common.atomic_write_json(meta_path, meta)
    common.log(f'asnames: wrote {out} rows={len(rows):,} with cc={n_cc:,} skipped={skipped} '
               f'[{time.time() - t0:.1f}s]')
    return 0


if __name__ == '__main__':
    sys.exit(main())
