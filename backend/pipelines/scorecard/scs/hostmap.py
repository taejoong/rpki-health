"""Publication-host map: exact host name -> {rir, class, operator}, versioned.

scs/refdata/hostmap.json holds the map (with the evidence for every host). Every build records
`hostmap_version` (the integer in the file), the file's sha256 and `digest`, the sha256 of the
classification itself (host, rir, class, operator); a change of the version or of the digest makes the
next run recompute the history of every metric it computes (populations would otherwise change
mid-series), while edits of the evidence and notes do not.

Classes: rir_hosted, rir_paas, nir_hosted, delegated, unknown. A listed host's class applies only in
the warehouse of its own RIR: rpki-rps.arin.net CAs under the RIPE NCC trust anchor are 'delegated' in
warehouse-ripencc (they are not operated by RIPE NCC). Unlisted hosts are 'delegated', objects without a
resolvable host 'unknown'. The operator label groups several hosts of one operator (IDNIC has used four,
CNNIC and TWNIC two); per-operator rows are reported for nir_hosted and rir_paas only.
"""
import hashlib
import json
import os

from . import common

HOSTMAP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'refdata', 'hostmap.json')
CLASSES = ('rir_hosted', 'rir_paas', 'nir_hosted', 'delegated', 'unknown')
LISTED_CLASSES = ('rir_hosted', 'rir_paas', 'nir_hosted', 'delegated')
OPERATOR_CLASSES = ('nir_hosted', 'rir_paas')


class HostMap:
    """Loaded host map: version, sha256 (of the file), digest (of the classification), path,
    hosts {host: (rir, class, operator)}."""

    def __init__(self, version, sha256, hosts, path=None):
        self.version, self.sha256, self.hosts, self.path = version, sha256, hosts, path
        self.digest = hashlib.sha256(json.dumps(self.rows()).encode()).hexdigest()

    def classify(self, host, rir):
        """(class, operator) of a publication host in the warehouse of `rir` (pure Python; the SQL
        equivalent is classify.classify_sql)."""
        if not host:
            return 'unknown', ''
        e = self.hosts.get(host.lower())
        if e is None or e[0] != rir:
            return 'delegated', ''
        return e[1], (e[2] if e[1] in OPERATOR_CLASSES else '')

    def rows(self):
        """[(host, rir, class, operator)] sorted by host, for a SQL table."""
        return [(h,) + self.hosts[h] for h in sorted(self.hosts)]

    def info(self):
        return {'version': self.version, 'sha256': self.sha256, 'digest': self.digest, 'hosts': len(self.hosts),
                'by_class': {c: sum(1 for e in self.hosts.values() if e[1] == c) for c in LISTED_CLASSES}}


def load(path=HOSTMAP_PATH):
    """Read and check the map: lower-case host names, known RIRs and classes, an integer version."""
    with open(path, 'rb') as f:
        raw = f.read()
    doc = json.loads(raw)
    version = doc.get('hostmap_version')
    if not isinstance(version, int) or version < 1:
        raise ValueError(f'{path}: hostmap_version must be a positive integer, got {version!r}')
    hosts = {}
    for host, e in (doc.get('hosts') or {}).items():
        if host != host.strip().lower() or '/' in host or ':' in host:
            raise ValueError(f'{path}: host {host!r} must be a lower-case host name')
        if e.get('rir') not in common.REGISTRIES:
            raise ValueError(f'{path}: {host}: unknown rir {e.get("rir")!r}')
        if e.get('class') not in LISTED_CLASSES:
            raise ValueError(f'{path}: {host}: unknown class {e.get("class")!r}')
        if not e.get('operator'):
            raise ValueError(f'{path}: {host}: operator missing')
        hosts[host] = (e['rir'], e['class'], e['operator'])
    return HostMap(version, hashlib.sha256(raw).hexdigest(), hosts, path)
