from parse_roa import ParsedROA, compute_semantic_hash, serialise_semantic_model
from util import PrefixAuthorisation


def test_semantic_hash_stable_under_order_changes():
    roa_a = ParsedROA(
        asn=64500,
        prefixes=[
            PrefixAuthorisation(afi=4, prefix="192.0.2.0/24", max_length=24),
            PrefixAuthorisation(afi=6, prefix="2001:db8::/32", max_length=32),
        ],
        ee_serial=None,
        ee_not_before=None,
        ee_not_after=None,
        signing_time=None,
        ee_public_key=None,
    )

    roa_b = ParsedROA(
        asn=64500,
        prefixes=[
            PrefixAuthorisation(afi=6, prefix="2001:db8::/32", max_length=32),
            PrefixAuthorisation(afi=4, prefix="192.0.2.0/24", max_length=24),
        ],
        ee_serial=None,
        ee_not_before=None,
        ee_not_after=None,
        signing_time=None,
        ee_public_key=None,
    )

    assert serialise_semantic_model(roa_a) == serialise_semantic_model(roa_b)
    assert compute_semantic_hash(roa_a) == compute_semantic_hash(roa_b)
