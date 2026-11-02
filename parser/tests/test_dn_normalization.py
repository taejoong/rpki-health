import unittest

from util import clean_dn


class TestDNNormalization(unittest.TestCase):
    def test_common_name_only(self):
        self.assertEqual(clean_dn("Common Name: AFRINIC"), "AFRINIC")

    def test_serial_then_org(self):
        self.assertEqual(
            clean_dn(
                "Serial Number: B87C5A75F3D957413AB998646946D4541D511455, AFRINIC"
            ),
            "AFRINIC",
        )

    def test_serial_then_chain(self):
        self.assertEqual(
            clean_dn(
                "Serial Number: 2B57897A7CA964C3C8B7F7BDDAA7A4DA34A98F80, ARINTOAFRINIC"
            ),
            "ARINTOAFRINIC",
        )
        self.assertEqual(
            clean_dn(
                "Serial Number: D5BF87DA3466C82D8C6D8E34E3BDD85BCD39D2EB, LACNICTOAFRINIC"
            ),
            "LACNICTOAFRINIC",
        )

    def test_mixed_spaces_commas(self):
        self.assertEqual(clean_dn("Serial Number: ABCD1234  ,   AFRINIC  "), "AFRINIC")

    def test_serial_number_with_equals(self):
        self.assertEqual(
            clean_dn("serial number = deadbeef, Example Org"),
            "Example Org",
        )


    def test_common_name_variants(self):
        self.assertEqual(clean_dn("Common Name : AFRINIC"), "AFRINIC")
        self.assertEqual(clean_dn("CN=AFRINIC"), "AFRINIC")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
