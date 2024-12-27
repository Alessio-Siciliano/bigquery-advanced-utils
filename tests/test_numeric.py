import unittest
from bigquery_advanced_utils.utils.numeric_utils import convert_bytes_to_unit


class TestNumeric(unittest.TestCase):

    def test_convert_bytes_to_kb(self) -> None:
        result = convert_bytes_to_unit(1024, "KB")
        self.assertEqual(result, 1.0, "1024 bytes should be 1 KB")

    def test_convert_bytes_to_mb(self) -> None:
        result = convert_bytes_to_unit(1048576, "MB")
        self.assertEqual(result, 1.0, "1048576 bytes should be 1 MB")

    def test_convert_bytes_to_gb(self) -> None:
        result = convert_bytes_to_unit(1073741824, "GB")
        self.assertEqual(result, 1.0, "1073741824 bytes should be 1 GB")

    def test_convert_bytes_to_tb(self) -> None:
        result = convert_bytes_to_unit(1099511627776, "TB")
        self.assertEqual(result, 1.0, "1099511627776 bytes should be 1 TB")

    def test_convert_bytes_to_unit_invalid_unit(self) -> None:
        with self.assertRaises(ValueError) as context:
            convert_bytes_to_unit(1024, "INVALID")
        self.assertIn("Unsupported unit", str(context.exception))

    def test_convert_bytes_to_unit_case_insensitivity(self) -> None:
        result = convert_bytes_to_unit(1024, "kb")
        self.assertEqual(
            result, 1.0, "The method should handle unit case insensitivity"
        )

    def test_convert_bytes_to_unit_negative_bytes(self) -> None:
        result = convert_bytes_to_unit(-1024, "KB")
        self.assertEqual(
            result, -1.0, "Negative byte values should be supported"
        )

    def test_convert_bytes_to_unit_zero_bytes(self) -> None:
        result = convert_bytes_to_unit(0, "KB")
        self.assertEqual(
            result, 0.0, "Zero bytes should result in zero for any unit"
        )


if __name__ == "__main__":
    unittest.main()
