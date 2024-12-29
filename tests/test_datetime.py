import unittest
from datetime import datetime
from bigquery_advanced_utils.utils.datetime_utils import resolve_datetime


class TestDatetimeFunctions(unittest.TestCase):

    def test_valid_string(self) -> None:
        """Test that a valid date string is correctly parsed into a datetime object."""
        date_string = "2024-12-29 15:30"
        result = resolve_datetime(date_string)
        expected = datetime(2024, 12, 29, 15, 30)
        self.assertEqual(result, expected)

    def test_valid_datetime(self) -> None:
        """Test that a valid datetime object is returned as is."""
        date_obj = datetime(2024, 12, 29, 15, 30)
        result = resolve_datetime(date_obj)
        self.assertEqual(result, date_obj)

    def test_invalid_string(self) -> None:
        """Test that an invalid date string raises a ValueError."""
        invalid_string = "invalid date"
        with self.assertRaises(ValueError):
            resolve_datetime(invalid_string)

    def test_invalid_parameter(self) -> None:
        """Test that an invalid parameter (non-datetime, non-string) raises a ValueError."""
        invalid_param = 12345  # An integer that is not a valid date
        with self.assertRaises(ValueError):
            resolve_datetime(invalid_param)


if __name__ == "__main__":
    unittest.main()
