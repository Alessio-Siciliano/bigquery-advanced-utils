# pylint: disable=no-untyped-def
import unittest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
from io import StringIO

from bigquery_advanced_utils.utils.data_checks import (
    run_data_checks,
    check_columns,
    check_unique,
    check_no_nulls,
    check_numeric_range,
    check_string_pattern,
    check_date_format,
    check_datatype,
    check_in_set,
)


class TestDataChecks(unittest.TestCase):

    def setUp(self) -> None:
        self.header = ["name", "age", "email", "dob"]
        self.column_sums: dict[str, set] = {n: set() for n in self.header}
        patch("google.cloud.storage.Client.__init__", lambda x: None).start()

    @patch("os.path.exists")
    def test_local_file_not_found(self, mock_exists) -> None:
        # Simulate a non-existing local file
        mock_exists.return_value = False

        with self.assertRaises(FileNotFoundError):
            run_data_checks(
                "local_file.csv", [lambda idx, row, header, sums: None]
            )

    @patch("google.cloud.storage.blob.Blob.exists")
    @patch("google.cloud.storage.Client")
    def test_gcs_file_not_found(
        self, mock_storage_client, mock_exists
    ) -> None:
        # Simulate a non-existing filen on GCS
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_exists.return_value = False
        mock_storage_client.return_value = mock_client

        with self.assertRaises(FileNotFoundError):
            run_data_checks(
                "gs://bucket_name/file.csv",
                [lambda idx, row, header, sums: None],
            )

    @patch("google.cloud.storage.blob.Blob.exists")
    @patch("google.cloud.storage.Blob.download_as_text")
    def test_gcs_file_download(self, mock_download_as_text, mock_exists):
        mock_exists.return_value = True
        mock_download_as_text.return_value = "test,data\n1,2\n"

        run_data_checks(
            "gs://bucket_name/file.csv", [lambda idx, row, header, sums: None]
        )

    @patch("os.path.exists")
    def test_empty_data_checks(self, mock_exists) -> None:
        # Simulate an existing local file
        mock_exists.return_value = True

        with self.assertRaises(ValueError):
            run_data_checks("local_file.csv", [])

    @patch("os.path.exists")
    def test_invalid_csv_format(self, mock_exists) -> None:
        # Simulate an existing local CSV file without header
        mock_exists.return_value = True

        with patch("builtins.open", mock_open(read_data="")):
            with self.assertRaises(ValueError):
                run_data_checks(
                    "local_file.csv", [lambda idx, row, header, sums: None]
                )

    @patch("os.path.exists")
    def test_successful_data_checks(self, mock_exists):
        # Simulate a good file
        mock_exists.return_value = True

        # Mock di un file CSV con una sola riga
        csv_data = "column1,column2\nvalue1,value2"
        mock_file = StringIO(csv_data)

        with patch("builtins.open", return_value=mock_file):
            result = run_data_checks(
                "local_file.csv",
                [
                    lambda idx, row, header, sums: sums["column1"].add(
                        row["column1"]
                    )
                ],
            )

        self.assertTrue(result)

    @patch("os.path.exists")
    def test_failed_data_checks(self, mock_exists):
        # Simulate a correct file but with a failure test
        mock_exists.return_value = True

        # Mock of a single-row CSV file
        csv_data = "column1,column2\nvalue1,value2"
        mock_file = StringIO(csv_data)

        # Should trigger error
        def failing_test(idx, row, header, sums):
            raise ValueError("Test failed.")

        with patch("builtins.open", return_value=mock_file):
            result = run_data_checks("local_file.csv", [failing_test])

        self.assertFalse(result)

    # Test check_columns
    def test_check_columns_valid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        check_columns(1, row, self.header, self.column_sums)

    def test_check_columns_invalid(self) -> None:
        row = {
            "name": "John",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_columns(1, row, self.header, self.column_sums)

    # Test check_unique
    def test_check_unique_valid(self) -> None:
        row1 = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        row2 = {
            "name": "Eric",
            "age": "3",
            "email": "eric@example.com",
            "dob": "1996-01-01",
        }
        check_unique(1, row1, self.header, self.column_sums)
        check_unique(2, row2, self.header, self.column_sums)

    def test_check_unique_invalid(self) -> None:
        row1 = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        row2 = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        check_unique(1, row1, self.header, self.column_sums)
        with self.assertRaises(ValueError):
            check_unique(2, row2, self.header, self.column_sums)

    def test_check_unique_invalid_column_name(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_unique(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
            )

    # Test check_no_nulls
    def test_check_no_nulls_valid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        check_no_nulls(1, row, self.header, self.column_sums)

    def test_check_no_nulls_invalid(self) -> None:
        row = {
            "name": "John",
            "age": "",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_no_nulls(1, row, self.header, self.column_sums)

    def test_check_no_nulls_invalid_column_name(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_no_nulls(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
            )

    # Test check_numeric_range
    def test_check_numeric_range_valid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        check_numeric_range(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["age"],
            min_value=18,
            max_value=35,
        )

    def test_check_numeric_range_invalid(self) -> None:
        row = {
            "name": "John",
            "age": "40",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_numeric_range(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["age"],
                min_value=18,
                max_value=35,
            )

    def test_check_numeric_range_missing_min_or_max(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_numeric_range(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
                max_value=35,
            )

    def test_check_numeric_range_invalid_column_name(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_numeric_range(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
                min_value=18,
                max_value=35,
            )

    def test_check_numeric_range_invalid_value(self) -> None:
        row = {
            "name": "John",
            "age": "hello",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_numeric_range(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["age"],
                min_value=18,
                max_value=35,
            )

    def test_check_numeric_range_null_value(self) -> None:
        row1 = {
            "name": "John",
            "age": "",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        row2 = {
            "name": "John",
            "age": "20",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        valid_rows = []
        for i, row in enumerate([row1, row2]):
            check_numeric_range(
                i,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["age"],
                min_value=18,
                max_value=35,
            )

            if row.get("age", "") != "":
                valid_rows.append(row)

        expected_output = [
            {
                "name": "John",
                "age": "20",
                "email": "john@example.com",
                "dob": "1993-01-01",
            }
        ]
        self.assertEqual(valid_rows, expected_output)

    # Test check_string_pattern
    def test_check_string_pattern_valid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        check_string_pattern(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["email"],
            regex_pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        )

    def test_check_string_pattern_invalid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "invalid",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_string_pattern(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["email"],
                regex_pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
            )

    def test_check_string_pattern_missing_pattern(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "invalid",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_string_pattern(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["email"],
            )

    def test_check_string_pattern_invalid_pattern(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "invalid",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_string_pattern(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["email"],
                regex_pattern="[",
            )

    def test_check_string_pattern_invalid_column_name(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "invalid",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_string_pattern(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column_name"],
                regex_pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
            )

    # Test check_date_format
    def test_check_date_format_valid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-1-1",
        }
        check_date_format(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["dob"],
            date_format="%Y-%m-%d",
        )

    def test_check_date_format_invalid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "01/01/1993",
        }
        with self.assertRaises(ValueError):
            check_date_format(
                1, row, self.header, self.column_sums, date_format="%Y-%m-%d"
            )

    def test_check_date_format_invalid_column_name(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "01/01/1993",
        }
        with self.assertRaises(ValueError):
            check_date_format(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column_name"],
                date_format="%Y-%m-%d",
            )

    # Test check_datatype
    def test_check_datatype_valid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        check_datatype(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["age"],
            expected_datatype=int,
        )

    def test_check_datatype_missing_expected_datatype(self) -> None:
        row = {
            "name": "John",
            "age": "30.5",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_datatype(1, row, self.header, self.column_sums)

    def test_check_datatype_invalid(self) -> None:
        row = {
            "name": "John",
            "age": "30.5",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_datatype(
                1, row, self.header, self.column_sums, expected_datatype=int
            )

    def test_check_datatype_invalid_column_name(self) -> None:
        row = {
            "name": "John",
            "age": "30.5",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_datatype(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column_name"],
                expected_datatype=int,
            )

    # Test check_in_set
    def test_check_in_set_valid(self) -> None:
        row = {
            "name": "John",
            "age": "30",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        check_in_set(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["age"],
            valid_values_set=[30, 25, 40],
        )

    def test_check_in_set_invalid(self) -> None:
        row = {
            "name": "John",
            "age": "50",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_in_set(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["age"],
                valid_values_set=[30, 25, 40],
            )

    def test_check_in_set_invalid_datatype(self) -> None:
        row = {
            "name": "John",
            "age": "50",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_in_set(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["email"],
                valid_values_set=[30, 25, 40],
            )

    def test_check_in_set_no_set(self) -> None:
        row = {
            "name": "John",
            "age": "50",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_in_set(
                1,
                row,
                self.header,
                self.column_sums,
            )

    def test_check_in_set_invalid_column_name(self) -> None:
        row = {
            "name": "John",
            "age": "50",
            "email": "john@example.com",
            "dob": "1993-01-01",
        }
        with self.assertRaises(ValueError):
            check_in_set(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column_name"],
                valid_values_set=[30, 25, 40],
            )


if __name__ == "__main__":
    unittest.main()
