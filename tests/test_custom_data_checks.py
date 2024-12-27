# pylint: disable=no-untyped-def
import unittest
from datetime import datetime
from bigquery_advanced_utils.utils.data_checks import (
    check_columns,
    check_unique,
    check_no_nulls,
    check_numeric_range,
    check_string_pattern,
    check_date_format,
    check_datatype,
    check_in_set,
)


# Classe di test per CustomDataChecks
class TestCustomDataChecks(unittest.TestCase):

    # Dati di esempio per i test
    def setUp(self) -> None:
        self.header = ["name", "age", "email", "dob"]
        self.column_sums: list[set] = [
            set() for _ in self.header
        ]  # per evitare duplicati nelle colonne

    # Test check_columns
    def test_check_columns_valid(self) -> None:
        row = ["John", "30", "john@example.com", "1993-01-01"]
        check_columns(1, row, self.column_sums, self.header)

    def test_check_columns_invalid(self) -> None:
        row = ["John", "30", "john@example.com"]  # manca una colonna
        with self.assertRaises(ValueError):
            check_columns(1, row, self.column_sums, self.header)

    # Test check_unique
    def test_check_unique_valid(self) -> None:
        row1 = ["John", "30", "john@example.com", "1993-01-01"]
        row2 = ["Jane", "25", "jane@example.com", "1998-01-01"]
        check_unique(1, row1, self.header, self.column_sums)
        check_unique(2, row2, self.header, self.column_sums)

    def test_check_unique_invalid(self) -> None:
        row1 = ["John", "30", "john@example.com", "1993-01-01"]
        row2 = ["John", "30", "john@example.com", "1993-01-01"]  # Duplicato
        check_unique(1, row1, self.header, self.column_sums)
        with self.assertRaises(ValueError):
            check_unique(2, row2, self.header, self.column_sums)

    def test_check_unique_invalid_column_name(self) -> None:
        row1 = ["John", "30", "john@example.com", "1993-01-01"]
        with self.assertRaises(ValueError):
            check_unique(
                1,
                row1,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
            )

    # Test check_no_nulls
    def test_check_no_nulls_valid(self) -> None:
        row = ["John", "30", "john@example.com", "1993-01-01"]
        check_no_nulls(1, row, self.header, self.column_sums)

    def test_check_no_nulls_invalid(self) -> None:
        row = [
            "John",
            "",
            "john@example.com",
            "1993-01-01",
        ]  # Valore vuoto in "age"
        with self.assertRaises(ValueError):
            check_no_nulls(1, row, self.header, self.column_sums)

    def test_check_no_nulls_invalid_column_name(self) -> None:
        row1 = ["John", "30", "john@example.com", "1993-01-01"]
        with self.assertRaises(ValueError):
            check_no_nulls(
                1,
                row1,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
            )

    # Test check_numeric_range
    def test_check_numeric_range_valid(self) -> None:
        row = ["John", "30", "john@example.com", "1993-01-01"]
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
        row = [
            "John",
            "40",
            "john@example.com",
            "1993-01-01",
        ]  # "age" out of interval
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
        row1 = ["John", "30", "john@example.com", "1993-01-01"]
        with self.assertRaises(ValueError):
            check_numeric_range(
                1,
                row1,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
                max_value=35,
            )

    def test_check_numeric_range_invalid_column_name(self) -> None:
        row1 = ["John", "30", "john@example.com", "1993-01-01"]
        with self.assertRaises(ValueError):
            check_numeric_range(
                1,
                row1,
                self.header,
                self.column_sums,
                columns_to_test=["not_a_valid_column"],
                min_value=18,
                max_value=35,
            )

    def test_check_numeric_range_invalid_value(self) -> None:
        row1 = ["John", "hello", "john@example.com", "1993-01-01"]
        with self.assertRaises(ValueError):
            check_numeric_range(
                1,
                row1,
                self.header,
                self.column_sums,
                columns_to_test=["age"],
                min_value=18,
                max_value=35,
            )

    def test_check_numeric_range_null_value(self) -> None:
        row1 = ["John", "", "john@example.com", "1993-01-01"]
        row2 = ["John", "20", "john@example.com", "1993-01-01"]

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

            if row[self.header.index("age")] != "":
                valid_rows.append(row)

        expected_output = [["John", "20", "john@example.com", "1993-01-01"]]
        self.assertEqual(valid_rows, expected_output)

    # Test check_string_pattern
    def test_check_string_pattern_valid(self) -> None:
        row = ["John", "30", "john@example.com", "1993-01-01"]
        check_string_pattern(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["email"],
            regex_pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        )

    def test_check_string_pattern_invalid(self) -> None:
        row = ["John", "30", "invalid-email", "1993-01-01"]
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
        row = ["John", "30", "invalid-email", "1993-01-01"]
        with self.assertRaises(ValueError):
            check_string_pattern(
                1,
                row,
                self.header,
                self.column_sums,
                columns_to_test=["email"],
            )

    def test_check_string_pattern_invalid_pattern(self) -> None:
        row = ["John", "30", "invalid-email", "1993-01-01"]
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
        row = ["John", "30", "invalid-email", "1993-01-01"]
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
        row = ["John", "30", "john@example.com", "1993-01-01"]
        check_date_format(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["dob"],
            date_format="%Y-%m-%d",
        )

    def test_check_date_format_invalid(self) -> None:
        row = [
            "John",
            "30",
            "john@example.com",
            "01/01/1993",
        ]
        with self.assertRaises(ValueError):
            check_date_format(
                1, row, self.header, self.column_sums, date_format="%Y-%m-%d"
            )

    def test_check_date_format_invalid_column_name(self) -> None:
        row = [
            "John",
            "30",
            "john@example.com",
            "01/01/1993",
        ]
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
        row = ["John", "30", "john@example.com", "1993-01-01"]
        check_datatype(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["age"],
            expected_datatype=int,
        )

    def test_check_datatype_missing_expected_datatype(self) -> None:
        row = [
            "John",
            "30.5",
            "john@example.com",
            "1993-01-01",
        ]
        with self.assertRaises(ValueError):
            check_datatype(1, row, self.header, self.column_sums)

    def test_check_datatype_invalid(self) -> None:
        row = [
            "John",
            "30.5",
            "john@example.com",
            "1993-01-01",
        ]
        with self.assertRaises(ValueError):
            check_datatype(
                1, row, self.header, self.column_sums, expected_datatype=int
            )

    def test_check_datatype_invalid_column_name(self) -> None:
        row = [
            "John",
            "30.5",
            "john@example.com",
            "1993-01-01",
        ]
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
        row = ["John", "30", "john@example.com", "1993-01-01"]
        check_in_set(
            1,
            row,
            self.header,
            self.column_sums,
            columns_to_test=["age"],
            valid_values_set=[30, 25, 40],
        )

    def test_check_in_set_invalid(self) -> None:
        row = [
            "John",
            "50",
            "john@example.com",
            "1993-01-01",
        ]
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
        row = [
            "John",
            "50",
            "john@example.com",
            "1993-01-01",
        ]
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
        row = [
            "John",
            "50",
            "john@example.com",
            "1993-01-01",
        ]
        with self.assertRaises(ValueError):
            check_in_set(
                1,
                row,
                self.header,
                self.column_sums,
            )

    def test_check_in_set_invalid_column_name(self) -> None:
        row = [
            "John",
            "50",
            "john@example.com",
            "1993-01-01",
        ]  # "age" non valido
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
