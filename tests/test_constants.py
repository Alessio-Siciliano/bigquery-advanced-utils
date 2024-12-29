import unittest
import re
from bigquery_advanced_utils.core.constants import (
    TABLES_PATTERN,
    NON_ALPHANUMERIC_CHARS,
    COMMENTS_PATTERNS,
)


class TestRegex(unittest.TestCase):

    def test_valid_regex_tables_pattern(self):
        valid_table_name = "schema.table_name.field"
        invalid_table_name = "invalid_table_name"

        self.assertIsNotNone(re.match(TABLES_PATTERN, valid_table_name))
        self.assertIsNone(re.match(TABLES_PATTERN, invalid_table_name))

    def test_comment_patterns(self):
        comment_test_cases = [
            ("// This is a comment", True),  # Line comment (//)
            (
                "-- This is another comment",
                True,
            ),  # Line comment (--), SQL-style
            ("/* This is a block comment */", True),  # Block comment
            ("SELECT * FROM table;", False),  # Non-comment line
            (
                "/* Unmatched comment",
                False,
            ),  # Non-matching block comment (incomplete)
        ]

        for text, expected in comment_test_cases:
            if expected:
                self.assertIsNotNone(
                    re.match(COMMENTS_PATTERNS["standard_sql"], text)
                )
            else:
                self.assertIsNone(
                    re.match(COMMENTS_PATTERNS["standard_sql"], text)
                )

    def test_table_pattern(self):
        table_test_cases = [
            ("project.dataset.table", True),  # Valid table name
            ("myproject.mydataset.mytable", True),  # Another valid table name
            ("invalid_table", False),  # Invalid (missing dataset and project)
            ("project..table", False),  # Missing dataset
            ("project.dataset.", False),  # Missing table
        ]

        for table_name, expected in table_test_cases:
            if expected:
                self.assertIsNotNone(re.match(TABLES_PATTERN, table_name))
            else:
                self.assertIsNone(re.match(TABLES_PATTERN, table_name))

    def test_non_alphanumeric_chars(self):
        non_alphanumeric_test_cases = [
            (
                "valid_table_name",
                False,
            ),  # Valid, no non-alphanumeric characters
            ("table-name", False),  # Valid, "-" is allowed
            ("table_name", False),  # Valid, "_" is allowed
            ("table@name", True),  # Invalid character "@"
            ("table#name", True),  # Invalid character "#"
            (
                "table name",
                False,
            ),  # Space char is valid
        ]

        for text, expected in non_alphanumeric_test_cases:
            if expected:
                self.assertTrue(bool(re.search(NON_ALPHANUMERIC_CHARS, text)))
            else:
                self.assertFalse(bool(re.search(NON_ALPHANUMERIC_CHARS, text)))


if __name__ == "__main__":
    unittest.main()
