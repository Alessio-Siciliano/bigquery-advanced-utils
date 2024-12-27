import unittest
from bigquery_advanced_utils.utils.string_utils import (
    remove_chars_from_string,
    remove_comments_from_string,
    parse_gcs_path,
    extract_tables_from_query,
    is_regex_pattern_valid,
)
from bigquery_advanced_utils.utils.exceptions import (
    InvalidArgumentToFunction,
)


class TestStringMethods(unittest.TestCase):

    def test_remove_chars_from_string(self) -> None:
        self.assertEqual(
            remove_chars_from_string("hello world", ["l", "o"]),
            "he wrd",
        )
        self.assertEqual(remove_chars_from_string("", ["a"]), "")
        with self.assertRaises(InvalidArgumentToFunction):
            remove_chars_from_string(None, ["l"])
        with self.assertRaises(InvalidArgumentToFunction):
            remove_chars_from_string("test", None)

    def test_remove_comments_from_string(self) -> None:
        input_query = "SELECT * FROM table -- this is a comment"
        expected_output = "SELECT * FROM table "
        self.assertEqual(
            remove_comments_from_string(input_query), expected_output
        )
        with self.assertRaises(InvalidArgumentToFunction):
            remove_comments_from_string(None)

    def test_extract_tables_from_query(self) -> None:
        query = "SELECT * FROM project.dataset.table"
        self.assertEqual(
            extract_tables_from_query(query), ["project.dataset.table"]
        )
        with self.assertRaises(InvalidArgumentToFunction):
            extract_tables_from_query(None)

    def test_parse_gcs_path(self) -> None:
        gcs_uri = "gs://my-bucket/path/to/file"
        self.assertEqual(parse_gcs_path(gcs_uri), ("my-bucket", "path/to"))
        with self.assertRaises(ValueError):
            parse_gcs_path("http://example.com")

    def test_is_regex_pattern_valid(self) -> None:
        valid_pattern = r"\d+"
        invalid_pattern = r"\d+("  # Invalid regex
        self.assertTrue(is_regex_pattern_valid(valid_pattern))
        with self.assertRaises(ValueError):
            is_regex_pattern_valid(invalid_pattern)


if __name__ == "__main__":
    unittest.main()
