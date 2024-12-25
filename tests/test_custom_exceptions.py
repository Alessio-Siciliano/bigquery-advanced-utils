import unittest
from bigquery_advanced_utils.utils import (
    InvalidArgumentToFunction,
    ScheduledQueryIdWrongFormat,
)


class TestCustomExceptions(unittest.TestCase):
    """Unit tests for custom exceptions."""

    def test_invalid_argument_to_function_message(self) -> None:
        """Test if InvalidArgumentToFunction contains the correct message."""
        with self.assertRaises(InvalidArgumentToFunction) as context:
            raise InvalidArgumentToFunction()
        self.assertEqual(
            str(context.exception),
            "Please pass the correct parameters to the function!",
        )

    def test_scheduled_query_id_wrong_format_message(self) -> None:
        """Test if ScheduledQueryIdWrongFormat contains the correct message."""
        with self.assertRaises(ScheduledQueryIdWrongFormat) as context:
            raise ScheduledQueryIdWrongFormat()
        self.assertEqual(
            str(context.exception), "The given ID isn't in the correct format"
        )

    def test_invalid_argument_to_function_is_exception(self) -> None:
        """Ensure InvalidArgumentToFunction is a subclass of Exception."""
        self.assertTrue(issubclass(InvalidArgumentToFunction, Exception))

    def test_scheduled_query_id_wrong_format_is_exception(self) -> None:
        """Ensure ScheduledQueryIdWrongFormat is a subclass of Exception."""
        self.assertTrue(issubclass(ScheduledQueryIdWrongFormat, Exception))


if __name__ == "__main__":
    unittest.main()
