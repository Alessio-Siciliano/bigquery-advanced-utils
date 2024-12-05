""" This module provides a set of useful functions to manipulate strings. """

import re
from bigquery_advanced_utils.utils.custom_exceptions import (
    InvalidArgumentToFunction,
)


class String:
    """This class provides all the utils functions for strings."""

    # Rule to remove all text inside a comment in each language.
    COMMENTS_PATTERNS = {"standard_sql": r"//.*|--.*|\/\*.*?\*\/"}

    # REGEX to identify a table with the pattern <project>.<dataset>.<table>
    TABLES_PATTERN = r"[\w'\"`_-]+\.[\w'\"`_-]+\.[\w'\"`_-]+"

    # REGEX pattern to identify all non alphanumeric, ., -,_
    NON_ALPHANUMERIC_CHARS = "[^a-zA-Z0-9._\\s-]"

    def remove_chars_from_string(
        self, string: str, chars_to_remove: list[str]
    ) -> str:
        """Removes some special characters from a given string.

        Parameters
        ----------
        string : str
            A string with text

        chars_to_remove: list[str]
            List of chars to remove from the given string

        Returns
        -------
        str
            The same string with no more the selected chars.

        Raises
        ------
        InvalidArgumentToFunction
            if the value passed to the function are wrong
        """
        if (
            string is None
            or not isinstance(chars_to_remove, list)
            or len(chars_to_remove) == 0
        ):
            raise InvalidArgumentToFunction()

        return re.sub("[" + "".join(chars_to_remove) + "]", "", string)

    def remove_comments_from_string(
        self, string: str, dialect: str = "standard_sql"
    ) -> str:
        """Removes all comments and the text inside from a given text string.

        Parameters
        ----------
        string : str
            A text with a query

        dialect: str
            Each language has its own coding rule for comments.
            Default: Standard SQL.

        Return
        ------
        str
            The same text with no more comments.

        Raises
        ------
        InvalidArgumentToFunction
            if the value passed to the function are wrong
        """
        if string is None:
            raise InvalidArgumentToFunction()
        return re.sub(self.COMMENTS_PATTERNS[dialect], "", string)

    def extract_tables_from_query(self, string: str) -> list[str]:
        """Extract all source tables from a query in a string.

        Parameters
        ----------
        string:
            Input query written in Standard SQL

        Returns
        -------
        List[str]
            A list with all sources tables.

        Raises
        ------
        InvalidArgumentToFunction
            if the value passed to the function are wrong
        """
        if string is None:
            raise InvalidArgumentToFunction()

        # Clear the input query, removing all comments and special chars
        cleaned_query = self.remove_comments_from_string(string)
        cleaned_query = re.sub(self.NON_ALPHANUMERIC_CHARS, "", cleaned_query)

        # Find all occurrences of the pattern inside the query
        matches = re.findall(self.TABLES_PATTERN, cleaned_query)

        # Remove duplicates with set()
        return list(set(matches))
