""" This module provides a set of useful functions to manipulate strings. """

import re
from typing import Tuple
from bigquery_advanced_utils.utils.exceptions import (
    InvalidArgumentToFunction,
)
from bigquery_advanced_utils.core.constants import (
    COMMENTS_PATTERNS,
    NON_ALPHANUMERIC_CHARS,
    TABLES_PATTERN,
)


def remove_chars_from_string(string: str, chars_to_remove: list[str]) -> str:
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
        or not chars_to_remove
    ):
        raise InvalidArgumentToFunction()

    return re.sub("[" + "".join(chars_to_remove) + "]", "", string)


def remove_comments_from_string(
    string: str, dialect: str = "standard_sql"
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
    return re.sub(COMMENTS_PATTERNS[dialect], "", string)


def extract_tables_from_query(string: str) -> list[str]:
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
    cleaned_query = remove_comments_from_string(string)
    cleaned_query = re.sub(NON_ALPHANUMERIC_CHARS, "", cleaned_query)

    # Find all occurrences of the pattern inside the query
    matches = re.findall(TABLES_PATTERN, cleaned_query)

    # Remove duplicates with set()
    return list(set(matches))


def parse_gcs_path(gcs_uri: str) -> Tuple[str, str]:
    """Parses a GCS URI and returns the bucket name and path.

    Parameters
    ----------
    gcs_uri: str
        URL of a storage bucket/element.

    Return
    -------
    bucket_name
        Name of the bucket.

    folder
        Folder inside the bucket.

    Raises
    ------
    ValueError
        Wrong URL.
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError("Path must start with 'gs://'")
    path_parts = gcs_uri.replace("gs://", "").split("/")
    bucket_name = path_parts[0]
    folder = "/".join(path_parts[1:-1]) if len(path_parts) > 1 else ""
    return bucket_name, folder


def is_regex_pattern_valid(pattern: str) -> bool:
    """Function to validate a regex pattern.

    Parameters
    --------
    pattern: str
        The pattern to validate.

    Returns
    --------
    bool
        The result of compile:
            * True: is a valid pattern
            * False: not

    Raises
    -------
    ValueError
        Trigger for invalid pattern.
    """
    try:
        re.compile(pattern)
        is_valid = True
    except re.error as e:
        is_valid = False
        raise ValueError(f"Pattern regex is not valid: {e}") from e

    return is_valid
