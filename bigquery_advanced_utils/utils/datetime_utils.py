"""Utilities for handling and processing datetime objects and date strings.

This module provides functions to parse date strings into datetime objects
and to validate or process inputs that may be either datetime objects or
strings representing dates.

Functions
---------
try_parse_datetime(data_string: str) -> Optional[datetime]
    Attempts to parse a string into a datetime object using common formats.
    
process_datetime(param: Union[datetime, str]) -> datetime
    Ensures an input is a datetime object, parsing strings if necessary.
"""

from datetime import datetime
from typing import Optional, Union


def try_parse_datetime(data_string: str) -> Optional[datetime]:
    """Attempts to parse a string into a datetime object.
    Uses common date formats for parsing.

    Parameters
    ----------
    data_string : str
        The string representing a date or datetime.

    Returns
    -------
    datetime or None
        A datetime object if parsing is successful.
        None if all formats fail.

    Raises
    ------
    None
        This function does not raise exceptions directly. If parsing fails,
        None is returned.
    """
    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(data_string, fmt)
        except ValueError:
            continue

    return None


def resolve_datetime(param: Union[datetime, str]) -> datetime:
    """Processes a parameter to ensure it is a datetime object. If a string is
    provided, it attempts to parse it into a datetime. If the input is
    invalid, raises an exception.

    Parameters
    ----------
    param : Union[datetime, str]
        The input parameter, either a datetime object or a string
        representing a date.

    Returns
    -------
    datetime
        A datetime object representing the input.

    Raises
    ------
    ValueError
        If the input is neither a valid datetime object nor a parseable
        date string.
    """
    if isinstance(param, datetime):
        return param

    if isinstance(param, str):
        parsed_date = try_parse_datetime(param)
        if parsed_date:
            return parsed_date

    raise ValueError(f"Invalid parameter: {param}")
