""" This module provides a set of custom Data Checks. """

import re
import os
import csv
from io import StringIO
import logging
from datetime import datetime
from typing import Optional, Union, Callable
from bigquery_advanced_utils.core.decorators import singleton_instance
from bigquery_advanced_utils.storage import CloudStorageClient


@singleton_instance([CloudStorageClient])
def run_data_checks(  # pylint: disable=too-many-locals
    file_path: str, data_checks: list[Callable], delimiter: str = ",", **kwargs
) -> bool:
    """Run data checks on local file or on GCS.

    Parameters
    ----------
    file_path : str
        Location of the file.

    data_checks: list[Callable]
        List of test functions.

    delimiter: Optional[str]
        Delimiter.

    **kwargs
        Keywords arguments.

    Returns
    ----------
    bool
        True if all success.

    Raises
    ----------
    ValueError
        Arguments with wrong length.
    FileNotFoundError
        File not found for the specific path.
    """
    logging.debug("Starting data checks from : %s", file_path)

    if not data_checks:
        raise ValueError("Should pass at least one test function.")
    # Check if the location is Cloud Storage
    if file_path.startswith("gs://"):
        client = kwargs.get("CloudStorageClient_instance")
        bucket_name, blob_name = file_path[5:].split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            raise FileNotFoundError(f"File not found in GCS: {file_path}")
        file_obj = StringIO(blob.download_as_text())

    else:
        # File is local
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Local file not found: {file_path}")
        uri = os.path.abspath(file_path)
        file_obj = open(  # type: ignore
            uri, mode="r", newline="", encoding="utf-8"
        )

    with file_obj:
        reader = csv.DictReader(file_obj, delimiter=delimiter)
        header = reader.fieldnames
        if not header:
            raise ValueError("CSV with wrong format or missing header.")
        column_sums: dict[str, set] = {n: set() for n in header}

        # Process file row by row
        for idx, row in enumerate(reader, start=1):
            # Run column-specific tests
            for test_function in data_checks:
                try:
                    test_function(idx, row, header, column_sums)
                except (TypeError, ValueError) as e:
                    logging.error("Validation failed: %s", e)
                    return False
    logging.debug("All data checks passed.")
    return True


def check_columns(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,  # pylint: disable=unused-argument
) -> None:
    """Check if the CSV has the correct format.
    (all rows have the same lenght)

    Parameters
    ----------
    idx: int
        Row number.

    row: list
        list of values of the given row for each column.

    header: list
        list of columns names

    column_sums: list
        list of memory set

    Raises
    ------
    ValueError
        if the CSV is wrong
    """

    # Check if all rows have the same length
    if len(row) != len(header):
        raise ValueError(
            f"row {idx} has a different number of values. "
            f"Row length: {len(row)}, Number of columns: {len(header)}"
        )


def check_unique(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,
    columns_to_test: Optional[list] = None,
) -> None:
    """Check if a column has unique values.

    Parameters
    ----------
    idx: int
        Row number.

    row: dict
        list of values of the given row for each column.

    header: list
        list of columns names

    column_sums: dict
        list of set for each column. Usefull to calculate sums/unique/..

    columns_to_test: list
        list of columns to check

    Raises
    ------
    ValueError
        if the column has duplicates
    """
    columns_to_test = columns_to_test or header

    for column_name in columns_to_test:
        if column_name not in header:
            raise ValueError(
                f"Column '{column_name}' not found in the header."
            )

        value = row.get(column_name)

        if value in column_sums.get(column_name):  # type: ignore
            raise ValueError(
                f"Duplicate value '{value}'"
                f" found at row {idx} in column '{column_name}'."
            )
        column_sums.get(column_name).add(value)  # type: ignore


def check_no_nulls(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,  # pylint: disable=unused-argument
    columns_to_test: Optional[list] = None,
) -> None:
    """Check if a column has null.

    Parameters
    ----------
    idx: int
        Row number.

    row: dict
        list of values of the given row for each column.

    header: list
        list of columns names

    column_sums: dict
        list of set, one for each column

    columns_to_test: list
        list of columns to check

    Raises
    ------
    ValueError
        if the column has null
    """
    # Use all columns if columns_to_test is not specified
    columns_to_test = columns_to_test or header

    for column_name in columns_to_test:
        if column_name not in header:
            raise ValueError(
                f"Column '{column_name}' not found in the header."
            )

        if not row or row.get(column_name).strip() == "":  # type: ignore
            raise ValueError(
                f"NULL value found at row {idx} in column '{column_name}'."
            )


def check_numeric_range(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,  # pylint: disable=unused-argument
    columns_to_test: Optional[list] = None,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
) -> None:
    """Check if a column has values in the interval.
    This function allows NULL.

    Parameters
    ----------
    idx: int
        Row number.

    row: dict
        list of values of the given row for each column.

    header: list
        list of columns names

    column_sums: dict
        dict of sets

    columns_to_test: list
        list of columns to check

    min_value: float or int
        Minimum value for the desidered interval

    max_value: float or int
        Maximum value for the desidered interval.

    Raises
    ------
    ValueError
        if the column has value out of given range
    """
    columns_to_test = columns_to_test or header

    if min_value is None or max_value is None:
        raise ValueError("Min value or max value missing!")

    for column_name in columns_to_test:
        if column_name not in header:
            raise ValueError(
                f"Column '{column_name}' not found in the header."
            )

        value = row.get(column_name)

        if value == "" or value is None:
            continue

        try:
            numeric_value = float(value)
        except ValueError as exc:
            raise ValueError(
                (
                    f"non-numeric value '{value}' "
                    f"found at row {idx} in column '{column_name}'."
                )
            ) from exc

        # Check the interval
        if (min_value is not None and numeric_value < min_value) or (
            max_value is not None and numeric_value > max_value
        ):
            raise ValueError(
                (
                    f"value '{numeric_value}' "
                    f"found at row {idx}"
                    f" in column '{column_name}' "
                    f"is out of range "
                    f"({min_value} to {max_value})."
                )
            )


# email: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
# phone with prefix: "^\+?[1-9]\d{1,14}$"
def check_string_pattern(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,  # pylint: disable=unused-argument
    columns_to_test: Optional[list] = None,
    regex_pattern: str = "",
) -> None:
    """Check if a column matches a regex pattern.
    This function allows NULL

    Parameters
    ----------
    idx: int
        Row number.

    row: dict
        list of values of the given row for each column.

    header: list
        list of columns names.

    column_sums: dict
        list of setsfor specific checks.

    columns_to_test: list
        list of columns to check.

    regex_pattern: str
        REGEX used as pattern.

    Raises
    ------
    ValueError
        if the column has value different from the pattern
    """

    columns_to_test = columns_to_test or header

    if regex_pattern == "":
        raise ValueError("REGEX is NULL!")

    try:
        re.compile(regex_pattern)
    except re.error as e:
        raise ValueError(f"Pattern regex is not valid: {e}") from e

    for column_name in columns_to_test:
        if column_name not in header:
            raise ValueError(f"Column '{column_name}' not in the header.")

        value = row.get(column_name)

        if (
            not re.compile(regex_pattern).match(value)  # type: ignore
            and value != ""
            and value is not None
        ):
            raise ValueError(
                (
                    f"value '{value}' at row {idx} inside the "
                    f"column '{column_name}' "
                    f"does not match the regex pattern. "
                    f"Pattern: {regex_pattern}."
                )
            )


def check_date_format(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,  # pylint: disable=unused-argument
    columns_to_test: Optional[list] = None,
    date_format: str = "%Y-%m-%d",
) -> None:
    """Check if the date match the pattern.
    This function allows NULL

    Parameters
    ----------
    idx: int
        Row number.

    row: dict
        list of values of the given row for each column.

    header: list
        list of columns names.

    column_sums: dict
        list of set for specific checks.

    columns_to_test: list
        list of columns to check.

    date_format: str
        Format of the date.
        DEFAULT: "%Y-%m-%d"

    Raises
    ------
    ValueError
        if the column has value different from the pattern
    """
    columns_to_test = columns_to_test or header

    for column_name in columns_to_test:
        if column_name not in header:
            raise ValueError(f"Column '{column_name}' not inside the header.")

        value = row.get(column_name)

        try:
            # Let's try to parse the string only if string
            if isinstance(value, str) and value is not None and value != "":
                datetime.strptime(value, date_format)

        except (ValueError, TypeError) as e:
            raise ValueError(
                f"the column '{column_name}'"
                f" at row {idx}"
                f" contains an invalid value '{value}'. "
                f"Expected format: {date_format}. "
                f"Full error: {str(e)}"
            ) from e


def check_datatype(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,  # pylint: disable=unused-argument
    columns_to_test: Optional[list] = None,
    expected_datatype: Optional[type] = None,
) -> None:
    """Check if the column match a datatype.
    This function allows NULL

    Parameters
    ----------
    idx: int
        Row number.

    row: dict
        list of values of the given row for each column.

    header: list
        list of columns names.

    column_sums: dict
        list of set for specific checks.

    columns_to_test: list
        list of columns to check.

    expected_datatype: type
        Expected datatype of the column.

    Raises
    ------
    ValueError
        if the column matches the datatype
    """
    columns_to_test = columns_to_test or header

    if expected_datatype is None:
        raise ValueError("An expected datatype should be specified.")

    for column_name in columns_to_test:
        if column_name not in header:
            raise ValueError(f"Column '{column_name}' not inside the header.")

        value = row.get(column_name)

        try:
            if value != "" and value is not None:
                expected_datatype(value)
        except Exception as e:
            raise ValueError(
                f"value '{value}' at row {idx} in column '{column_name}' "
                f"is not of type {expected_datatype.__name__}."
            ) from e


def check_in_set(
    idx: int,
    row: dict,
    header: list,
    column_sums: dict,  # pylint: disable=unused-argument
    columns_to_test: Optional[list] = None,
    valid_values_set: Optional[list] = None,
) -> None:
    """Check if the value is inside a list
    If a field is NULL this function returns error

    Parameters
    ----------
    idx: int
        Row number.

    row: dict
        list of values of the given row for each column.

    header: list
        list of columns names.

    column_sums: dict
        list of set for specific checks.

    columns_to_test: list
        list of columns to check.

    valid_values_set: list
        list of possible values.

    Raises
    ------
    ValueError
        if the column contains values outside the list
    """
    columns_to_test = columns_to_test or header

    if valid_values_set is None:
        raise ValueError("The set of valid values cannot be empty")

    for column_name in columns_to_test:
        if column_name not in header:
            raise ValueError(f"Column '{column_name}' not inside the header.")

        value = row.get(column_name)

        found = False
        for item in valid_values_set:
            datatype_of_item_in_list = type(item)
            try:
                converted_value = datatype_of_item_in_list(value)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"The column data type does not match the type of the"
                    f" values provided for the check: {e}"
                ) from e

            if item == converted_value:
                found = True
                break

        if not found:
            raise ValueError(
                (
                    f"value '{value}' at row {idx} in column '{column_name}' "
                    f"is not valid. "
                    f"Valid values: {valid_values_set}."
                )
            )
