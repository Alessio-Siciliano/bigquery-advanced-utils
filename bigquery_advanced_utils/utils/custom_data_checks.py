""" This module provides a set of custom Data Checks. """

import re
from datetime import datetime
from typing import Optional


class CustomDataChecks:
    """Set of useful data checks functions."""

    def check_columns(
        self,
        idx: int,
        row: list,
        column_sums: list,  # pylint: disable=unused-argument
        header: list,
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
                f"Row {idx} has a different number of values. "
                f"Row length: {len(row)}, Number of columns: {len(header)}"
            )

    def check_unique(
        self,
        idx: int,
        row: list,
        header: list,
        column_sums: list,
        columns_to_test: Optional[list] = None,
    ) -> None:
        """Check if a column has unique values.

        Parameters
        ----------
        idx: int
            Row number.

        row: list
            list of values of the given row for each column.

        header: list
            list of columns names

        column_sums: list
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

            col_index = header.index(column_name)
            value = row[col_index]

            if value in column_sums[col_index]:
                raise ValueError(
                    f"Row {idx}: Duplicate value '{value}'"
                    f" found in column '{column_name}'."
                )
            column_sums[col_index].add(value)

    def check_no_nulls(
        self,
        idx: int,
        row: list,
        header: list,
        column_sums: list,  # pylint: disable=unused-argument
        columns_to_test: Optional[list] = None,
    ) -> None:
        """Check if a column has null.

        Parameters
        ----------
        idx: int
            Row number.

        row: list
            list of values of the given row for each column.

        header: list
            list of columns names

        column_sums: list
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

            col_index = header.index(column_name)
            if row[col_index].strip() == "":
                raise ValueError(
                    f"Row {idx}: NULL value found in column '{column_name}'."
                )

    def check_numeric_range(
        self,
        idx: int,
        row: list,
        header: list,
        column_sums: list,  # pylint: disable=unused-argument
        columns_to_test: Optional[list] | None = None,
        min_value: Optional[int | float] = None,
        max_value: Optional[int | float] = None,
    ) -> None:
        """Check if a column has values in the interval.
        This function allows NULL.

        Parameters
        ----------
        idx: int
            Row number.

        row: list
            list of values of the given row for each column.

        header: list
            list of columns names

        column_sums: list
            list of sets

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

            col_index = header.index(column_name)
            value = row[col_index]

            if value == "" or value is None:
                continue

            try:
                numeric_value = float(value)
            except ValueError as exc:
                raise ValueError(
                    (
                        f"Row {idx}: "
                        f"Non-numeric value '{value}' "
                        f"found in column '{column_name}'."
                    )
                ) from exc

            # Check the interval
            if (min_value is not None and numeric_value < min_value) or (
                max_value is not None and numeric_value > max_value
            ):
                raise ValueError(
                    (
                        f"Row {idx}: "
                        f"Value '{numeric_value}' "
                        f"in column '{column_name}' "
                        f"is out of range "
                        f"({min_value} to {max_value})."
                    )
                )

    # EMAIL: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    # CELL con prefisso: "^\+?[1-9]\d{1,14}$"
    def check_string_pattern(
        self,
        idx: int,
        row: list,
        header: list,
        column_sums: list,  # pylint: disable=unused-argument
        columns_to_test: Optional[list] = None,
        regex_pattern: str = "",
    ) -> None:
        """Check if a column matches a regex pattern.
        This function allows NULL

        Parameters
        ----------
        idx: int
            Row number.

        row: list
            list of values of the given row for each column.

        header: list
            list of columns names.

        column_sums: list
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
            regex = re.compile(regex_pattern)
        except re.error as e:
            raise ValueError(f"Pattern regex is not valid: {e}") from e

        for column_name in columns_to_test:
            if column_name not in header:
                raise ValueError(f"Column '{column_name}' not in the header.")

            col_index = header.index(column_name)
            value = row[col_index]

            if not regex.match(value) and value != "" and value is not None:
                raise ValueError(
                    (
                        f"Row {idx}: "
                        f"Value '{value}' inside the column '{column_name}' "
                        f"does not match the regex pattern. "
                        f"Pattern: {regex_pattern}."
                    )
                )

    def check_date_format(
        self,
        idx: int,
        row: list,
        header: list,
        column_sums: list,  # pylint: disable=unused-argument
        columns_to_test: Optional[list] = None,
        date_format: str = "%Y-%m-%d",
    ) -> None:
        """Check if the date match the pattern.
        This function allows NULL

        Parameters
        ----------
        idx: int
            Row number.

        row: list
            list of values of the given row for each column.

        header: list
            list of columns names.

        column_sums: list
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
                raise ValueError(
                    f"Column '{column_name}' not inside the header."
                )

            col_index = header.index(column_name)
            value = row[col_index]

            try:
                # Let's try to parse the string only if string
                if (
                    isinstance(value, str)
                    and value is not None
                    and value != ""
                ):
                    datetime.strptime(value, date_format)

            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"Row {idx}: "
                    f"The column '{column_name}'"
                    f" contains an invalid value '{value}'. "
                    f"Expected format: {date_format}. "
                    f"Error: {str(e)}"
                ) from e

    def check_datatype(
        self,
        idx: int,
        row: list,
        header: list,
        column_sums: list,  # pylint: disable=unused-argument
        columns_to_test: Optional[list] = None,
        expected_datatype: Optional[type] = None,
    ) -> None:
        """Check if the column match a datatype.
        This function allows NULL

        Parameters
        ----------
        idx: int
            Row number.

        row: list
            list of values of the given row for each column.

        header: list
            list of columns names.

        column_sums: list
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
                raise ValueError(
                    f"Column '{column_name}' not inside the header."
                )

            col_index = header.index(column_name)
            value = row[col_index]

            try:
                if value != "" and value is not None:
                    expected_datatype(value)
            except Exception as e:
                raise ValueError(
                    f"Row {idx}: Value '{value}' in column '{column_name}' "
                    f"is not of type {expected_datatype.__name__}."
                ) from e

    def check_in_set(
        self,
        idx: int,
        row: list,
        header: list,
        column_sums: list,  # pylint: disable=unused-argument
        columns_to_test: Optional[list] = None,
        valid_values_set: Optional[list] = None,
    ) -> None:
        """Check if the value is inside a list
        If a field is NULL this function returns error

        Parameters
        ----------
        idx: int
            Row number.

        row: list
            list of values of the given row for each column.

        header: list
            list of columns names.

        column_sums: list
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
                raise ValueError(
                    f"Column '{column_name}' not inside the header."
                )

            col_index = header.index(column_name)
            value = row[col_index]

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
                        f"Row {idx}: "
                        f"The value '{value}' in column '{column_name}' "
                        f"is not valid. "
                        f"Valid values: {valid_values_set}."
                    )
                )
