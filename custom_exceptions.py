""" This module provides a set of custom Exceptions. """


class InvalidArgumentToFunction(Exception):
    """Exception when the arguments of a function are invalid."""

    def __init__(self) -> None:
        message = "Please pass the correct parameters to the function!"
        Exception.__init__(self, message)


class ScheduledQueryIdWrongFormat(Exception):
    """Exception when user send an invalid ID for a scheduled query."""

    def __init__(self) -> None:
        message = "The given ID isn't in the correct format"
        Exception.__init__(self, message)
