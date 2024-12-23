""" Module with all decorators. """

from typing import Callable, Any


def run_once(  # pylint: disable=missing-return-doc,missing-function-docstring
    method: Callable,
) -> Callable:
    def wrapper(  # pylint: disable=missing-return-doc
        self: Any, *args: Any, **kwargs: Any
    ) -> Callable | None:
        if not getattr(self, "_initialized", False):
            result = method(self, *args, **kwargs)
            self._initialized = True  # pylint: disable=protected-access
            return result
        return None

    return wrapper
