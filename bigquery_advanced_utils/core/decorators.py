""" Module with all decorators. """

# pylint: disable=import-outside-toplevel, protected-access, missing-param-doc, missing-return-doc
from typing import Callable, Any, Optional, Type, List
from functools import wraps
from bigquery_advanced_utils.core import SingletonBase


def run_once(  # pylint: disable=missing-return-doc,missing-function-docstring
    method: Callable,
) -> Callable:
    def wrapper(  # pylint: disable=missing-return-doc
        self: Any, *args: Any, **kwargs: Any
    ) -> Optional[Callable]:
        if not getattr(self, "_initialized", False):
            result = method(self, *args, **kwargs)
            self._initialized = True  # pylint: disable=protected-access
            return result
        return None

    return wrapper


def singleton_instance(class_types: List[Type[SingletonBase]]) -> Callable:
    """Decorator to get the singleton instance of a specific class.

    Parameters:
        class_types: The classes from which you want to get
            the singleton instance
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Get the singleton instances of the classes passed as parameters
            instances = {
                f"{cls.__name__}_instance": cls() for cls in class_types
            }

            # Pass the instances as keyword arguments to the function
            return func(self, *args, **kwargs, **instances)

        return wrapper

    return decorator
