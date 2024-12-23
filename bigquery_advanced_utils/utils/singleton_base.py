""" Singleton base model. """

import threading
import logging
from typing import TypeVar, Dict, Type, Any, cast


T = TypeVar("T", bound="SingletonBase")


class SingletonBase:
    """Singleton base model."""

    _instances: Dict[Type["SingletonBase"], "SingletonBase"] = {}
    _lock: threading.Lock = threading.Lock()

    def __new__(cls: Type[T], *args: Any, **kwargs: Any) -> T:
        """Read and process a CSV file from a Google Cloud Storage bucket.

        Parameters
        ----------
        cls: Callable
            Class.

        """

        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    try:
                        logging.debug(
                            "Creating a new %s instance.", cls.__name__
                        )
                        instance = super().__new__(cls)
                        # instance.__init__(*args, **kwargs)
                        instance._initialize(  # type: ignore # pylint: disable=no-member
                            *args,
                            **kwargs,
                        )
                        cls._instances[cls] = instance

                        logging.info(
                            "%s instance successfully initialized.",
                            cls.__name__,
                        )
                    except OSError as e:
                        logging.error(
                            "%s initialization error: %s",
                            cls.__name__,
                            e,
                        )
                        raise RuntimeError(
                            f"Failed to initialize {cls.__name__}",
                        ) from e
        else:
            logging.info("Reusing existing %s instance.", cls.__name__)
        return cast(T, cls._instances[cls])
