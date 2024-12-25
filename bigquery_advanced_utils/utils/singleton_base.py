""" Singleton base model. """

import threading
import logging
from typing import TypeVar, Dict, Type, cast


T = TypeVar("T", bound="SingletonBase")


class SingletonBase:  # pylint: disable=too-few-public-methods
    """Singleton base model."""

    _instances: Dict[Type["SingletonBase"], "SingletonBase"] = {}
    _lock: threading.Lock = threading.Lock()

    def __new__(cls: Type[T]) -> T:
        """This method is called when creating a new instance of the class.

        Parameters
        ----------
        cls: Callable
            Class.

        """
        logging.debug("Initialization of __new__ from SingletonBase")
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    try:
                        logging.debug(
                            "Creating a new %s instance.", cls.__name__
                        )
                        instance = super().__new__(cls)
                        # instance.__init__(*args, **kwargs)  # type: ignore
                        # instance._initialize(
                        #    *args,
                        #    **kwargs,
                        # )
                        cls._instances[cls] = instance
                        # cls._instances[cls]._initialized = (  # type: ignore
                        #    True
                        # )

                        logging.info(
                            "%s instance successfully initialized.",
                            cls.__name__,
                        )

                    except OSError as e:  # pragma: no cover
                        logging.error(  # pragma: no cover
                            "%s initialization error: %s",
                            cls.__name__,
                            e,
                        )
                        raise RuntimeError(  # pragma: no cover
                            f"Failed to initialize {cls.__name__}",
                        ) from e
        else:
            logging.info("Reusing existing %s instance.", cls.__name__)

        return cast(T, cls._instances[cls])
