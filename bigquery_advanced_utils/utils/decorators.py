""" Module with all decorators. """

# pylint: disable=import-outside-toplevel, protected-access, missing-param-doc, missing-return-doc
from typing import Callable, Any, Optional
from functools import wraps
import logging


def ensure_bigquery_instance(func: Callable) -> Callable:
    """Decorator"""

    @wraps(func)
    def wrapper(self, *args: Any, **kwargs: Any) -> Any:
        from bigquery_advanced_utils.bigquery import (
            BigQueryClient,
        )  # Assicurati che il nome del modulo sia corretto

        # Se l'istanza di BigQuery è già stata creata dall'utente, usiamola
        if (
            not hasattr(self, "_bigquery_instance")
            or self._bigquery_instance is None
        ):
            if BigQueryClient in BigQueryClient._instances:
                # Usa l'istanza esistente di BigQuery dal dizionario _instances
                self._bigquery_instance = BigQueryClient._instances[
                    BigQueryClient
                ]
                logging.debug("Using existing BigQuery instance.")
            else:
                # Altrimenti, crea un'istanza di BigQuery
                self._bigquery_instance = BigQueryClient()
                logging.debug("BigQuery instance created inside Storage.")
        else:
            logging.debug(
                "BigQuery instance already exists, reusing the same instance."
            )

        return func(
            self, *args, bigquery_instance=self._bigquery_instance, **kwargs
        )

    return wrapper


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
