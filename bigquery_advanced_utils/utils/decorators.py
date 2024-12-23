""" Module with all decorators. """

from typing import Callable
from bigquery_advanced_utils.storage.storage import CloudStorageClient
from bigquery_advanced_utils.bigquery.bigquery import BigQueryClient


def ensure_cloud_storage_instance(func: Callable) -> Callable:
    """Decorator that ensures the Google Cloud Storage instance is valid.

    Parameters
    -------
    func: Callable
        Function.

    Return
    -------
    Callable
        Function.
    """

    def wrapper(*args, **kwargs) -> Callable:
        """Wrapper.

        Parameters
        -------
        *args
            Positional arguments.

        **kwargs
            Keyword arguments.

        Return
        -------
        Callable
            A wrapped execution function.

        Raises
        ------
        ValueError
            Client not valid.
        """
        gcs_instance = CloudStorageClient()
        if not gcs_instance:
            raise ValueError("Google Cloud Storage client is not valid.")
        return func(*args, **kwargs)

    return wrapper


def ensure_bigquery_instance(func: Callable) -> Callable:
    """Decorator that ensures the BigQuery instance is valid.

    Parameters
    ----------
    func: Callable
        A callable function.

    Return
    -------
    Callable
        A wrapped execution function.
    """

    def wrapper(*args, **kwargs) -> Callable:
        """Wrapper.

        Parameters
        -------
        *args
            Positional arguments.

        **kwargs
            Keyword arguments.

        Return
        -------
        Callable
            A wrapped execution function.

        Raises
        ------
        ValueError
            Client not valid.
        """
        bq_instance = BigQueryClient()
        if not bq_instance:
            raise ValueError("BigQuery client is not valid.")
        return func(*args, **kwargs)

    return wrapper
