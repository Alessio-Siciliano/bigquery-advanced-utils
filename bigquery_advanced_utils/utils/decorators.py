from typing import Callable
from bigquery_advanced_utils.storage.storage import GoogleCloudStorage
from bigquery_advanced_utils.bigquery.bigquery import BigQueryClient


def ensure_storage_instance(func: Callable) -> Callable:
    """
    Decorator that ensures the Google Cloud Storage instance is valid.
    """

    def wrapper(*args, **kwargs):
        gcs_instance = GoogleCloudStorage()
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
