""" Module to manage all the functions regarding Cloud Storage. """

import csv
from typing import Any
from google.cloud.storage import Client  # type: ignore
from bigquery_advanced_utils.utils import SingletonBase
from bigquery_advanced_utils.utils.decorators import run_once


# in tutti gli altri file la richiamo così "gcs_instance = GoogleCloudStorage()" (dopo aver messo il decoratore)
class CloudStorageClient(Client, SingletonBase):
    """Singleton Cloud Storage Client class (child of the original client)"""

    @run_once
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def _process_csv(self, bucket_name: str, blob_name: str) -> None:
        """Read and process a CSV file from a Google Cloud Storage bucket.

        Parameters
        ----------
        bucket_name: str
            The name of the bucket.

        blob_name: str
            The name of the blob.

        """

        bucket = self.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        with blob.open("r") as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                print(row)

    # def data_checks_file(self, bucket_name: str, blob_name: str) -> None:
    #    """Check the data in a file.


#
#    Parameters
#    ----------
#    bucket_name: str
#        The name of the bucket.
#
#    blob_name: str
#        The name of the blob.
#
#    """
#    # First retrieve the extension of the file
#    extension = blob_name.split(".")[-1]
#
#    # Get the filename with extension
#    filename = blob_name.split("/")[-1]
#
#    try:
#        logging.debug("Starting checks of file: %s", filename)
#        if extension == "csv":
#            self._process_csv(bucket_name, blob_name)
#        else:
#            raise ValueError("File extension not yet supported.")
#    except (TypeError, ValueError) as e:
#        logging.error("Validation failed for row %d: %s", idx, e)
#        raise
