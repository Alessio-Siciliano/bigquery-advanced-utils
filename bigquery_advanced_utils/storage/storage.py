""" Module to manage all the functions regarding Cloud Storage. """

import logging
import csv
import threading
from typing import Optional, Any, Type

from google.cloud.storage import Client  # type: ignore


# in tutti gli altri file la richiamo così "gcs_instance = GoogleCloudStorage()" (dopo aver messo il decoratore)
class CloudStorageClient(Client):
    """Singleton Cloud Storage Client class (child of the original client)"""

    _instance: Optional["CloudStorageClient"] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(
        cls: Type["CloudStorageClient"], *args: Any, **kwargs: Any
    ) -> "CloudStorageClient":
        if cls._instance is None:
            with cls._lock:  # Ensure thread safety
                if cls._instance is None:  # Double-checked locking
                    try:
                        logging.debug(
                            "Creating a new CloudStorageClient instance."
                        )
                        cls._instance = super().__new__(
                            cls,
                        )
                        cls._instance.__init__(*args, **kwargs)  # type: ignore
                        Client.__init__(cls._instance, *args, **kwargs)
                        logging.info(
                            "CloudStorageClient instance "
                            "successfully initialized."
                        )
                    except OSError as e:
                        logging.error(
                            "CloudStorageClient initialization error: %s", e
                        )
                        raise RuntimeError(
                            "Failed to initialize CloudStorageClient"
                        ) from e
        else:
            logging.debug("Reusing existing CloudStorageClient instance.")
        return cls._instance

    def __init__(self, *args: tuple, **kwargs: dict) -> None:
        if not hasattr(self, "initialized"):
            try:
                logging.debug("Starting the CloudStorageClient")
                super().__init__(*args, **kwargs)
                logging.debug("BigQueryClient successfully initialized.")
                self.initialized = True
            except OSError as e:
                logging.error("CloudStorageClient initialization error: %s", e)
                raise

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
