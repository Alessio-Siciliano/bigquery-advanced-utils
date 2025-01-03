""" Module to manage all the functions regarding Cloud Storage. """

import csv
import json
from typing import Any, Optional
from io import StringIO
from google.cloud.storage import Client  # type: ignore
from bigquery_advanced_utils.core import SingletonBase
from bigquery_advanced_utils.core.decorators import run_once


class CloudStorageClient(Client, SingletonBase):
    """Singleton Cloud Storage Client class (child of the original client)"""

    @run_once
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def upload_dict_to_gcs(
        self,
        bucket_name: str,
        file_name: str,
        data: dict,
        fields_names: Optional[list] = None,
        file_format: str = "CSV",
    ):
        """Load a list of dicts to Google Cloud Storage.

        Parameters
        ----------
        bucket_name : str
            Bucket name on GCS.
        file_name : str
            File name (blob).
        data : dict
            List of dicts to load on GCS.
        fields_names: Optional
            List with header fields names.
        file_format: str
            Output format on GCS (json/csv).

        Raises
        ----------
        ValueError
            Wrong file_format value.

        """
        if not isinstance(data, list) or not all(
            isinstance(item, dict) for item in data
        ):
            raise ValueError(
                "Parameter 'data' must be a list of dictionaries."
            )

        # Get bucket name
        bucket = self.bucket(bucket_name)

        # Create a new file
        blob = bucket.blob(file_name)

        if file_format.lower() == "json":
            blob.upload_from_string(
                data=json.dumps(data, indent=2),
                content_type="application/json",
            )
        elif file_format.lower() == "csv":
            output = StringIO()
            writer = csv.DictWriter(
                output, fieldnames=fields_names or data[0].keys()
            )
            writer.writeheader()
            writer.writerows(data)
            blob.upload_from_string(
                data=output.getvalue(),
                content_type="text/csv",
            )
        else:
            raise ValueError(f"Format '{ file_format }' non recognized!")
