""" Module to manage all the functions regarding Bigquery. """

import csv
from typing import Optional, Union, Dict
from google.cloud.bigquery import Client
from google.cloud.bigquery.job import (
    QueryJobConfig,
    LoadJobConfig,
    SourceFormat,
)
from google.api_core.client_info import ClientInfo
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound, BadRequest
from google.auth.credentials import Credentials
import requests


class BigQueryClient(Client):
    """BigQuery Client class (child of the original client)"""

    def __init__(  # pylint: disable=useless-parent-delegation
        self,
        project_id: Optional[str] = None,
        credentials: Optional[Credentials] = None,
        _http: Optional[requests.Session] = None,
        location: Optional[str] = None,
        default_query_job_config: Optional[QueryJobConfig] = None,
        default_load_job_config: Optional[LoadJobConfig] = None,
        client_info: Optional[ClientInfo] = None,
        client_options: Optional[Union[ClientOptions, Dict]] = None,
    ) -> None:
        super().__init__(
            project=project_id,
            credentials=credentials,
            _http=_http,
            location=location,
            default_query_job_config=default_query_job_config,
            default_load_job_config=default_load_job_config,
            client_info=client_info,
            client_options=client_options,
        )

    def get_estimated_bytes_by_query(self, query: str) -> float:
        """Get billing GB for a given query

        Parameters
        ----------
        query:
            Query as string.

        Return
        -------
        Float
            Total processed byte

        """
        job_config = QueryJobConfig()
        job_config.dry_run = True
        job_config.use_query_cache = False
        query_job = self.query(query=query, job_config=job_config)
        return query_job.total_bytes_processed

    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a table exists in a given dataset

        Parameters
        ----------
        dataset_id:
            Name of the dataset.

        table_id:
            Name of the table.

        Returns
        -------
        Bool
            True if the table exists

        """
        try:
            self.get_table(f"{dataset_id}.{table_id}")
            return True
        except NotFound:
            return False

    def load_data_from_csv(  # pylint: disable=too-many-locals
        self,
        dataset_id: str,
        table_id: str,
        csv_file_path: str,
        test_functions: list | None = None,
        encoding: str = "UTF-8",
    ) -> None:
        """Load a CSV file to BigQuery with tests.

        Parameters
        ----------
        dataset_id:
            Name of the destination dataset.

        table_id:
            Name of the destination table.

        csv_file_path: str
            Path of the CSV file.

        test_functions: list
            List of checks that can be implemented.

        encoding: str
            Encoding of the file.
            DEFAULT: utf-8

        """
        with open(csv_file_path, "r", encoding=encoding) as file_text:
            reader = csv.reader(file_text)
            header = next(reader)  # Header (first row)

            # Run tests
            if test_functions:
                column_sums: list[set] = [set() for _ in header]
                for idx, row in enumerate(reader, start=1):
                    for test_function in test_functions:
                        test_function(
                            idx,
                            row,
                            header,
                            column_sums,
                        )

        # Open again the file with the binary to load in BigQuery
        with open(csv_file_path, "rb") as file_binary:
            job_config = LoadJobConfig(
                source_format=SourceFormat.CSV,
                autodetect=True,  # Autodetect schema
            )
            load_job = self.load_table_from_file(
                file_binary, f"{dataset_id}.{table_id}", job_config=job_config
            )

            try:
                load_job.result()  # Wait
                print(
                    f"Data loaded successfully into {dataset_id}.{table_id}."
                )
            except BadRequest as e:
                print(f"Error loading data: {e}")
