""" Module to manage all the functions regarding Bigquery. """

import csv
import logging
from typing import Optional, Union, Dict, List, Literal, get_args, Any
from google.cloud.bigquery import (
    Client,
    AccessEntry,
)
from google.cloud.bigquery.job import (
    QueryJobConfig,
    LoadJobConfig,
    SourceFormat,
    ExtractJobConfig,
)
from google.api_core.client_info import ClientInfo
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound, BadRequest
from google.auth.exceptions import RefreshError
from google.auth.credentials import Credentials
from google.cloud.exceptions import GoogleCloudError
import requests
from bigquery_advanced_utils.utils.constants import (
    LOG_SUCCESS,
    LOG_FAILED,
    LOG_UNHANDLED_EXCEPTION,
    OutputFileFormat,
    PermissionActionTypes,
    IAM_TO_DATASET_ROLES,
    # PartitionTimeGranularities,
)


class BigQueryClient(Client):
    """BigQuery Client class (child of the original client)"""

    def __init__(  # pylint: disable=useless-parent-delegation
        self,
        project_id: str,
        credentials: Optional[Credentials] = None,
        _http: Optional[requests.Session] = None,
        location: Optional[str] = None,
        default_query_job_config: Optional[QueryJobConfig] = None,
        default_load_job_config: Optional[LoadJobConfig] = None,
        client_info: Optional[ClientInfo] = None,
        client_options: Optional[Union[ClientOptions, Dict]] = None,
    ) -> None:
        logging.debug("Starting the BigQueryClient")

        try:
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
            logging.debug("BigQueryClient successfully initialized.")
        except OSError as e:
            logging.error("BigQueryClient initialization error: %s", e)
            raise

    def _add_permission(
        self,
        is_table: bool,
        resource_id: str,
        user_email: str,
        role: str,
        bindings: list,
        entries: list,
    ) -> None:
        """Helper function to add permission."""
        if is_table:
            if not any(
                binding["role"] == role
                and f"user:{user_email}" in binding["members"]
                for binding in bindings
            ):
                bindings.append(
                    {"role": role, "members": {f"user:{user_email}"}}
                )
                logging.info(
                    "Permission '%s' added for '%s' on table '%s'.",
                    role,
                    user_email,
                    resource_id,
                )
        else:
            access_entry = AccessEntry(
                role=role,
                entity_type="userByEmail",
                entity_id=user_email,
            )
            if access_entry not in entries:
                entries.append(access_entry)
                logging.info(
                    "Permission '%s' added for '%s' on dataset '%s'.",
                    role,
                    user_email,
                    resource_id,
                )

    def _remove_permission(
        self,
        is_table: bool,
        resource_id: str,
        user_email: str,
        role: str,
        bindings: list,
        entries: list,
    ) -> None:
        """Helper function to remove permission."""
        if is_table:
            for binding in bindings:
                if (
                    f"user:{user_email}" in binding["members"]
                    and binding["role"] == role
                ):
                    binding["members"].remove(f"user:{user_email}")
                    logging.info(
                        "Permission '%s' removed for '%s' on table '%s'.",
                        role,
                        user_email,
                        resource_id,
                    )
        else:
            entries[:] = [
                entry
                for entry in entries
                if not (
                    entry.entity_type == "userByEmail"
                    and entry.entity_id == user_email
                    and entry.role == role
                )
            ]

            logging.info(
                "Permission '%s' removed for '%s' on dataset '%s'.",
                role,
                user_email,
                resource_id,
            )

    def _update_permission(
        self,
        is_table: bool,
        resource_id: str,
        user_email: str,
        role: str,
        bindings: list,
        entries: list,
    ) -> None:
        """Helper function to update permission."""
        if is_table:
            for binding in bindings:
                if f"user:{user_email}" in binding["members"]:
                    binding["members"].remove(f"user:{user_email}")
            bindings.append({"role": role, "members": {f"user:{user_email}"}})
            logging.info(
                "Permission updated to '%s' for '%s' on table '%s'.",
                role,
                user_email,
                resource_id,
            )
        else:
            entries[:] = [
                entry
                for entry in entries
                if entry.entity_type != "userByEmail"
                or entry.entity_id != user_email
            ]
            entries.append(
                AccessEntry(
                    role=role, entity_type="userByEmail", entity_id=user_email
                )
            )
            logging.info(
                "Permission updated to '%s' for '%s' on dataset '%s'.",
                role,
                user_email,
                resource_id,
            )

    def check_table_existence(self, dataset_id: str, table_id: str) -> bool:
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
        result = None
        logging.debug(
            "Starting table existence check: '%s.%s'", dataset_id, table_id
        )

        try:
            self.get_table(f"{dataset_id}.{table_id}")
            result = True
            return result
        except (NotFound, RefreshError) as e:
            result = False
            logging.error("Existence check exit with error %s", e)
            return result
        finally:
            status_message = (
                LOG_SUCCESS
                if result
                else (
                    LOG_FAILED
                    if result is not None
                    else LOG_UNHANDLED_EXCEPTION
                )
            )
            logging.debug(
                "Finished table existence check: '%s.%s' with status %s",
                dataset_id,
                table_id,
                status_message,
            )

    def load_data_from_csv(  # pylint: disable=too-many-locals
        self,
        dataset_id: str,
        table_id: str,
        csv_file_path: str,
        test_functions: Optional[list] = None,
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

        Raises
        -------
        BadRequest
            Request made with the wrong format.

        ValueError
            Error with values in the data.

        TypeError
            Parameter with the wrong datatype.

        Exception
            Unexpected error.
        """
        logging.debug("Starting data load from CSV: %s", csv_file_path)
        try:
            # Step 1: Validate and Test Data
            with open(csv_file_path, "r", encoding=encoding) as file_text:
                reader = csv.reader(file_text)
                header = next(reader)
                if not header:
                    raise ValueError("CSV file must have a header row.")

                logging.debug("CSV header: %s", header)

                if test_functions and len(test_functions) > 0:
                    logging.info("Running validation tests on CSV data.")
                    column_sums: list[set] = [set() for _ in header]
                    for idx, row in enumerate(reader, start=1):
                        # Run column-specific tests
                        for test_function in test_functions:
                            try:
                                test_function(idx, row, header, column_sums)
                            except (TypeError, ValueError) as e:
                                logging.error(
                                    "Validation failed for row %d: %s", idx, e
                                )
                                raise

        except (TypeError, ValueError) as e:
            logging.error("Error during CSV validation: %s", e)
            raise
        try:
            # Step 2: Load Data into BigQuery
            with open(csv_file_path, "rb") as file_binary:
                job_config = LoadJobConfig(
                    source_format=SourceFormat.CSV,
                    autodetect=True,
                )
                load_job = self.load_table_from_file(
                    file_binary,
                    f"{dataset_id}.{table_id}",
                    job_config=job_config,
                )
                logging.info(
                    "Starting BigQuery load job for table: '%s.%s'",
                    dataset_id,
                    table_id,
                )
                load_job.result()  # Wait for job to complete
                logging.info(
                    "Data successfully loaded into '%s.%s'",
                    dataset_id,
                    table_id,
                )

        except BadRequest as be:
            logging.error("BigQuery loading error: %s", be)
            raise
        except Exception as e:
            logging.error("Unexpected error: %s", e)
            raise
        finally:
            logging.debug(
                "Data load operation completed for: '%s.%s'",
                dataset_id,
                table_id,
            )

    def simulate_query(self, query: str) -> Dict[str, Any]:
        """Simulates the execution of the query
        and returns some important statistics.

        Parameters
        ----------
        query:
            Query as string.

        Return
        -------
        dict
            Dictionary with the simulation statistics including schema,
            query plan, and processed bytes.

        Raises
        ------
        Exception
            Unhanded error.
        """

        # Initialize the job configuration for a dry-run query
        job_config = QueryJobConfig(dry_run=True, use_query_cache=False)

        # Log the start of the simulation
        logging.debug("Starting query simulation for: %s", query)

        try:
            # Run the query in dry-run mode
            query_job = self.query(query, job_config=job_config)

            # Wait for the dry-run job to complete
            query_job.result()

            # Collect the relevant simulation statistics
            simulation_info = {
                "schema": query_job.schema,
                "referenced_tables": query_job.referenced_tables,
                "total_bytes_processed": query_job.total_bytes_processed,
            }

            # Log the success of the simulation
            logging.info("Query simulation completed successfully.")

            return simulation_info

        except Exception as e:
            # Log the error details
            logging.error("Error occurred during query simulation: %s", str(e))
            raise  # Re-raise the exception for higher-level handling

    def grant_permission(
        self,
        resource_id: str,
        user_permissions: List[Dict[str, str]],
        action: PermissionActionTypes,
    ) -> None:
        """Manages permissions (add, remove, or update)
        for multiple users on a specific dataset or table in BigQuery.

        Parameters
        ----------
        resource_id : str
            The path of the dataset or table:
            - For a dataset: "project_id.dataset_id".
            - For a table: "project_id.dataset_id.table_id".

        user_permissions : List[Dict[str, str]]
            A list of dictionaries, each containing:
            - 'user_email' (str): The email address of the user.
            - 'role' (str): The permission role (e.g., "OWNER", "READER").

        action : str
            (Not case-sensitive)
            The action to perform:
            - 'add': Add the specified permissions.
            - 'remove': Remove the specified permissions.
            - 'update': Update the specified permissions.

        Raises
        -------
        ValueError
            Error with values in the data.

        Exception
            Unexpected error.
        """
        logging.debug(
            "Starting permission management for resource '%s'.", resource_id
        )

        action_type = action.upper()

        # Validate action input
        if action_type not in get_args(PermissionActionTypes):
            logging.error(
                "Invalid action '%s'. Valid actions are %s.",
                action_type,
                ", ".join(get_args(PermissionActionTypes)),
            )
            raise ValueError(f"Invalid action: {action_type}")

        # Determine whether it's a table or dataset
        is_table = resource_id.count(".") == 2
        resource: dict = {}
        bindings = []
        entries = []

        if is_table:
            resource["table"] = self.get_table(resource_id)
            policy = self.get_iam_policy(resource["table"])
            bindings = policy.bindings
        else:
            resource["dataset"] = self.get_dataset(resource_id)
            entries = list(resource["dataset"].access_entries)

        # Process user permissions
        for user_permission in user_permissions:
            user_email = user_permission["user_email"]
            role = (
                IAM_TO_DATASET_ROLES[user_permission["role"]]
                if user_permission["role"] in IAM_TO_DATASET_ROLES
                else user_permission["role"]
            )

            try:
                if action_type == "ADD":
                    self._add_permission(
                        is_table,
                        resource_id,
                        user_email,
                        role,
                        bindings,
                        entries,
                    )
                elif action_type == "REMOVE":
                    self._remove_permission(
                        is_table,
                        resource_id,
                        user_email,
                        role,
                        bindings,
                        entries,
                    )
                elif action_type == "UPDATE":
                    self._update_permission(
                        is_table,
                        resource_id,
                        user_email,
                        role,
                        bindings,
                        entries,
                    )

            except Exception as e:
                logging.error(
                    "Error processing permission for user '%s' on '%s': %s",
                    user_email,
                    resource_id,
                    e,
                )
                raise

        # Update resource IAM policy
        if is_table:
            policy.bindings = bindings
            self.set_iam_policy(resource["table"], policy)
        else:
            resource["dataset"].access_entries = entries
            self.update_dataset(resource["dataset"], ["access_entries"])

        logging.info(
            "Permission management completed for resource '%s'.", resource_id
        )

    def export_table(
        self,
        dataset_id: str,
        table_id: str,
        destination: str,
        output_file_format: OutputFileFormat = "CSV",
        compression: Literal["GZIP", "DEFLATE", "SNAPPY", "NONE"] = "NONE",
    ) -> None:
        """Export a BigQuery table to CSV/JSON (local or Cloud Storage).

        Parameters
        ----------
        dataset_id: str
            Name of the dataset.

        table_id : str
            The name of the table.

        destination : str
            Exporting to Cloud Storage,
                provide the gs://bucket_name/path/to/file.

        output_file_format : OutputFileFormat, optional
            The format of the export: 'CSV', 'JSON', or 'AVRO'.
            Default is 'CSV'.

        compression : str, optional
            Compression to use for the export file. Can be one of:
            'GZIP', 'DEFLATE', 'SNAPPY', or 'NONE'.
            Default is 'NONE'.

        Raises
        ------
        GoogleCloudError
            Error related to GCP.

        Exception
            Unhanded error.
        """

        table_id = dataset_id + "." + table_id
        logging.debug(
            "Starting export of table '%s' to '%s'"
            " in format '%s' with compression '%s'.",
            table_id,
            destination,
            output_file_format,
            compression,
        )

        # Validate the export format
        valid_formats = {
            "CSV": SourceFormat.CSV,
            "JSON": SourceFormat.NEWLINE_DELIMITED_JSON,
            "AVRO": SourceFormat.AVRO,
        }

        if output_file_format not in valid_formats:
            logging.error("Unsupported format: %s", output_file_format)
            raise ValueError(f"Unsupported format: {output_file_format}")

        destination_format = valid_formats[output_file_format]

        # Set up the extract job configuration
        job_config = ExtractJobConfig(destination_format=destination_format)

        # Handle compression
        if compression != "NONE":
            job_config.compression = compression  # Set the compression type

        # Handle the export to Cloud Storage or local file system
        try:
            logging.info("Exporting to Cloud Storage: '%s'", destination)
            extract_job = self.extract_table(
                table_id, destination, job_config=job_config
            )

            # Wait for the job to complete and log the result
            extract_job.result()  # Waits for the job to complete
            logging.info("Table exported successfully to '%s'.", destination)

        except GoogleCloudError as e:
            logging.error("Google Cloud Error: %s", e)
            raise
        except Exception as e:
            logging.error("Unexpected error during table export: %s", e)
            raise
