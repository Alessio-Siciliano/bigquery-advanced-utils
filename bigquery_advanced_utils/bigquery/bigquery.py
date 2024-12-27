"""Module to manage all the functions regarding BigQuery.

This module provides a singleton BigQueryClient class that extends the 
google.cloud.bigquery.Client class. It ensures that only one instance of 
the BigQueryClient is created and reused throughout the application.

Usage:
    client = BigQueryClient()
    query_job = client.query("SELECT * FROM dataset.table")

    results = query_job.result()

    for row in results:
        print(row)
"""

import logging
from typing import (
    Optional,
    Dict,
    List,
    Literal,
    get_args,
    Any,
)
from google.cloud.bigquery.job import (
    QueryJobConfig,
    ExtractJobConfig,
)
from google.cloud.bigquery import AccessEntry
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.auth.exceptions import RefreshError
from google.cloud.exceptions import GoogleCloudError
from bigquery_advanced_utils.core.constants import (
    OUTPUT_FILE_FORMAT,
    OutputFileFormat,
    PermissionActionTypes,
)
from bigquery_advanced_utils.core import SingletonBase
from bigquery_advanced_utils.core.decorators import run_once


class BigQueryClient(bigquery.Client, SingletonBase):
    """Singleton BigQuery Client class (child of the original client)"""

    @run_once
    def __init__(self, *args, **kwargs):
        logging.debug("Init BigQueryClient")
        super().__init__(*args, **kwargs)

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
                logging.debug(
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
                logging.debug(
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
                    logging.debug(
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

            logging.debug(
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
            logging.debug(
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
            logging.debug(
                "Permission updated to '%s' for '%s' on dataset '%s'.",
                role,
                user_email,
                resource_id,
            )

    def manage_roles(
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

        Example
        -------
        grant_roles(
            "project.dataset.table",
            [
                {
                    "user_email": "my_email",
                    "role": "roles/bigquery.dataViewer",
                }
            ],
            "UPDATE",
        )
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
            role = user_permission["role"]

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

    def check_table_existence(self, *args, **kwargs) -> bool:
        """Check if a table exists in a given dataset.

        Parameters
        -------
        *args
            Positional arguments. The first argument can be the full path.

        **kwargs
            Keyword arguments. The following arguments are supported:
            - project_id: str
                The project ID.
            - dataset_id: str
                The dataset ID.
            - table_id: str
                The table ID.

        Returns
        -------
        bool
            True if the table exists.

        Raises
        ------
        ValueError
            If input values are invalid.
        """
        try:
            if len(args) == 1:
                # Provided full path to table. Check format
                full_path = args[0]
                if full_path.count(".") != 2:
                    raise ValueError(
                        "The first parameter must be in the format "
                        "'project.dataset.table'."
                    )
                project_id, dataset_id, table_id = full_path.split(".")
            elif len(args) == 2:
                # If there are two positional arguments, assign them to dataset and table
                dataset_id, table_id = args
                project_id = kwargs.get(
                    "project_id", self.project
                )  # If project is not provided, use default
            elif len(args) == 3:
                # If there are three positional arguments:
                # assign them to project, dataset, and table
                project_id, dataset_id, table_id = args
            elif not args:
                # If no args
                project_id = kwargs.get("project_id", self.project)
                dataset_id = kwargs.get("dataset_id")
                table_id = kwargs.get("table_id")
            else:
                raise ValueError(
                    "You must pass 2 positional arguments "
                    "or use keyword arguments."
                )

            # Ensure all required parameters are provided
            if not project_id or not dataset_id or not table_id:
                raise ValueError(
                    "You must provide project, dataset, and table."
                )
            # Log input values
            logging.debug(
                "Checking existence of table: '%s.%s.%s'",
                project_id,
                dataset_id,
                table_id,
            )

            # Attempt to retrieve the table
            self.get_table(f"{project_id}.{dataset_id}.{table_id}")
            logging.debug(
                "Table info: '%s.%s.%s'", project_id, dataset_id, table_id
            )
            return True

        except NotFound:
            logging.debug(
                "Table does not exist: '%s.%s.%s'",
                project_id,
                dataset_id,
                table_id,
            )
            return False

        except RefreshError as e:
            logging.error(
                "Failed to refresh credentials while checking "
                "table existence: %s",
                e,
            )
            return False

    def simulate_query(
        self,
        query: str,
        execution_project_id: Optional[str] = None,
        execution_location: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Simulates the execution of the query
        and returns some important statistics.

        Parameters
        ----------
        query: str
            Query as string.

        execution_project_id: Optional[str]
            Optional project in which execute the query.
            DEFAULT: client's project

        execution_location: Optional[str]
            Location where execute the query

        Return
        -------
        dict
            Dictionary with the simulation statistics including schema,
            query plan, and processed bytes.

        Raises
        ------
        Exception
            If an unhandled error occurs.
        """
        execution_project_id = execution_project_id or self.project
        execution_location = execution_location or self.location

        # Initialize the job configuration for a dry-run query
        job_config = QueryJobConfig(dry_run=True, use_query_cache=False)

        # Log the start of the simulation
        logging.debug(
            "Starting query simulation inside the project '%s' for: '%s'",
            execution_project_id,
            query,
        )

        try:
            # Run the query in dry-run mode
            query_job = self.query(
                query=query,
                job_config=job_config,
                project=execution_project_id,
                location=execution_location,
            )

            # Wait for the dry-run job to complete
            query_job.result()

            # Collect the relevant simulation statistics
            simulation_info = {
                "schema": query_job.schema,
                "referenced_tables": query_job.referenced_tables,
                "total_bytes_processed": query_job.total_bytes_processed,
            }

            # Log the success of the simulation
            logging.info(
                "Query simulation completed successfully "
                "inside the project '%s'.",
                execution_project_id,
            )

            return simulation_info

        except Exception as e:
            # Log the error details
            logging.error("Error occurred during query simulation: %s", str(e))
            raise  # Re-raise the exception for higher-level handling

    def export_data_to_storage(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        destination: str,
        output_file_format: OutputFileFormat = "CSV",
        compression: Literal["GZIP", "DEFLATE", "SNAPPY", "NONE"] = "NONE",
        execution_project_id: Optional[str] = None,
        execution_location: Optional[str] = None,
    ) -> None:
        """Export a BigQuery table to CSV/JSON.

        Parameters
        ----------
        project_id: str
            Name of the project.

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

        execution_project_id: str, optional
            Project where execute the job

        execution_location: str, optional
            Location where execute the job

        Raises
        ------
        GoogleCloudError
            Error related to GCP.

        Exception
            Unhanded error.
        """

        table_path = f"{project_id}.{dataset_id}.{table_id}"
        logging.debug(
            "Starting export of table '%s' to '%s'"
            " in format '%s' with compression '%s'.",
            table_path,
            destination,
            output_file_format,
            compression,
        )

        if output_file_format.upper() not in OUTPUT_FILE_FORMAT:
            logging.error("Unsupported format: %s", output_file_format)
            raise ValueError(f"Unsupported format: {output_file_format}")

        # Set up the extract job configuration
        job_config = ExtractJobConfig(
            destination_format=output_file_format.upper()
        )

        # Handle compression
        if compression != "NONE":
            job_config.compression = compression  # Set the compression type

        # Check if destination contains only a bucket or a bucket with a folder
        if destination.startswith("gs://"):
            # Remove the 'gs://' prefix
            destination_path = destination[5:]

            # Full path to new file in storage provided by user
            if "/" in destination and "." in destination.split("/")[-1]:
                destination = f"gs://{destination_path}"
                logging.info(
                    "Full path provided, creating file: '%s'",
                    destination,
                )
            else:
                # Check if the destination ends with a slash
                if destination_path.endswith("/"):
                    # If it's just a bucket with a trailing slash
                    # generate a file name
                    destination = (
                        f"gs://{destination_path}{table_id}."
                        f"{output_file_format.lower()}"
                    )
                    logging.info(
                        "Destination was just a bucket with slash,"
                        " creating file: '%s'",
                        destination,
                    )
                # elif "/" not in destination_path:
                else:
                    # If no folder or file is specified,
                    # generate a file name based on the table name
                    file_name = f"{table_id}.{output_file_format.lower()}"
                    destination = f"gs://{destination_path}/{file_name}"
                    logging.info(
                        "Destination was just a bucket name, "
                        "creating file: '%s'",
                        destination,
                    )

        # Handle the export to Cloud Storage
        try:
            logging.info("Exporting to Cloud Storage: '%s'", destination)
            extract_job = self.extract_table(
                source=table_path,
                destination_uris=destination,
                job_config=job_config,
                project=execution_project_id,
                location=execution_location,
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
