""" Wrapper of original Logging module. """

import re
import logging
from datetime import datetime, timedelta, timezone
from google.cloud.logging import Client, DESCENDING
from bigquery_advanced_utils.storage import CloudStorageClient
from bigquery_advanced_utils.core.constants import (
    MATCHING_RULE_TABLE_REF_ID,
    FILTER_ACCESS_LOGS,
    SOURCE_ORIGIN_TYPE,
    OutputFileFormat,
)
from bigquery_advanced_utils.core.decorators import (
    run_once,
    singleton_instance,
)


class LoggingClient(Client):
    """Singleton class to manage the logging client."""

    @run_once
    def __init__(self, *args, **kwargs):
        logging.debug("Init LoggingClient")
        super().__init__(*args, **kwargs)
        self.data_access_logs = []
        self.cached = False

    def get_all_data_access_logs(  # pylint: disable=too-many-locals
        self, *args, **kwargs
    ) -> list[dict]:
        """Get all data access logs

        Parameters
        ----------
        *args: int
            Numbers of days back to consider.

        **kwargs: datetime
            Start and end datetime.

        Returns
        -------
        List
            List of data access object.

        Raises
        ------
        ValueError
            If the arguments are not valid.

        Exception
            If an error occurs while getting logs.
        """

        if len(args) == 1 and not kwargs:
            days = args[0]
            start_time = datetime.now(timezone.utc) - timedelta(days=days)
            end_time = datetime.now(timezone.utc)
        elif len(kwargs) == 2 and not args:
            start_time = kwargs.get("start_date")
            end_time = kwargs.get("end_date")
            if start_time >= end_time:
                raise ValueError("Start date must be before end date.")
        elif len(kwargs) == 1 and not args and "start_date" in kwargs:
            start_time = kwargs.get("start_date")
            end_time = datetime.now(timezone.utc)
        else:
            raise ValueError("Invalid arguments.")

        filter_query = (
            FILTER_ACCESS_LOGS + " " + f'logName="projects/{self.project}/"'
            "logs/cloudaudit.googleapis.com%2Fdata_access"
        )

        # Time interval
        time_filter = (
            f'timestamp >= "{start_time.isoformat()}" '
            f'and timestamp <= "{end_time.isoformat()}"'
        )
        # Combine filter with time interval
        combined_filter = f"{filter_query} AND {time_filter}"

        # Get logs
        try:
            entries = self.list_entries(
                filter_=combined_filter, order_by=DESCENDING
            )
        except Exception as e:
            logging.error("Error getting logs: %s", e)
            raise

        # Loop each log
        for entry in entries:
            # Dict to store a single log data
            log_entry = {}

            # Get the payload of the log
            dict_payload = dict(entry.payload)

            # Log ID
            log_entry["id"] = entry.insert_id

            # Timestamp of the log
            log_entry["timestamp"] = entry.timestamp.isoformat()

            # User email
            log_entry["user_email"] = dict_payload.get(
                "authenticationInfo", {}
            ).get("principalEmail", "Unknown")

            # Request source origin
            log_entry["request_source_origin"] = (
                "Datatransfer"
                if dict_payload.get("requestMetadata", {}).get(
                    "callerSuppliedUserAgent"
                )
                == "BigQuery Data Transfer Service"
                else (
                    SOURCE_ORIGIN_TYPE.get("looker_studio")
                    if dict_payload.get("serviceData", {})
                    .get("jobQueryResponse", {})
                    .get("job", {})
                    .get("jobConfiguration", {})
                    .get("labels", {})
                    .get("requestor", {})
                    == "looker_studio"
                    else (
                        SOURCE_ORIGIN_TYPE.get("power_bi")
                        if dict_payload.get("requestMetadata", {})
                        .get("callerSuppliedUserAgent", "")
                        .startswith("MicrosoftODBCDriverforGoogleBigQuery")
                        else (
                            SOURCE_ORIGIN_TYPE.get("query_api")
                            if dict_payload.get("serviceData", {})
                            .get("jobInsertRequest", {})
                            .get("resource", {})
                            .get("jobConfiguration", {})
                            .get("query", {})
                            .get("queryPriority")
                            == "QUERY_INTERACTIVE"
                            else SOURCE_ORIGIN_TYPE.get("other")
                        )
                    )
                )
            )

            # Referenced tables
            if log_entry["request_source_origin"] not in (
                SOURCE_ORIGIN_TYPE.get("looker_studio"),
                SOURCE_ORIGIN_TYPE.get("power_bi"),
            ):
                list_of_resources = list(
                    set(
                        item["resource"]
                        for item in dict_payload.get("authorizationInfo", [])
                        if "resource" in item
                        and re.match(
                            MATCHING_RULE_TABLE_REF_ID, item["resource"]
                        )
                    )
                )

                tables = [
                    f"{match[0][0]}.{match[0][1]}.{match[0][2]}"
                    for s in list_of_resources
                    if (
                        match := re.findall(
                            MATCHING_RULE_TABLE_REF_ID,
                            s,
                        )
                    )
                ]
            else:
                tables = set(
                    f'{x.get("projectId")}.{x.get("datasetId")}'
                    f'.{x.get("tableId")}'
                    for x in dict_payload.get("serviceData", {})
                    .get("jobQueryResponse", {})
                    .get("job", {})
                    .get("jobStatistics", {})
                    .get("referencedTables", {})
                )
                views = set(
                    f'{x.get("projectId")}.{x.get("datasetId")}'
                    f'.{x.get("tableId")}'
                    for x in dict_payload.get("serviceData", {})
                    .get("jobQueryResponse", {})
                    .get("job", {})
                    .get("jobStatistics", {})
                    .get("referencedViews", {})
                )
                tables = tables.union(views)

            log_entry["referenced_tables"] = list(tables)

            # If no tables are found, skip log entry
            if len(log_entry["referenced_tables"]) == 0:
                continue

            log_entry["datatransfer_details"] = {
                "project_id": dict_payload.get("serviceData", {})
                .get("jobInsertResponse", {})
                .get("resource", {})
                .get("jobName", {})
                .get("projectId"),
                "config_id": dict_payload.get("serviceData", {})
                .get("jobInsertRequest", {})
                .get("resource", {})
                .get("jobConfiguration", {})
                .get("labels", {})
                .get("dts_run_id")
                or dict_payload.get("serviceData", {})
                .get("jobInsertResponse", {})
                .get("resource", {})
                .get("jobName", {})
                .get("jobId"),
            }

            log_entry["looker_studio_details"] = {
                "dashboard_id": dict_payload.get("serviceData", {})
                .get("jobQueryResponse", {})
                .get("job", {})
                .get("jobConfiguration", {})
                .get("labels", {})
                .get("looker_studio_report_id"),
                "datasource_id": dict_payload.get("serviceData", {})
                .get("jobQueryResponse", {})
                .get("job", {})
                .get("jobConfiguration", {})
                .get("labels", {})
                .get("looker_studio_datasource_id"),
            }

            # Save the log entry
            self.data_access_logs.append(log_entry)

        self.cached = True
        return self.data_access_logs

    def _flatten_dictionaries(self):
        def flatten_dictionary(  # pylint: disable=missing-return-doc
            dictionary, parent_key="", separator="."
        ) -> dict:
            flattened = {}
            for k, v in dictionary.items():
                new_key = f"{parent_key}{separator}{k}" if parent_key else k
                if isinstance(v, dict):
                    flattened.update(flatten_dictionary(v, new_key, separator))
                elif isinstance(v, list):
                    flattened[new_key] = v
                else:
                    flattened[new_key] = v

            return flattened

        expanded_data = []
        for item in self.data_access_logs:
            return_value = flatten_dictionary(item)
            has_list = any(
                isinstance(value, list) for value in return_value.values()
            )

            if has_list:
                for key, value in return_value.items():
                    if isinstance(value, list):
                        for item in value:
                            new_entry = {
                                k: v
                                for k, v in return_value.items()
                                if k != key
                            }
                            new_entry[key] = item
                            expanded_data.append(new_entry)
            else:
                expanded_data.append(return_value)
        return expanded_data

    def get_all_data_access_logs_by_table_id(
        self, table_full_path: str
    ) -> list[dict]:
        """Return all the access to a table.

        Parameters
        ----------
        table_full_path : str
            Project.Dataset.Table ID.

        Returns
        -------
        list[dict]
            List with the access events

        Raises
        ------
        ValueError
            If the table_full_path is not in the correct format.
        """
        if table_full_path.count(".") != 2:
            raise ValueError(
                "The first parameter must be in the format "
                "'project.dataset.table'."
            )

        if not self.cached:
            self.get_all_data_access_logs()

        return [
            x
            for x in self.data_access_logs
            if x.get("referenced_tables", "").lower()
            == table_full_path.lower()
        ]

    @singleton_instance([CloudStorageClient])
    def export_logs_to_storage(
        self,
        bucket_name: str,
        file_name: str,
        file_format: OutputFileFormat = "CSV",
        **kwargs,
    ) -> None:
        """Export the logs to a storage bucket.

        Parameters
        ----------
        bucket_name: str
            Path of GCS folder.

        file_name : str
            Output file name.

        file_format : OutputFileFormat, optional
            Output file format, by default "CSV".

        **kwargs:
            Keywords arguments.
        """

        if not self.cached:
            self.get_all_data_access_logs()

        # Flatten all dictionaries
        expanded_data = self._flatten_dictionaries()

        # Get keys
        all_keys = []
        for item in expanded_data:
            for key in item.keys():
                if key not in all_keys:
                    all_keys.append(key)

        kwargs.get("CloudStorageClient_instance").upload_dict_to_gcs(
            bucket_name=bucket_name,
            file_name=file_name,
            data=expanded_data,
            fields_names=all_keys,
            file_format=file_format,
        )
