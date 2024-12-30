""" Wrapper of original Logging module. """

import re
import logging
from typing import Tuple
from datetime import datetime, timedelta
from google.cloud.logging import Client, DESCENDING
from bigquery_advanced_utils.storage import CloudStorageClient
from bigquery_advanced_utils.core.constants import (
    MATCHING_RULE_TABLE_REF_ID,
    FILTER_ACCESS_LOGS,
    SOURCE_ORIGIN_TYPE,
)
from bigquery_advanced_utils.core.types import OutputFileFormat
from bigquery_advanced_utils.core.decorators import (
    run_once,
    singleton_instance,
)
from bigquery_advanced_utils.utils import datetime_utils


class LoggingClient(Client):
    """Singleton class to manage the logging client."""

    @run_once
    def __init__(self, *args, **kwargs):
        logging.debug("Init LoggingClient")
        super().__init__(*args, **kwargs)
        self.data_access_logs = []
        self.cache = {"cached": False, "start_time": None, "end_time": None}

    def _calculate_interval(
        self, *args, **kwargs
    ) -> Tuple[datetime, datetime]:
        """Calculate the time interval based on the provided arguments.

        Parameters
        ----------
        *args: int
            Number of days to look back.

        **kwargs: datetime
            Start date and end date.

        Returns
        -------
        Tuple[datetime, datetime]
            Start and end of the interval.

        Raises
        ------
        ValueError
            If the arguments are not valid.
        """
        # Check presence of keywords
        check_if_start_time = kwargs.get("start_time") is not None
        days = next(
            (item for item in args if isinstance(item, int)),
            kwargs.get("days", None),
        )

        if days is not None:
            start_time = datetime.now() - timedelta(days=days)
            end_time = datetime.now()
        elif check_if_start_time is True:
            start_time = datetime_utils.resolve_datetime(
                kwargs.get("start_time")
            )
            end_time = (
                datetime_utils.resolve_datetime(kwargs.get("end_time"))
                if kwargs.get("end_time") is not None
                else datetime.now()
            )
            if start_time > end_time:
                raise ValueError("Start time must be before end time.")
        else:
            raise ValueError("Invalid arguments, datetime range missing.")

        return start_time, end_time

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

        start_time, end_time = self._calculate_interval(*args, **kwargs)

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
                    item["resource"]
                    for item in dict_payload.get("authorizationInfo", [])
                    if "resource" in item
                    and "granted" in item
                    and item["granted"] is True
                    and re.match(MATCHING_RULE_TABLE_REF_ID, item["resource"])
                )
                tables = set(
                    f"{match[0][0]}.{match[0][1]}.{match[0][2]}"
                    for s in list_of_resources
                    if (
                        match := re.findall(
                            MATCHING_RULE_TABLE_REF_ID,
                            s,
                        )
                    )
                )
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

        self.cache["cached"] = True
        self.cache["start_time"] = start_time
        self.cache["end_time"] = end_time
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
        self, table_full_path: str, *args, **kwargs
    ) -> list[dict]:
        """Return all the access to a table.

        Parameters
        ----------
        table_full_path : str
            Project.Dataset.Table ID.

        *args:
            Positional arguments.

        **kwargs:
            Keywords arguments.

        Returns
        -------
        list[dict]
            List with the access events

        Raises
        ------
        ValueError
            If the table_full_path is not in the correct format.
        """
        start_time, end_time = self._calculate_interval(*args, **kwargs)

        if table_full_path.count(".") != 2:
            raise ValueError(
                "The first parameter must be in the format "
                "'project.dataset.table'."
            )

        # The selected interval must be a sub-set of the cached one
        if not self.cache.get("cached") or not (
            self.cache.get("start_time") is not None
            and self.cache.get("end_time") is not None
            and start_time >= self.cache.get("start_time")
            and end_time <= self.cache.get("end_time")
        ):
            self.get_all_data_access_logs(
                start_time=start_time, end_time=end_time
            )

        return [
            x
            for x in self.data_access_logs
            for table in x.get("referenced_tables", [])
            if table.lower() == table_full_path.lower()
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

        Raises
        ---------
        ValueError
            Call before the get_all_data_access_logs.
        """

        if not self.cache.get("cached"):
            raise ValueError(
                "No data in cache, you should get logs with the desidered "
                "time interval and then call this function."
            )

        # Flatten all dictionaries
        expanded_data = self._flatten_dictionaries()

        # Get keys
        all_keys = []
        for item in expanded_data:
            for key in item.keys():
                if key not in all_keys:  # pragma: no cover
                    all_keys.append(key)

        kwargs.get("CloudStorageClient_instance").upload_dict_to_gcs(
            bucket_name=bucket_name,
            file_name=file_name,
            data=expanded_data,
            fields_names=all_keys,
            file_format=file_format,
        )
