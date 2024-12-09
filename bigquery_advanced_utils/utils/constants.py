""" Module with the constants of the project. """

from typing import Literal
from google.cloud.bigquery.job import SourceFormat

# General constants
DEFAULT_LOG_LEVEL = "DEBUG"  # Default log level for the application

# Logging messages
LOG_SUCCESS = "SUCCESS"  # Log message for successful operations
LOG_FAILED = "FAILED"  # Log message for failed operations
LOG_UNHANDLED_EXCEPTION = (
    "UNHANDLED EXCEPTION"  # Log message for unhandled exceptions
)

# Bigquery
IAM_TO_DATASET_ROLES = {
    "roles/bigquery.dataViewer": "READER",
    "roles/bigquery.dataEditor": "WRITER",
    "roles/bigquery.dataOwner": "OWNER",
}
OUTPUT_FILE_FORMAT = {
    "CSV": SourceFormat.CSV,
    "JSON": SourceFormat.NEWLINE_DELIMITED_JSON,
    "AVRO": SourceFormat.AVRO,
}

# Literal
PartitionTimeGranularities = Literal["HOUR", "DAY", "MONTH", "YEAR"]
OutputFileFormat = Literal["CSV", "JSON", "AVRO"]
PermissionActionTypes = Literal["ADD", "REMOVE", "UPDATE"]
