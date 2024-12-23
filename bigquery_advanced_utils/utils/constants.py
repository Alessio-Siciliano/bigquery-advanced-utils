""" Module with the constants of the project. """

from typing import Literal
from google.cloud.bigquery.job import SourceFormat

# General constants
DEFAULT_LOG_LEVEL = "DEBUG"  # Default log level for the application

OUTPUT_FILE_FORMAT = {
    "CSV": SourceFormat.CSV,
    "JSON": SourceFormat.NEWLINE_DELIMITED_JSON,
    "AVRO": SourceFormat.AVRO,
}

# Regex patterns
MATCHING_PROJECT_LOCATION = r"projects\/([^\/]+)\/locations\/([^\/]+)"
MATCHING_RULE_TABLE_REF_ID = (
    r"projects\/([^\/]+)\/datasets\/([^\/]+)\/tables\/([^\/]+)"
)
MATCHING_RULE_TRANSFER_CONFIG_ID = (
    MATCHING_PROJECT_LOCATION + r"\/transferConfigs\/([^\/]+)"
)

# Literal
PartitionTimeGranularities = Literal["HOUR", "DAY", "MONTH", "YEAR"]
OutputFileFormat = Literal["CSV", "JSON", "AVRO"]
PermissionActionTypes = Literal["ADD", "REMOVE", "UPDATE"]

# Cloud Logging
FILTER_ACCESS_LOGS = """
    -protoPayload.methodName="jobservice.jobcompleted"
    -protoPayload.methodName="google.logging.v2.LoggingServiceV2.ReadLogEntriesLegacy"
    resource.type="bigquery_resource"           
    -protoPayload.serviceData.jobInsertRequest.resource.jobConfiguration.dryRun="true"
    -protoPayload.methodName="jobservice.getqueryresults"
    -protoPayload.methodName="tabledataservice.list"
    -protoPayload.methodName="google.logging.v2.LoggingServiceV2.AggregateLogs"
    -protoPayload.methodName="google.logging.v2.LoggingServiceV2.ListResourceKeys"
    -protoPayload.methodName="GetResourceBillingInfo"
    (
        -protoPayload.serviceData.jobInsertResponse.resource.jobName.jobId = ""
        OR (
            NOT protoPayload.serviceData.jobInsertResponse.resource.jobName.jobId:*
            and protoPayload.serviceData.jobQueryResponse.job.jobConfiguration.labels.requestor="looker_studio"
        )
        OR (
            NOT protoPayload.serviceData.jobInsertResponse.resource.jobName.jobId:*
            and protoPayload.serviceData.jobQueryResponse.job.jobName.jobId:*
        )
    )
    -- If LookerStudio removes the query that hit the cache
    (
        (
            protoPayload.serviceData.jobQueryResponse.job.jobConfiguration.labels.requestor="looker_studio"
            AND protoPayload.serviceData.jobQueryResponse.job.jobStatistics.referencedTables:*
        )
        OR (
            NOT protoPayload.serviceData.jobQueryResponse.job.jobConfiguration.labels.requestor="looker_studio"
        )
        OR (
            NOT protoPayload.serviceData.jobQueryResponse.job.jobConfiguration.labels.requestor:*
        )
    )

    -protoPayload.serviceData.jobInsertResponse.resource.jobConfiguration.query.statementType="SCRIPT"
"""
SOURCE_ORIGIN_TYPE = {
    "looker_studio": "Looker Studio",
    "datatransfer": "Datatransfer",
    "power_bi": "Power BI",
    "query_api": "Query / API",
    "other": "Other",
}
