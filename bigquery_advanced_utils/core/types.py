""" Module with all types."""

from typing import Literal

PartitionTimeGranularities = Literal["HOUR", "DAY", "MONTH", "YEAR"]
OutputFileFormat = Literal["CSV", "JSON", "AVRO"]
PermissionActionTypes = Literal["ADD", "REMOVE", "UPDATE"]
