""" Module to extend the original DataTransferServiceClient. """

# import re

# from typing import Optional, Sequence, Tuple, Union
from google.cloud.bigquery_datatransfer import DataTransferServiceClient

# from google.cloud.bigquery_datatransfer_v1.types.datatransfer import (
#    ListTransferConfigsRequest,
# )
# from google.api_core.retry import Retry
# from google.api_core.gapic_v1.method import _MethodDefault

# from bigquery_advanced_utils.utils.string import String
# from bigquery_advanced_utils.bigquery.bigquery import BigQueryClient
from bigquery_advanced_utils.datatransfer.extended_transfer_config import (
    ExtendedTransferConfig,
)
from bigquery_advanced_utils.utils import SingletonBase
from bigquery_advanced_utils.utils.decorators import run_once

# from bigquery_advanced_utils.utils.constants import (
#    MATCHING_RULE_TRANSFER_CONFIG_ID,
# )


class DataTransferClient(DataTransferServiceClient, SingletonBase):
    """Custom class of DataTransferServiceClient"""

    @run_once
    def __init__(self, credentials, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cached_transfer_configs_list: list[ExtendedTransferConfig] = []
        self.credentials = credentials

    # def _initialize(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)

    # self.parent = f"projects/{self.project_id}/locations/{self.location}"
