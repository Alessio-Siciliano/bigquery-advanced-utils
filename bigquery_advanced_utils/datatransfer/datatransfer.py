""" Module to extend the original DataTransferServiceClient. """

import re
import logging

from typing import Optional, Sequence, Tuple, Union
from google.cloud.bigquery_datatransfer import DataTransferServiceClient

from google.cloud.bigquery_datatransfer_v1 import (
    ListTransferConfigsRequest,
)
from google.api_core.retry import Retry
from google.api_core.gapic_v1.method import _MethodDefault

from bigquery_advanced_utils.utils import string_utils
from bigquery_advanced_utils.datatransfer.extended_transfer_config import (
    ExtendedTransferConfig,
)
from bigquery_advanced_utils.core import SingletonBase
from bigquery_advanced_utils.core.decorators import (
    run_once,
    singleton_instance,
)
from bigquery_advanced_utils.core.constants import (
    MATCHING_RULE_PROJECT_LOCATION,
)

from bigquery_advanced_utils.bigquery import (  # pragma: no cover
    BigQueryClient,
)


class DataTransferClient(DataTransferServiceClient, SingletonBase):
    """Custom class of DataTransferServiceClient"""

    @run_once
    def __init__(self, *args, **kwargs):
        logging.debug("Init DataTransferClient")
        super().__init__(*args, **kwargs)
        self.cached_transfer_configs_list: list[ExtendedTransferConfig] = []

    @singleton_instance([BigQueryClient])
    def get_transfer_configs(
        self,
        request: Optional[Union[ListTransferConfigsRequest, dict]] = None,
        parent: Optional[str] = None,
        retry: Optional[Union[Retry, _MethodDefault, None]] = None,
        timeout: Optional[Union[float, object]] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        additional_configs: bool = False,
        **kwargs,
    ) -> list["ExtendedTransferConfig"]:
        """Get ALL schedule queries of the project.

        Parameters
        ----------
        request:
            A request to list data transfers configured for a BigQuery project.

        parent: Optional, str
            Required (if request is not provided).
            BigQuery project id for which transfer configs should be returned:
                projects/{project_id} or
                projects/{project_id}/locations/{location_id}.
            This corresponds to the parent field on the request instance;

        retry:
            Designation of what errors, if any, should be retried.

        timeout: float
            The timeout for this request.

        metadata: Sequence[Tuple[str, str]]
            Sequence of metadata as the original function.

        additional_configs: bool
            this field makes another request to get more informations.
            Default value is False to avoid useless requests.

        **kwargs: Optional
            List of instances.

        Returns
        -------
        List[ExtendedTransferConfig]
            Iterator of the ExtendedTransferConfig

        Raises
        -------
        ValueError
            if the value passed to the function are wrong
        """

        # If request is a dict(), convert to ListTransferConfigsRequest
        if isinstance(request, dict):
            request = ListTransferConfigsRequest(**request)

        # At least one between request and parent should be not empty
        if (request is None or not request.parent) and not parent:
            raise ValueError("Request or parent parameters must be provided!")

        if (
            parent is not None
            and re.match(MATCHING_RULE_PROJECT_LOCATION, parent) is None
        ):
            raise ValueError(
                "Parent should be in the format projects/{}/locations/{}"
            )

        transfer_configs_request_response = self.list_transfer_configs(
            request=request,
            parent=parent,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        # Clears all elements from the list
        self.cached_transfer_configs_list.clear()

        # For each TransferConfig we make a single request to get the email
        for transfer_config_original in transfer_configs_request_response:
            transfer_config = ExtendedTransferConfig(transfer_config_original)
            if additional_configs:
                transfer_config_email = self.get_transfer_config(
                    name=transfer_config.base_config.name
                ).owner_info
                transfer_config.additional_configs["owner_email"] = (
                    transfer_config_email.email
                )

                simulated_attributes = kwargs.get(
                    "BigQueryClient_instance"
                ).simulate_query(
                    transfer_config.base_config.params.get("query")
                )
                transfer_config.additional_configs[
                    "total_estimated_processed_bytes"
                ] = simulated_attributes.get("total_bytes_processed")
                transfer_config.additional_configs["referenced_tables"] = (
                    simulated_attributes.get("referenced_tables")
                )
            self.cached_transfer_configs_list.append(transfer_config)
            break
        return self.cached_transfer_configs_list

    def get_transfer_configs_by_owner_email(
        self, owner_email: str
    ) -> list[ExtendedTransferConfig]:
        """Get ALL schedule queries of a given user.

        Parameters
        ----------
        owner_email:
            Owner of the scheduled query.

        Returns
        -------
        list[ExtendedTransferConfig]
            List of all ExtendedTransferConfig object

        Raises
        -------
        ValueError
            if the value passed to the function are wrong

        """
        # If not cached, run it
        if (
            not self.cached_transfer_configs_list
            or self.cached_transfer_configs_list[0].additional_configs == {}
        ):
            self.cached_transfer_configs_list = self.get_transfer_configs(
                additional_configs=True
            )

        return list(
            filter(
                lambda x: x.additional_configs.get("owner_email").lower()
                == owner_email.lower(),
                self.cached_transfer_configs_list,
            )
        )

    def get_transfer_configs_by_table_id(
        self, table_id: str
    ) -> list[ExtendedTransferConfig]:
        """List transfer configs by table in the query

        Parameters
        ----------
        table_id:
            Name of the table (not needed entire path).

        Returns
        -------
        list[ExtendedTransferConfig]
            List of all TransferConfig object

        """
        # If not cached, run it
        if (
            not self.cached_transfer_configs_list
            or self.cached_transfer_configs_list[0].additional_configs == {}
        ):
            self.cached_transfer_configs_list = self.get_transfer_configs(
                additional_configs=True
            )

        return list(
            filter(
                lambda x: table_id.lower()
                in [
                    t.lower().split(".")[-1]
                    for t in string_utils.extract_tables_from_query(
                        x.base_config.params.get("query")
                    )
                ],
                self.cached_transfer_configs_list,
            )
        )

    def get_transfer_run_history(self, transfer_config_id: str) -> list[dict]:
        """Retrieve all the execution history of a transfer.

        Parameters
        ----------
        transfer_config_id : str
            Transfer config ID.
            In the format:
                projects/{}/locations/{}/transferConfig/{}

        Returns
        -------
        list[dict]
            List where each element is a dictionary of a single run.
        """

        response = self.list_transfer_runs(transfer_config_id)

        # Get the dictionary
        runs = []
        for run in response:
            runs.append(
                {
                    "run_time": run.schedule_time,
                    "start_time": run.start_time,
                    "end_time": run.end_time,
                    "state": run.state.name,
                    "error_message": (
                        run.error_status.message
                        if run.error_status.message
                        else None
                    ),
                }
            )

        return runs
