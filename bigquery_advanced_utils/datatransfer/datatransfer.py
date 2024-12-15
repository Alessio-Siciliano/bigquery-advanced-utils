""" Module to extend the original DataTransferServiceClient. """

import re
from typing import Optional, Sequence, Tuple, Union
from google.cloud.bigquery_datatransfer import DataTransferServiceClient
from google.auth.credentials import Credentials
from google.cloud.bigquery_datatransfer_v1.types.datatransfer import (
    ListTransferConfigsRequest,
)
from google.api_core.retry import Retry
from google.api_core.gapic_v1.method import _MethodDefault

from bigquery_advanced_utils.utils.string import String
from bigquery_advanced_utils.bigquery.bigquery import BigQueryClient
from bigquery_advanced_utils.datatransfer.extended_transfer_config import (
    ExtendedTransferConfig,
)


class DataTransferClient(DataTransferServiceClient):
    """Custom class of DataTransferServiceClient"""

    # Matching rule for the format of a Scheduled Query ID.
    MATCHING_RULE_TRANSFER_CONFIG_ID = (
        ""
        r"projects\/[a-zA-Z0-9-]+\/locations\/[a-zA-Z-]+\/transferConfigs\/[a-zA-Z0-9-]+"  # pylint: disable=line-too-long
    )

    # Matching rule for the format of a parent string.
    MATCHING_RULE_PROJECT_LOCATION = (
        r"projects\/[a-zA-Z0-9-]+\/locations\/[a-zA-Z-]+"
    )

    def __init__(
        self,
        credentials: Credentials,
        project_id: str,
        location: str = "US",
        client_options: Optional[dict] = None,
    ) -> None:
        """Init of the class, same as parent

        Parameters
        ----------
        credentials : Credentials
            credentials of the current user

        location: str
            Location of the data. Default: US

        client_options: Optional[dict]
            custom client settings

        Returns
        -------
        None
        """
        super().__init__(
            credentials=credentials, client_options=client_options
        )
        self.location = location
        self.project_id = project_id
        self.string_utils = String()
        self.bigquery_client = BigQueryClient(
            credentials=credentials, project_id=project_id, location=location
        )
        self.cached_transfer_configs_list: list[ExtendedTransferConfig] = []

        self.parent = f"projects/{self.project_id}/locations/{self.location}"

    def get_transfer_configs(
        self,
        request: Optional[Union[ListTransferConfigsRequest, dict]] = None,
        *,
        # parent: Optional[str] = None,
        retry: Optional[Union[Retry, _MethodDefault, None]] = None,
        timeout: Optional[Union[float, object]] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        additional_configs: bool = False,
    ) -> list["ExtendedTransferConfig"]:
        """Get ALL schedule queries of the project.

        Parameters
        ----------
        request:
            A request to list data transfers configured for a BigQuery project.

        retry:
            Designation of what errors, if any, should be retried.

        timeout: float
            The timeout for this request.

        metadata: Sequence[Tuple[str, str]]
            Sequence of metadata as the original function.

        additional_configs: bool
            this field makes another request to get more informations.
            Default value is False to avoid useless requests.

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
        if (request is None or not request.parent) and not self.parent:
            raise ValueError("Request or parent parameters must be provided!")

        if (
            self.parent is not None
            and re.match(self.MATCHING_RULE_PROJECT_LOCATION, self.parent)
            is None
        ):
            raise ValueError(
                "Parent should be in the format projects/{}/locations/{}"
            )

        transfer_configs_request_response = super().list_transfer_configs(
            request=request,
            parent=self.parent,
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
                    transfer_config_email
                ).email

                # Care this can trigger an exception if fails
                simulated_attributes = self.bigquery_client.simulate_query(
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
            len(self.cached_transfer_configs_list) == 0
            or self.cached_transfer_configs_list[0].additional_configs == {}
        ):
            self.cached_transfer_configs_list = self.get_transfer_configs(
                additional_configs=True
            )

        return list(
            filter(
                lambda x: x.additional_configs.get("owner_email")
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
            len(self.cached_transfer_configs_list) == 0
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
                    for t in self.string_utils.extract_tables_from_query(
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

        Returns
        -------
        list[dict]
            List where each element is a dictionary of a single run.
        """

        response = self.list_transfer_runs(
            parent=self.parent + "/transferConfigs/" + transfer_config_id
        )

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
