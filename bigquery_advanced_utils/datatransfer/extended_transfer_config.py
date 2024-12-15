""" Small class to extend to original one with more attributes. """

from typing import Optional, Any
from google.cloud.bigquery_datatransfer_v1.types.transfer import TransferConfig


class ExtendedTransferConfig:
    """Custom class of TransferConfig with more attributes."""

    def __init__(
        self,
        transfer_config: TransferConfig,
        additional_configs: Optional[dict[str, Any]] = None,
    ) -> None:
        """Init of the class,
        same as parent but with more attributes.

        Parameters
        ----------
        transfer_config: TransferConfig
            Original istance of TransferConfig.

        additional_configs : Optional[int]
            total processed bytes
            (cost of the query)

        """
        # Set of standards config from TransferConfig
        self.base_config = transfer_config
        # Additional informats made by this package
        self.additional_configs = additional_configs or {}

    @classmethod
    def from_transfer_config(
        cls,
        transfer_config: TransferConfig,
        additional_configs: Optional[dict[str, Any]] = None,
    ) -> "ExtendedTransferConfig":
        """Create an ExtendedTransferConfig instance from
        a standard TransferConfig.

        Parameters
        ----------
        transfer_config: TransferConfig
            Original istance of TransferConfig.

        additional_configs : Optional[int]
            total processed bytes
            (cost of the query)

        Returns
        -------
        ExtendedTransferConfig
            Instance with the wrapper.
        """
        # Start a new instance with copied params
        return cls(
            transfer_config,
            additional_configs=additional_configs,
        )

    def to_transfer_config(self) -> "TransferConfig":
        """Return the original TransferConfig instance

        Returns
        -------
        TransferConfig
            Original TransferConfig instance.
        """
        return self.base_config

    def __repr__(self) -> str:
        # Rende la rappresentazione dell'oggetto più "carina"
        base_config_str = repr(
            self.base_config
        )  # O un altro formato leggibile per base_config
        additional_configs_str = repr(
            self.additional_configs
        )  # Mostra i contenuti del dizionario
        return (
            f"ExtendedTransferConfig(base_config={base_config_str}, "
            f"additional_configs={additional_configs_str})"
        )
