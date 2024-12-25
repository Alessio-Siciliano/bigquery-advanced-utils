import unittest
from unittest.mock import MagicMock
from typing import Any, Optional
from google.cloud.bigquery_datatransfer_v1.types.transfer import TransferConfig
from bigquery_advanced_utils.datatransfer import (
    ExtendedTransferConfig,
)


class TestExtendedTransferConfig(unittest.TestCase):
    def setUp(self):
        # Crea un'istanza mock di TransferConfig
        self.mock_transfer_config = MagicMock(spec=TransferConfig)
        self.additional_configs = {"processed_bytes": 123456, "cost": 50.5}

    def test_initialization(self):
        # Testa l'inizializzazione della classe
        config = ExtendedTransferConfig(
            transfer_config=self.mock_transfer_config,
            additional_configs=self.additional_configs,
        )

        self.assertEqual(config.base_config, self.mock_transfer_config)
        self.assertEqual(config.additional_configs, self.additional_configs)

    def test_initialization_with_defaults(self):
        # Testa l'inizializzazione con valori predefiniti
        config = ExtendedTransferConfig(
            transfer_config=self.mock_transfer_config
        )

        self.assertEqual(config.base_config, self.mock_transfer_config)
        self.assertEqual(config.additional_configs, {})

    def test_from_transfer_config(self):
        # Testa la creazione di un'istanza tramite il metodo di classe
        config = ExtendedTransferConfig.from_transfer_config(
            transfer_config=self.mock_transfer_config,
            additional_configs=self.additional_configs,
        )

        self.assertIsInstance(config, ExtendedTransferConfig)
        self.assertEqual(config.base_config, self.mock_transfer_config)
        self.assertEqual(config.additional_configs, self.additional_configs)

    def test_to_transfer_config(self):
        # Testa il metodo che restituisce il TransferConfig originale
        config = ExtendedTransferConfig(
            transfer_config=self.mock_transfer_config,
            additional_configs=self.additional_configs,
        )

        self.assertEqual(
            config.to_transfer_config(), self.mock_transfer_config
        )

    def test_repr(self):
        # Testa la rappresentazione della stringa dell'oggetto
        config = ExtendedTransferConfig(
            transfer_config=self.mock_transfer_config,
            additional_configs=self.additional_configs,
        )

        repr_str = repr(config)
        self.assertIn("ExtendedTransferConfig", repr_str)
        self.assertIn("base_config=", repr_str)
        self.assertIn("additional_configs=", repr_str)


if __name__ == "__main__":
    unittest.main()
