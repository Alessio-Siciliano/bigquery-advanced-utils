import unittest
from unittest.mock import MagicMock, patch
from bigquery_advanced_utils.datatransfer import DataTransferClient
from google.cloud.bigquery_datatransfer_v1 import ListTransferConfigsRequest
from bigquery_advanced_utils.utils.decorators import (
    run_once,
    singleton_instance,
)
from bigquery_advanced_utils.utils import SingletonBase
from bigquery_advanced_utils.datatransfer.extended_transfer_config import (
    ExtendedTransferConfig,
)


class MockBigQueryClient(SingletonBase):
    def __init__(self):
        pass


class TestDataTransferClient(unittest.TestCase):
    @singleton_instance([MockBigQueryClient])
    def mock_singleton_method(self, MockBigQueryClient: MockBigQueryClient):
        # Check that we got the singleton instance of MockBigQueryClient
        return "Singleton Executed"

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.__init__",
        lambda x: None,
    )  # Mock init to avoid unnecessary API calls
    def test_init(self):
        """Test initialization of DataTransferClient."""
        client = DataTransferClient()
        client.cached_transfer_configs_list = []
        self.assertIsInstance(client, DataTransferClient)
        self.assertEqual(client.cached_transfer_configs_list, [])

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.list_transfer_configs"
    )
    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_config"
    )
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient.simulate_query")
    def test_get_transfer_configs(
        self,
        mock_simulate_query,
        mock_get_transfer_config,
        mock_list_transfer_configs,
    ):
        """Test get_transfer_configs method."""
        client = DataTransferClient()

        # Mock the response for list_transfer_configs
        mock_transfer_config = MagicMock()
        mock_transfer_config.base_config.name = "config_name"
        mock_list_transfer_configs.return_value = [mock_transfer_config]

        # Mock the response for get_transfer_config
        mock_owner_info = MagicMock()
        mock_owner_info.email = "owner@example.com"
        mock_get_transfer_config.return_value.owner_info = mock_owner_info

        # Mock the response for simulate_query
        mock_simulate_query.return_value = {
            "total_bytes_processed": 1000,
            "referenced_tables": ["table1", "table2"],
        }

        # Now call get_transfer_configs with additional_configs=True
        result = client.get_transfer_configs(
            parent="projects/project-id/locations/us", additional_configs=True
        )

        # Ensure additional_configs is populated correctly
        self.assertIsInstance(result[0], ExtendedTransferConfig)
        self.assertTrue("owner_email" in result[0].additional_configs)
        self.assertEqual(
            result[0].additional_configs["owner_email"], "owner@example.com"
        )
        self.assertEqual(
            result[0].additional_configs["total_estimated_processed_bytes"],
            1000,
        )
        self.assertEqual(
            result[0].additional_configs["referenced_tables"],
            ["table1", "table2"],
        )

    def test_get_transfer_configs_invalid(self):
        """Test get_transfer_configs with invalid parameters."""
        client = DataTransferClient()

        # Test ValueError when neither request nor parent is provided
        with self.assertRaises(ValueError):
            client.get_transfer_configs(parent=None)

        # Test ValueError when parent does not match expected format
        with self.assertRaises(ValueError):
            client.get_transfer_configs(parent="projects/project-id")

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.list_transfer_configs"
    )
    def test_get_transfer_configs_with_dict(self, mock_list_transfer_configs):
        # Crea una configurazione mock per ListTransferConfigsRequest
        request_dict = {"parent": "projects/my-project"}

        # Crea una risposta mock per list_transfer_configs
        mock_response = [
            {
                "transferConfigId": "config1",
                "query": "SELECT * FROM project.dataset.table",
            }
        ]
        mock_list_transfer_configs.return_value = mock_response

        # Crea l'oggetto client
        client = DataTransferClient()

        # Esegui la chiamata al metodo
        result = client.get_transfer_configs(request=request_dict)

        # Verifica che list_transfer_configs sia stato chiamato correttamente
        mock_list_transfer_configs.assert_called_once()

        # Verifica se il risultato è quello che ti aspetti
        self.assertEqual(len(result), 1)
        print(result)
        self.assertEqual(result[0].base_config["transferConfigId"], "config1")

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_configs"
    )
    def test_get_transfer_configs_called(self, mock_get_transfer_configs):
        # Configura il mock per get_transfer_configs
        mock_transfer_config = MagicMock(spec=ExtendedTransferConfig)
        mock_transfer_config.additional_configs = {
            "owner_email": "test@example.com"
        }
        mock_get_transfer_configs.return_value = [mock_transfer_config]

        # Inizializza il client e imposta la cache vuota
        client = DataTransferClient()
        client.cached_transfer_configs_list = []

        # Esegui il metodo
        result = client.get_transfer_configs_by_owner_email("test@example.com")

        # Verifica che get_transfer_configs sia stato chiamato con il parametro corretto
        mock_get_transfer_configs.assert_called_once_with(
            additional_configs=True
        )

        # Verifica il risultato
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], mock_transfer_config)

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_configs"
    )
    def test_get_transfer_configs_by_owner_email(
        self, mock_get_transfer_configs
    ):
        """Test get_transfer_configs_by_owner_email method."""
        client = DataTransferClient()

        # Mock the transfer configs list
        mock_transfer_config = MagicMock()
        mock_transfer_config.additional_configs = {
            "owner_email": "owner@example.com"
        }
        mock_get_transfer_configs.return_value = [mock_transfer_config]

        result = client.get_transfer_configs_by_owner_email(
            "owner@example.com"
        )

        # Check if it returns the correct config for the owner email
        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0].additional_configs["owner_email"], "owner@example.com"
        )

    @patch(
        "bigquery_advanced_utils.utils.string.String.extract_tables_from_query"
    )
    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_configs"
    )
    def test_if_branch_executed(
        self, mock_get_transfer_configs, mock_extract_tables_from_query
    ):
        # Configura il mock per get_transfer_configs
        mock_transfer_config = MagicMock()
        mock_transfer_config.base_config.params.get.return_value = (
            "SELECT * FROM dataset.table"
        )
        mock_transfer_config.additional_configs = {}
        mock_get_transfer_configs.return_value = [mock_transfer_config]

        # Configura il mock per extract_tables_from_query
        mock_extract_tables_from_query.return_value = ["dataset.table"]

        # Inizializza il client e imposta la cache vuota
        client = DataTransferClient()
        client.cached_transfer_configs_list = []

        # Esegui il metodo
        result = client.get_transfer_configs_by_table_id("table")

        # Verifica che get_transfer_configs sia stato chiamato
        mock_get_transfer_configs.assert_called_once_with(
            additional_configs=True
        )

        # Verifica il risultato
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], mock_transfer_config)

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.list_transfer_runs"
    )
    def test_get_transfer_run_history(self, mock_list_transfer_runs):
        """Test get_transfer_run_history method."""
        client = DataTransferClient()

        # Mock the response of list_transfer_runs
        mock_run = MagicMock()
        mock_run.schedule_time = "2023-01-01T00:00:00Z"
        mock_run.start_time = "2023-01-01T00:01:00Z"
        mock_run.end_time = "2023-01-01T00:02:00Z"
        mock_run.state.name = "SUCCESS"
        mock_run.error_status.message = None
        mock_list_transfer_runs.return_value = [mock_run]

        result = client.get_transfer_run_history(
            "projects/project-id/locations/us/transferConfigs/config-id"
        )

        # Test that the history is returned correctly
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["state"], "SUCCESS")
        self.assertEqual(result[0]["run_time"], "2023-01-01T00:00:00Z")

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.__init__",
        lambda x: None,
    )  # Mock init
    def test_run_once_decorator(self):
        """Test run_once decorator."""
        client = DataTransferClient()

        # Create a method with the run_once decorator
        @run_once
        def mock_method(self):
            return "Executed"

        # Test first call
        result = mock_method(client)
        # self.assertEqual(result, "Executed")

        # Test second call (should return None)
        result = mock_method(client)
        self.assertIsNone(result)

    def test_singleton_instance_decorator(self):
        """Test singleton instance decorator functionality."""

        # Mock the BigQuery client
        mock_bigquery_client = MagicMock(spec=MockBigQueryClient)

        # Simulate the method call
        result = self.mock_singleton_method()

        # Assert the result
        self.assertEqual(result, "Singleton Executed")
