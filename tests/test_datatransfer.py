import unittest
import logging
from unittest.mock import patch, MagicMock
from google.cloud.bigquery_datatransfer_v1 import ListTransferConfigsRequest
from bigquery_advanced_utils.datatransfer import (
    DataTransferClient,
    ExtendedTransferConfig,
)


class TestDataTransferClient(unittest.TestCase):
    def setUp(self):
        self.client = DataTransferClient()

    @patch("logging.debug")
    def test_init_logging(self, mock_logging_debug):
        logging.basicConfig(level=logging.DEBUG)

        # Creazione di una nuova istanza per assicurarsi che `__init__` venga chiamato
        client = object.__new__(DataTransferClient)
        client.__init__()

        # Verifica che logging.debug sia stato chiamato con il messaggio corretto
        mock_logging_debug.assert_called_once_with("Init DataTransferClient")

    @patch(
        "google.cloud.bigquery_datatransfer.DataTransferServiceClient.get_transfer_config"
    )
    @patch(
        "google.cloud.bigquery_datatransfer.DataTransferServiceClient.list_transfer_configs"
    )
    @patch(
        "bigquery_advanced_utils.datatransfer.extended_transfer_config.ExtendedTransferConfig"
    )
    @patch(
        "google.cloud.bigquery.Client.query"
    )  # Mock per la funzione esterna
    def test_get_transfer_configs(
        self,
        mock_query,
        mock_extended_config,
        mock_list_configs,
        mock_get_config,
    ):
        # Configurazione delle risposte simulate
        mock_transfer_config = (
            MagicMock()
        )  # Simula una risposta TransferConfig
        mock_transfer_config.name = "transfer-config-name"
        mock_transfer_config.params = {"query": "SELECT * FROM dataset.table"}
        mock_list_configs.return_value = [mock_transfer_config]

        mock_owner_info = MagicMock()
        mock_owner_info.email = "owner@example.com"
        mock_get_config.return_value.owner_info = mock_owner_info

        mock_extended_instance = MagicMock()
        mock_extended_config.return_value = mock_extended_instance

        # Configura il mock di `query`
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [
            {
                "total_bytes_processed": 12345,
                "referenced_tables": ["dataset.table"],
            }
        ]
        mock_query.return_value = mock_query_result

        # Esegui il metodo
        client = DataTransferClient()
        result = client.get_transfer_configs(
            parent="projects/project-id/locations/eu", additional_configs=True
        )

        # Verifica le chiamate
        mock_list_configs.assert_called_once_with(
            request=None,
            parent="projects/project-id/locations/eu",
            retry=None,
            timeout=None,
            metadata=(),
        )

        mock_get_config.assert_called_once_with(name="transfer-config-name")

        self.assertEqual(mock_query_result.result.call_count, 1)

    def test_get_transfer_configs_invalid_parent(self):
        with self.assertRaises(ValueError) as context:
            self.client.get_transfer_configs(parent="invalid_format")
        self.assertIn("Parent should be in the format", str(context.exception))

    @patch(
        "google.cloud.bigquery_datatransfer.DataTransferServiceClient.list_transfer_configs"
    )
    @patch("google.cloud.bigquery_datatransfer_v1.ListTransferConfigsRequest")
    def test_get_transfer_configs_request_as_dict(
        self, MockListTransferConfigsRequest, mock_list_transfer_configs
    ):
        request_dict = {"parent": "projects/test-project"}

        MockListTransferConfigsRequest.return_value = (
            ListTransferConfigsRequest(**request_dict)
        )

        mock_response = [MagicMock()]
        mock_list_transfer_configs.return_value = mock_response
        result = self.client.get_transfer_configs(request=request_dict)

        # MockListTransferConfigsRequest.assert_called_with(**request_dict)

        mock_list_transfer_configs.assert_called_once_with(
            request=MockListTransferConfigsRequest.return_value,
            parent=None,
            retry=None,
            timeout=None,
            metadata=(),
        )

        print(result, mock_response)
        self.assertEqual([result[0].base_config], mock_response)

    def test_get_transfer_configs_no_request_no_parent(self):
        with self.assertRaises(ValueError) as context:
            self.client.get_transfer_configs()
        self.assertIn(
            "Request or parent parameters must be provided!",
            str(context.exception),
        )

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_configs"
    )
    def test_get_transfer_configs_by_owner_email(
        self, mock_get_transfer_configs
    ):
        mock_config = MagicMock()
        mock_config.additional_configs = {"owner_email": "user@example.com"}
        mock_get_transfer_configs.return_value = [mock_config]

        result = self.client.get_transfer_configs_by_owner_email(
            "user@example.com"
        )

        # mock_get_transfer_configs.assert_called_once_with(
        #    additional_configs=True
        # )
        # self.assertEqual(len(result), 1)
        # self.assertEqual(result[0], mock_config)

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_configs"
    )
    @patch("bigquery_advanced_utils.utils.String.extract_tables_from_query")
    def test_get_transfer_configs_by_table_id(
        self, mock_extract_tables, mock_get_transfer_configs
    ):
        mock_config = MagicMock()
        mock_config.base_config.params = {"query": "SELECT * FROM table"}
        mock_get_transfer_configs.return_value = [mock_config]
        mock_extract_tables.return_value = ["dataset.table"]

        result = self.client.get_transfer_configs_by_table_id("table")

        # mock_get_transfer_configs.assert_called_once_with(
        #    additional_configs=True
        # )
        # mock_extract_tables.assert_called_once_with("SELECT * FROM table")
        # self.assertEqual(len(result), 1)
        # self.assertEqual(result[0], mock_config)

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_configs"
    )  # Mock del metodo
    @patch(
        "bigquery_advanced_utils.utils.String.extract_tables_from_query"
    )  # Mock della funzione di utilità
    def test_get_transfer_configs_called(
        self, mock_extract_tables, mock_get_transfer_configs
    ):
        # Mock per `String.extract_tables_from_query`
        mock_extract_tables.return_value = ["dataset.my_table"]

        # Simula il comportamento di `get_transfer_configs`
        mock_transfer_config = MagicMock()
        mock_transfer_config.base_config.params = {
            "query": "SELECT * FROM dataset.my_table"
        }
        mock_transfer_config.additional_configs = {"key": "value"}
        mock_get_transfer_configs.return_value = [mock_transfer_config]

        # Inizializza cached_transfer_configs_list a None
        self.client.cached_transfer_configs_list = None

        # Chiama il metodo da testare
        result = self.client.get_transfer_configs_by_table_id("my_table")

        # Verifica che `get_transfer_configs` sia stato chiamato con il parametro corretto
        mock_get_transfer_configs.assert_called_once_with(
            additional_configs=True
        )

        # Verifica che `cached_transfer_configs_list` sia stato aggiornato
        self.assertEqual(
            self.client.cached_transfer_configs_list, [mock_transfer_config]
        )

        # Verifica il risultato del filtro
        self.assertEqual(
            result, [mock_transfer_config]
        )  # Dovrebbe corrispondere al mock che abbiamo configurato

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.get_transfer_configs"
    )  # Mock del metodo
    def test_cached_transfer_configs_refresh(self, mock_get_transfer_configs):
        # Prepopola la cache con un valore
        mock_transfer_config = MagicMock()
        mock_transfer_config.additional_configs = {
            "owner_email": "user@example.com"
        }
        self.client.cached_transfer_configs_list = []

        # Chiama il metodo da testare
        result = self.client.get_transfer_configs_by_owner_email(
            "user@example.com"
        )

        mock_get_transfer_configs.assert_called()

        # Verifica il risultato del filtro
        # self.assertEqual(
        #    result, [mock_transfer_config]
        # )  # Dovrebbe corrispondere al mock configurato

    @patch(
        "bigquery_advanced_utils.datatransfer.DataTransferClient.list_transfer_runs"
    )
    def test_get_transfer_run_history(self, mock_list_transfer_runs):
        mock_run = MagicMock()
        mock_run.schedule_time = "2023-01-01T00:00:00Z"
        mock_run.start_time = "2023-01-01T00:01:00Z"
        mock_run.end_time = "2023-01-01T00:02:00Z"
        mock_run.state.name = "SUCCESS"
        mock_run.error_status.message = None
        mock_list_transfer_runs.return_value = [mock_run]

        result = self.client.get_transfer_run_history(
            "projects/project-id/locations/us/transferConfigs/config-id"
        )

        mock_list_transfer_runs.assert_called_once_with(
            "projects/project-id/locations/us/transferConfigs/config-id"
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["state"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
