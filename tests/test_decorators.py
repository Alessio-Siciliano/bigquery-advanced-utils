import unittest
from unittest.mock import patch, MagicMock
from bigquery_advanced_utils.datatransfer import DataTransferClient

from bigquery_advanced_utils.utils.decorators import (
    ensure_bigquery_instance,
)


class MockClass:
    _bigquery_instance = MagicMock()

    @ensure_bigquery_instance
    def mock_method(self, bigquery_instance=None):
        pass


class TestEnsureBigQueryInstance(unittest.TestCase):

    def setUp(self):
        class MockClass:
            @ensure_bigquery_instance
            def mock_method(self, *args, **kwargs):
                return kwargs.get("bigquery_instance")

        self.mock_class = MockClass()

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances", {})
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient")
    def test_instance_created_when_not_exists(self, mock_bigquery_client):
        mock_instance = MagicMock()
        mock_bigquery_client.return_value = mock_instance

        result = self.mock_class.mock_method()

        mock_bigquery_client.assert_called_once()
        self.assertEqual(result, mock_instance)

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances")
    def test_existing_instance_used(self, mock_instances):
        mock_instance = MagicMock()
        mock_instances.__contains__.return_value = True
        mock_instances.__getitem__.return_value = mock_instance

        result = self.mock_class.mock_method()

        self.assertEqual(result, mock_instance)

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances", {})
    def test_no_instance_and_creation_fails(self):
        with patch(
            "bigquery_advanced_utils.bigquery.BigQueryClient",
            side_effect=Exception("Creation failed"),
        ):
            with self.assertRaises(Exception) as context:
                self.mock_class.mock_method()

            self.assertEqual(str(context.exception), "Creation failed")

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances")
    def test_logging_for_existing_instance(self, mock_instances):
        mock_instance = MagicMock()
        mock_instances.__contains__.return_value = True
        mock_instances.__getitem__.return_value = mock_instance

        with patch("logging.debug") as mock_logging:
            self.mock_class.mock_method()
            mock_logging.assert_called_with(
                "Using existing BigQuery instance."
            )

    @patch("logging.debug")  # Mock del logging.debug
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient")
    def test_logging_debug_called(self, MockBigQueryClient, mock_debug):
        mock_bigquery_instance = MagicMock()
        MockBigQueryClient.return_value = mock_bigquery_instance
        MockBigQueryClient._instances = {}

        @ensure_bigquery_instance
        def dummy_function(self, *args, **kwargs):
            return "Success"

        class TestClass:
            _bigquery_instance = None

        test_instance = TestClass()

        result = dummy_function(test_instance)

        mock_debug.assert_any_call("BigQuery instance created inside Storage.")
        self.assertEqual(result, "Success")

    @patch("logging.debug")  # Mock del logging.debug
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient")
    def test_logging_debug_called_v2(self, MockBigQueryClient, mock_debug):
        mock_bigquery_instance = MagicMock()
        MockBigQueryClient.return_value = mock_bigquery_instance
        MockBigQueryClient._instances = {}

        @ensure_bigquery_instance
        def dummy_function(self, *args, **kwargs):
            return "Success"

        class TestClass:
            _bigquery_instance = None

        test_instance = TestClass()

        result = dummy_function(test_instance)

        mock_debug.assert_any_call("BigQuery instance created inside Storage.")
        self.assertEqual(result, "Success")

    @patch("logging.debug")  # Mock del logger
    def test_logging_debug_message(self, mock_logging_debug):
        # Configura un'istanza della classe simulata
        obj = MockClass()

        # Richiama il metodo decorato
        obj.mock_method()

        # Verifica che il messaggio di debug sia stato registrato
        mock_logging_debug.assert_called_once_with(
            "BigQuery instance already exists, reusing the same instance."
        )


if __name__ == "__main__":
    unittest.main()
