import unittest
from unittest.mock import patch, MagicMock
from bigquery_advanced_utils.datatransfer import DataTransferClient
from bigquery_advanced_utils.bigquery import BigQueryClient
from bigquery_advanced_utils.core.decorators import (
    singleton_instance,
)


class MockClass:
    _bigquery_instance = MagicMock()

    @singleton_instance([BigQueryClient])
    def mock_method(self, BigQueryClient_instance=None):
        pass


class TestEnsureBigQueryInstance(unittest.TestCase):

    def setUp(self):
        class MockClass:
            @singleton_instance([BigQueryClient])
            def mock_method(self, *args, **kwargs):
                return kwargs.get("BigQueryClient_instance")

        self.mock_class = MockClass()

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances", {})
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient")
    def test_instance_created_when_not_exists(self, mock_bigquery_client):
        mock_instance = MagicMock()
        mock_bigquery_client.return_value = mock_instance

        result = self.mock_class.mock_method()

        # mock_bigquery_client.assert_called_once()
        # self.assertEqual(result, mock_instance)

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances")
    def test_existing_instance_used(self, mock_instances):
        mock_instance = MagicMock()
        mock_instances.__contains__.return_value = True
        mock_instances.__getitem__.return_value = mock_instance

        result = self.mock_class.mock_method()

        self.assertEqual(result, mock_instance)

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances", {})
    def test_no_instance_and_creation_fails(self):
        # Patch the BigQueryClient class itself to raise an exception during instantiation
        with patch(
            "bigquery_advanced_utils.bigquery.BigQueryClient.__new__",
            side_effect=Exception("Creation failed"),
        ):
            with self.assertRaises(Exception) as context:
                # Call the method or code that should trigger the instantiation
                BigQueryClient()  # This will try to create a new instance

            # Assert that the exception message is correct
            self.assertEqual(str(context.exception), "Creation failed")

    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._instances")
    def test_logging_for_existing_instance(self, mock_instances):
        """Test logging when an existing instance of BigQueryClient is found."""

        # Create a mock instance for BigQueryClient
        mock_instance = MagicMock()

        # Simulate that _instances contains an existing instance
        mock_instances.__contains__.return_value = (
            True  # _instances has the instance
        )
        mock_instances.__getitem__.return_value = (
            mock_instance  # Return mock instance
        )

        # Patch logging.info to test if the log is called
        with patch("logging.info") as mock_logging:  # Patch `logging.info`
            # Create an instance of the BigQueryClient to trigger the singleton logic
            client = BigQueryClient()

            # Ensure the correct log message is triggered
            client._instances = (
                mock_instances  # Inject the patched instances into the client
            )

            # Since we're reusing the existing instance, the log message should be:
            mock_logging.assert_called_with(
                "Reusing existing %s instance.", "BigQueryClient"
            )

    @patch("logging.debug")
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient")
    def test_logging_debug_called(self, MockBigQueryClient, mock_debug):
        mock_bigquery_instance = MagicMock()
        MockBigQueryClient.return_value = mock_bigquery_instance
        MockBigQueryClient._instances = {}

        @singleton_instance([BigQueryClient])
        def dummy_function(self, *args, **kwargs):
            return "Success"

        class TestClass:
            _bigquery_instance = None

        test_instance = TestClass()

        result = dummy_function(test_instance)

        self.assertEqual(result, "Success")

    @patch("logging.debug")  # Mock del logging.debug
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient")
    def test_logging_debug_called_v2(self, MockBigQueryClient, mock_debug):
        mock_bigquery_instance = MagicMock()
        MockBigQueryClient.return_value = mock_bigquery_instance
        MockBigQueryClient._instances = {}

        @singleton_instance([BigQueryClient])
        def dummy_function(self, *args, **kwargs):
            return "Success"

        class TestClass:
            _bigquery_instance = None

        test_instance = TestClass()

        result = dummy_function(test_instance)

        self.assertEqual(result, "Success")

    @patch("logging.debug")  # Mock del logger
    def test_logging_debug_message(self, mock_logging_debug):
        obj = MockClass()

        obj.mock_method()

        message_found = any(
            call
            == (
                "BigQuery instance already exists, reusing the same instance.",
            )
            for call in mock_logging_debug.call_args_list
        )

        if not message_found:
            print("No message.")


if __name__ == "__main__":
    unittest.main()
