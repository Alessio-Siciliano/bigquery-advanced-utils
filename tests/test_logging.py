import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
from bigquery_advanced_utils.logging import LoggingClient
from google.cloud.logging import Client


class TestLoggingClassSingleton(unittest.TestCase):

    @patch("google.cloud.logging.Client.__new__")
    @patch("google.cloud.logging.Client.__init__", autospec=True)
    def test_singleton_pattern(self, mock_init, mock_new):
        """Test that LoggingClient follows the singleton pattern."""

        instance = MagicMock(name="LoggingClient instance")
        mock_new.return_value = instance

        # Reset the instance for the test
        LoggingClient._instance = None

        instance1 = LoggingClient()
        instance2 = LoggingClient()

        self.assertIs(instance1, instance2)
        mock_new.assert_called_once()
        mock_init.assert_called_once_with(instance)

        # Reset the instance for the test
        LoggingClient._instance = None


class TestLoggingClient(unittest.TestCase):

    def setUp(self):
        self.client = LoggingClient()

    @patch("google.cloud.logging.Client.__new__")
    @patch("google.cloud.logging.Client.__init__", autospec=True)
    def test_initialization_error(self, mock_init, mock_new):
        """Test that initialization errors are handled correctly."""
        instance = MagicMock(name="LoggingClient instance")
        mock_new.return_value = instance

        # Simula un errore durante l'inizializzazione
        mock_init.side_effect = OSError("Initialization failed")

        # Reset the instance for the test
        LoggingClient._instance = None

        with self.assertRaises(RuntimeError) as context:
            LoggingClient()
        self.assertEqual(
            str(context.exception), "Failed to initialize LoggingClient"
        )
        mock_new.assert_called_once()
        mock_init.assert_called_once_with(instance)

        LoggingClient._instance = None


class TestGetAllAccessData(unittest.TestCase):

    def setUp(self):
        self.obj = LoggingClient(project="test_project")

    @patch.object(LoggingClient, "list_entries", return_value=[])
    @patch("logging.error")
    def test_single_positional_argument(
        self, mock_logging_error, mock_list_entries
    ):
        """Test with a single positional argument."""
        days = 5
        self.obj.get_all_data_access_logs(days)
        mock_list_entries.assert_called_once()

    @patch.object(LoggingClient, "list_entries", return_value=[])
    @patch("logging.error")
    def test_two_keyword_arguments(
        self, mock_logging_error, mock_list_entries
    ):
        """Test with two keyword arguments."""
        start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 2, tzinfo=timezone.utc)
        self.obj.get_all_data_access_logs(
            start_date=start_date, end_date=end_date
        )
        mock_list_entries.assert_called_once()

    @patch.object(LoggingClient, "list_entries", return_value=[])
    @patch("logging.error")
    def test_single_keyword_argument(
        self, mock_logging_error, mock_list_entries
    ):
        """Test with a single keyword argument."""
        start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        self.obj.get_all_data_access_logs(start_date=start_date)
        mock_list_entries.assert_called_once()

    def test_invalid_arguments(self):
        """Test with invalid arguments."""
        with self.assertRaises(ValueError):
            self.obj.get_all_data_access_logs(
                5, start_date=datetime.now(), end_date=datetime.now()
            )
        with self.assertRaises(ValueError):
            self.obj.get_all_data_access_logs()
        with self.assertRaises(ValueError):
            self.obj.get_all_data_access_logs(
                start_date=datetime.now(), end_date=datetime.now(), extra_arg=1
            )
        with self.assertRaises(ValueError):
            self.obj.get_all_data_access_logs(5, start_date=datetime.now())

    @patch.object(
        LoggingClient, "list_entries", side_effect=Exception("Test exception")
    )
    @patch("logging.error")
    def test_logging_error(self, mock_logging_error, mock_list_entries):
        """Test that logging captures errors."""
        with self.assertRaises(Exception):
            self.obj.get_all_data_access_logs(5)
        mock_logging_error.assert_called_once()
        args, _ = mock_logging_error.call_args
        self.assertEqual(args[0], "Error getting logs: %s")
        self.assertEqual(str(args[1]), "Test exception")

    def test_start_date_after_end_date(self):
        """Test that ValueError is raised when start_date is after end_date."""
        start_date = datetime(2022, 1, 2, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 1, tzinfo=timezone.utc)

        with self.assertRaises(ValueError) as context:
            self.obj.get_all_data_access_logs(
                start_date=start_date, end_date=end_date
            )
        self.assertEqual(
            str(context.exception), "Start date must be before end date."
        )


class TestGetAllDataAccessLogs(unittest.TestCase):

    def setUp(self):
        # Assumiamo che LoggingClient sia la tua classe che contiene la funzione get_all_data_access_logs
        self.client = LoggingClient(project="test_project")
        self.client.data_access_logs = []

    @patch.object(LoggingClient, "list_entries", return_value=[])
    @patch("logging.error")
    def test_single_positional_argument(
        self, mock_logging_error, mock_list_entries
    ):
        """Test with a single positional argument."""
        days = 5
        result = self.client.get_all_data_access_logs(days)
        # Verifica che list_entries sia stata chiamata
        mock_list_entries.assert_called_once()
        self.assertEqual(result, [])

    @patch.object(LoggingClient, "list_entries", return_value=[])
    @patch("logging.error")
    def test_two_keyword_arguments(
        self, mock_logging_error, mock_list_entries
    ):
        """Test with two keyword arguments."""
        start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 2, tzinfo=timezone.utc)
        result = self.client.get_all_data_access_logs(
            start_date=start_date, end_date=end_date
        )
        # Verifica che list_entries sia stata chiamata
        mock_list_entries.assert_called_once()
        self.assertEqual(result, [])

    @patch.object(LoggingClient, "list_entries", return_value=[])
    @patch("logging.error")
    def test_single_keyword_argument(
        self, mock_logging_error, mock_list_entries
    ):
        """Test with a single keyword argument."""
        start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        result = self.client.get_all_data_access_logs(start_date=start_date)
        # Verifica che list_entries sia stata chiamata
        mock_list_entries.assert_called_once()
        self.assertEqual(result, [])

    def test_invalid_arguments(self):
        """Test with invalid arguments."""
        with self.assertRaises(ValueError):
            self.client.get_all_data_access_logs(
                5, start_date=datetime.now(), end_date=datetime.now()
            )
        with self.assertRaises(ValueError):
            self.client.get_all_data_access_logs()
        with self.assertRaises(ValueError):
            self.client.get_all_data_access_logs(
                start_date=datetime.now(), end_date=datetime.now(), extra_arg=1
            )
        with self.assertRaises(ValueError):
            self.client.get_all_data_access_logs(5, start_date=datetime.now())

    def test_start_date_after_end_date(self):
        """Test that ValueError is raised when start_date is after end_date."""
        start_date = datetime(2022, 1, 2, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        with self.assertRaises(ValueError) as context:
            self.client.get_all_data_access_logs(
                start_date=start_date, end_date=end_date
            )
        self.assertEqual(
            str(context.exception), "Start date must be before end date."
        )

    @patch.object(
        LoggingClient, "list_entries", side_effect=Exception("Test exception")
    )
    @patch("logging.error")
    def test_logging_error(self, mock_logging_error, mock_list_entries):
        """Test that logging captures errors."""
        with self.assertRaises(Exception):
            self.client.get_all_data_access_logs(5)
        mock_logging_error.assert_called_once()
        # Verifica che il messaggio di errore loggato contenga il messaggio dell'eccezione
        args, _ = mock_logging_error.call_args
        self.assertEqual(args[0], "Error getting logs: %s")
        self.assertEqual(str(args[1]), "Test exception")

    @patch.object(LoggingClient, "list_entries")
    @patch("logging.error")
    def test_valid_data_returned(self, mock_logging_error, mock_list_entries):
        """Test that valid data is returned correctly."""
        # Finto log entry
        fake_entry = MagicMock()
        fake_entry.insert_id = "log123"
        fake_entry.timestamp = datetime(2022, 1, 1, tzinfo=timezone.utc)
        fake_entry.payload = {
            "authenticationInfo": {"principalEmail": "user@example.com"},
            "serviceData": {
                "jobQueryResponse": {
                    "job": {
                        "jobConfiguration": {
                            "labels": {"requestor": "looker_studio"}
                        },
                        "jobStatistics": {
                            "referencedTables": [
                                {
                                    "projectId": "proj",
                                    "datasetId": "dataset",
                                    "tableId": "table",
                                }
                            ]
                        },
                    },
                }
            },
        }

        mock_list_entries.return_value = [fake_entry]

        result = self.client.get_all_data_access_logs(5)
        # Verifica che list_entries sia stata chiamata
        mock_list_entries.assert_called_once()
        # Verifica che il risultato sia corretto
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "log123")
        self.assertEqual(result[0]["timestamp"], "2022-01-01T00:00:00+00:00")
        self.assertEqual(result[0]["user_email"], "user@example.com")

    @patch.object(LoggingClient, "list_entries")
    @patch("logging.error")
    def test_list_of_resources(self, mock_logging_error, mock_list_entries):
        """Test that list_of_resources is populated correctly."""
        # Finto log entry con authorizationInfo
        fake_entry = MagicMock()
        fake_entry.insert_id = "log123"
        fake_entry.timestamp = datetime(2022, 1, 1, tzinfo=timezone.utc)
        fake_entry.payload = {
            "authenticationInfo": {"principalEmail": "user@example.com"},
            "authorizationInfo": [
                {"resource": "projects/proj/datasets/dataset/tables/table1"},
                {"resource": "projects/proj/datasets/dataset/tables/table2"},
            ],
        }
        mock_list_entries.return_value = [fake_entry]
        result = self.client.get_all_data_access_logs(5)
        print("result", result)
        # Verifica che list_entries sia stata chiamata
        mock_list_entries.assert_called_once()
        # Verifica che il risultato sia corretto
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "log123")
        self.assertEqual(result[0]["timestamp"], "2022-01-01T00:00:00+00:00")
        self.assertEqual(result[0]["user_email"], "user@example.com")
        self.assertEqual(
            sorted(result[0]["referenced_tables"]),
            sorted(["proj.dataset.table1", "proj.dataset.table2"]),
        )

    @patch.object(LoggingClient, "list_entries")
    @patch("logging.error")
    def test_tables_populated_correctly(
        self, mock_logging_error, mock_list_entries
    ):
        """Test that the tables list is populated correctly."""
        # Finto log entry con authorizationInfo
        fake_entry = MagicMock()
        fake_entry.insert_id = "log124"
        fake_entry.timestamp = datetime(2022, 1, 1, tzinfo=timezone.utc)
        fake_entry.payload = {
            "authenticationInfo": {"principalEmail": "user@example.com"},
            "authorizationInfo": [
                {"resources": "projects/proj/datasets/dataset/tables/table1"}
            ],
        }

        mock_list_entries.return_value = [fake_entry]
        result = self.client.get_all_data_access_logs(5)
        print("result", result)
        # Verifica che list_entries sia stata chiamata
        mock_list_entries.assert_called_once()
        # Verifica che il risultato sia corretto
        self.assertEqual(len(result), 0)


class TestLoggingClientByTableID(unittest.TestCase):

    def setUp(self):
        self.client = LoggingClient(project="test_project")
        self.client.cached = False
        self.client.data_access_logs = []

    def test_invalid_table_full_path_format(self):
        """Test that ValueError is raised for invalid table_full_path format."""
        with self.assertRaises(ValueError) as context:
            self.client.get_all_data_access_logs_by_table_id("invalid_format")
        self.assertEqual(
            str(context.exception),
            "The first parameter must be in the format 'project.dataset.table'.",
        )

    @patch.object(LoggingClient, "get_all_data_access_logs")
    def test_get_all_data_access_logs_called_when_not_cached(
        self, mock_get_all_data_access_logs
    ):
        """Test that get_all_data_access_logs is called when not cached."""
        self.client.get_all_data_access_logs_by_table_id(
            "project.dataset.table"
        )
        mock_get_all_data_access_logs.assert_called_once()

    def test_valid_data_returned(self):
        """Test that valid data is returned correctly for a specific table ID."""
        self.client.cached = True
        self.client.data_access_logs = [
            {"referenced_tables": "project.dataset.table", "id": "log1"},
            {"referenced_tables": "project.dataset.other_table", "id": "log2"},
        ]
        result = self.client.get_all_data_access_logs_by_table_id(
            "project.dataset.table"
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "log1")

    def test_case_insensitivity(self):
        """Test that table_full_path matching is case-insensitive."""
        self.client.cached = True
        self.client.data_access_logs = [
            {"referenced_tables": "Project.Dataset.Table", "id": "log1"},
            {"referenced_tables": "project.dataset.other_table", "id": "log2"},
        ]
        result = self.client.get_all_data_access_logs_by_table_id(
            "project.dataset.table"
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "log1")


class TestFlattenDictionaries(unittest.TestCase):

    def setUp(self):
        self.client = LoggingClient()

    def test_flatten_simple_dict(self):
        self.client.data_access_logs = [{"key1": "value1", "key2": "value2"}]
        result = self.client._flatten_dictionaries()
        expected = [{"key1": "value1", "key2": "value2"}]
        self.assertEqual(result, expected)

    def test_flatten_nested_dict(self):
        self.client.data_access_logs = [
            {"key1": {"subkey1": "value1"}, "key2": "value2"}
        ]
        result = self.client._flatten_dictionaries()
        expected = [{"key1.subkey1": "value1", "key2": "value2"}]
        self.assertEqual(result, expected)

    def test_flatten_dict_with_list(self):
        self.client.data_access_logs = [
            {"key1": ["value1", "value2"], "key2": "value2"}
        ]
        result = self.client._flatten_dictionaries()
        expected = [
            {"key1": "value1", "key2": "value2"},
            {"key1": "value2", "key2": "value2"},
        ]
        self.assertEqual(result, expected)

    def test_flatten_nested_dict_with_list(self):
        self.client.data_access_logs = [
            {"key1": {"subkey1": ["value1", "value2"]}, "key2": "value2"}
        ]
        result = self.client._flatten_dictionaries()
        expected = [
            {"key1.subkey1": "value1", "key2": "value2"},
            {"key1.subkey1": "value2", "key2": "value2"},
        ]
        self.assertEqual(result, expected)

    def test_empty_list(self):
        self.client.data_access_logs = []
        result = self.client._flatten_dictionaries()
        expected = []
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
