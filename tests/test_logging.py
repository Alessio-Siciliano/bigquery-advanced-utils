import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone
from google.auth.credentials import (
    AnonymousCredentials,
)
from bigquery_advanced_utils.logging import LoggingClient


class TestLoggingClient(unittest.TestCase):

    def setUp(self):
        patch("google.cloud.bigquery.Client.__init__", lambda x: None).start()

        self.logging_client = LoggingClient()
        self.logging_client.project = "test_project"
        self.logging_client.cached = False
        self.mock_entries = [
            MagicMock(
                insert_id="1",
                timestamp=datetime.now(),
                payload={
                    "authenticationInfo": {
                        "principalEmail": "test@example.com"
                    },
                    "requestMetadata": {
                        "callerSuppliedUserAgent": "BigQuery Data Transfer Service"
                    },
                    "authorizationInfo": [
                        {
                            "resource": "projects/project-id/datasets/dataset-name/tables/table-name",
                            "granted": True,
                        }
                    ],
                    "serviceData": {
                        "jobQueryResponse": {
                            "job": {
                                "jobConfiguration": {
                                    "labels": {"requestor": "looker_studio"}
                                }
                            }
                        }
                    },
                },
            )
        ]

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_with_days(self, mock_list_entries):
        days = 10

        mock_list_entries.return_value = self.mock_entries

        logs = self.logging_client.get_all_data_access_logs(days)

        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["id"], "1")
        self.assertEqual(logs[0]["user_email"], "test@example.com")
        self.assertEqual(logs[0]["request_source_origin"], "Datatransfer")
        self.assertEqual(
            logs[0]["referenced_tables"],
            ["project-id.dataset-name.table-name"],
        )

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_without_referenced_tables(
        self, mock_list_entries
    ):
        days = 10

        mock_list_entries.return_value = [
            MagicMock(
                insert_id="1",
                timestamp=datetime.now(),
                payload={
                    "authenticationInfo": {
                        "principalEmail": "test@example.com"
                    },
                    "requestMetadata": {
                        "callerSuppliedUserAgent": "BigQuery Data Transfer Service"
                    },
                    "serviceData": {
                        "jobQueryResponse": {
                            "job": {
                                "jobConfiguration": {
                                    "labels": {"requestor": "looker_studio"}
                                }
                            }
                        }
                    },
                },
            )
        ]

        logs = self.logging_client.get_all_data_access_logs(days)

        self.assertEqual(len(logs), 0)

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_referenced_views(
        self, mock_list_entries
    ):
        days = 10

        mock_list_entries.return_value = [
            MagicMock(
                insert_id="1",
                timestamp=datetime.now(),
                payload={
                    "authenticationInfo": {
                        "principalEmail": "test@example.com"
                    },
                    "serviceData": {
                        "jobQueryResponse": {
                            "job": {
                                "jobConfiguration": {
                                    "labels": {"requestor": "looker_studio"}
                                },
                                "jobStatistics": {
                                    "referencedViews": [
                                        {
                                            "projectId": "project-id",
                                            "datasetId": "dataset-id",
                                            "tableId": "table-id",
                                        }
                                    ]
                                },
                            }
                        },
                    },
                },
            )
        ]

        logs = self.logging_client.get_all_data_access_logs(days)

        self.assertEqual(len(logs), 1)

    def test_start_time_greater_than_end_time(self):
        start_time = datetime.now() + timedelta(days=1)
        end_time = datetime.now()
        with self.assertRaises(ValueError) as context:
            self.logging_client._calculate_interval(
                start_time=start_time, end_time=end_time
            )
        self.assertEqual(
            str(context.exception), "Start time must be before end time."
        )

    def test_start_time_only(self):
        start_time = datetime.now() - timedelta(days=2)
        result = self.logging_client._calculate_interval(start_time=start_time)
        self.assertEqual(result[0], start_time)
        self.assertTrue(result[1] > start_time)

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_with_start_end(self, mock_list_entries):
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()

        mock_list_entries.return_value = self.mock_entries

        logs = self.logging_client.get_all_data_access_logs(
            start_time=start_date, end_time=end_date
        )
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["id"], "1")

    def test_get_all_data_access_logs_invalid_arguments(self):
        with self.assertRaises(ValueError):
            self.logging_client.get_all_data_access_logs(
                test=datetime.now(), end_date=datetime.now()
            )

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_no_logs(self, mock_list_entries):
        mock_list_entries.return_value = []
        logs = self.logging_client.get_all_data_access_logs(5)
        self.assertEqual(logs, [])

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_invalid_format(self, mock_list_entries):
        mock_list_entries.return_value = self.mock_entries
        with self.assertRaises(ValueError):
            self.logging_client.get_all_data_access_logs_by_table_id(
                "invalid_table_format"
            )

    def test_get_all_data_access_logs_invalid_format_raise(self):
        with self.assertRaises(ValueError):
            self.logging_client.get_all_data_access_logs_by_table_id(
                "invalid_table_format",
                start_time=datetime.now(),
                end_time=datetime.now(),
            )

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_exception(self, mock_list_entries):
        mock_list_entries.side_effect = Exception("Simulated error")

        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        with self.assertRaises(Exception) as context:
            self.logging_client.get_all_data_access_logs(
                start_time=start_time, end_time=end_time
            )

        self.assertIn("Simulated error", str(context.exception))

    @patch("bigquery_advanced_utils.storage.CloudStorageClient")
    @patch(
        "bigquery_advanced_utils.storage.CloudStorageClient.upload_dict_to_gcs"
    )
    def test_export_logs_to_storage(
        self, mock_upload_dict_to_gcs, MockCloudStorageClient
    ):
        mock_upload_dict_to_gcs.return_value = None
        MockCloudStorageClient.return_value.upload_dict_to_gcs = (
            mock_upload_dict_to_gcs
        )
        self.logging_client.cache = {
            "cached": True,
            "start_time": datetime.now() - timedelta(days=2),
            "end_time": datetime.now(),
        }
        self.logging_client.data_access_logs = self.mock_entries

        self.logging_client.export_logs_to_storage(
            bucket_name="test_bucket", file_name="test_file.csv"
        )
        mock_upload_dict_to_gcs.assert_called_once()

    def test_export_logs_no_cache(self):
        with self.assertRaises(ValueError) as context:
            self.logging_client.export_logs_to_storage(
                bucket_name="test-bucket",
                file_name="test-file.csv",
            )
        self.assertIn("No data in cache", str(context.exception))

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_by_table_id(self, mock_list_entries):
        mock_list_entries.return_value = [
            MagicMock(
                insert_id="1",
                timestamp=datetime.now(),
                payload={
                    "authenticationInfo": {
                        "principalEmail": "test@example.com"
                    },
                    "authorizationInfo": [
                        {
                            "resource": "projects/project/datasets/dataset/tables/table",
                            "granted": True,
                        }
                    ],
                    "requestMetadata": {
                        "callerSuppliedUserAgent": "BigQuery Data Transfer Service"
                    },
                    "serviceData": {
                        "jobQueryResponse": {
                            "job": {
                                "jobConfiguration": {
                                    "labels": {"requestor": "looker_studio"}
                                }
                            }
                        }
                    },
                },
            )
        ]

        logs = self.logging_client.get_all_data_access_logs_by_table_id(
            "project.dataset.table", days=2
        )

        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["id"], "1")

    @patch("google.cloud.logging.Client.list_entries")
    def test_get_all_data_access_logs_by_table_id_invalid_format(
        self, mock_list_entries
    ):
        with self.assertRaises(ValueError):
            self.logging_client.get_all_data_access_logs_by_table_id(
                "invalid_table_id"
            )

    def test_flatten_dictionaries(self):
        self.logging_client.data_access_logs = [
            {"a": 1, "b": {"x": 10, "y": 20}},
            {"c": 2, "d": [3, 4]},
        ]
        flattened = self.logging_client._flatten_dictionaries()
        self.assertEqual(flattened[0]["a"], 1)
        self.assertEqual(flattened[0]["b.x"], 10)
        self.assertEqual(len(flattened), 3)


if __name__ == "__main__":
    unittest.main()
