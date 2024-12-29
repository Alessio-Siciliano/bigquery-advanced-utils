import unittest
from unittest.mock import MagicMock, patch
from google.auth.credentials import AnonymousCredentials
from bigquery_advanced_utils.storage import CloudStorageClient


class TestCloudStorageClient(unittest.TestCase):

    def setUp(self):
        # Mock google.auth.default to return fake credentials (AnonymousCredentials)
        self.patcher_auth = patch("google.auth.default")
        self.mock_auth = self.patcher_auth.start()
        self.mock_auth.return_value = (AnonymousCredentials(), "dummy-project")

        # Mock google.cloud.storage.Client to prevent actual initialization
        self.patcher_client = patch("google.cloud.storage.Client")
        self.mock_client = self.patcher_client.start()
        self.mock_client.return_value = MagicMock()

        # Mock the bucket method
        self.patcher_bucket = patch(
            "bigquery_advanced_utils.storage.CloudStorageClient.bucket"
        )
        self.mock_bucket = self.patcher_bucket.start()

    def tearDown(self):
        # Stop all patches after each test
        self.patcher_auth.stop()
        self.patcher_client.stop()
        self.patcher_bucket.stop()

    def test_upload_dict_to_gcs_json(self):
        # Arrange
        client = CloudStorageClient()
        mock_blob = MagicMock()
        self.mock_bucket.return_value.blob.return_value = mock_blob

        bucket_name = "test-bucket"
        file_name = "test-file.json"
        data = [{"key1": "value1", "key2": "value2"}]

        # Act
        client.upload_dict_to_gcs(
            bucket_name=bucket_name,
            file_name=file_name,
            data=data,
            file_format="json",
        )

        # Assert
        self.mock_bucket.assert_called_once_with(bucket_name)
        mock_blob.upload_from_string.assert_called_once_with(
            data="""[
  {
    "key1": "value1",
    "key2": "value2"
  }
]""",
            content_type="application/json",
        )

    def test_upload_dict_to_gcs_invalid_format(self):
        # Arrange
        client = CloudStorageClient()
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        data = [{"key1": "value1", "key2": "value2"}]

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            client.upload_dict_to_gcs(
                bucket_name=bucket_name,
                file_name=file_name,
                data=data,
                file_format="txt",
            )

        self.assertEqual(
            str(context.exception), "Format 'txt' non recognized!"
        )

    def test_upload_dict_to_gcs_invalid_data(self):
        # Arrange
        client = CloudStorageClient()
        bucket_name = "test-bucket"
        file_name = "test-file.json"
        data = "invalid_data"

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            client.upload_dict_to_gcs(
                bucket_name=bucket_name,
                file_name=file_name,
                data=data,
                file_format="json",
            )

        self.assertEqual(
            str(context.exception),
            "Parameter 'data' must be a list of dictionaries.",
        )


if __name__ == "__main__":
    unittest.main()
