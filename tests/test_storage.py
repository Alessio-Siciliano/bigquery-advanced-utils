from unittest.mock import MagicMock, patch
import unittest
from bigquery_advanced_utils.storage import CloudStorageClient


class TestCloudStorageClient(unittest.TestCase):
    @patch("bigquery_advanced_utils.storage.CloudStorageClient.bucket")
    def test_upload_dict_to_gcs_json(self, mock_bucket):
        # Arrange
        client = CloudStorageClient()
        mock_blob = MagicMock()  # Create a mock blob object
        mock_bucket.return_value.blob.return_value = (
            mock_blob  # Mock the bucket's blob method
        )

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
        mock_bucket.assert_called_once_with(
            bucket_name
        )  # Ensure the bucket method was called correctly
        mock_blob.upload_from_string.assert_called_once_with(  # Check that upload is called with correct data
            data="""[
  {
    "key1": "value1",
    "key2": "value2"
  }
]""",
            content_type="application/json",
        )

    @patch("bigquery_advanced_utils.storage.CloudStorageClient.bucket")
    def test_upload_dict_to_gcs_csv(self, mock_bucket):
        # Arrange
        client = CloudStorageClient()
        mock_blob = MagicMock()
        mock_bucket.return_value.blob.return_value = mock_blob

        bucket_name = "test-bucket"
        file_name = "test-file.csv"
        data = [
            {"key1": "value1", "key2": "value2"},
            {"key1": "value3", "key2": "value4"},
        ]

        # Act
        client.upload_dict_to_gcs(
            bucket_name=bucket_name,
            file_name=file_name,
            data=data,
            fields_names=["key1", "key2"],
            file_format="csv",
        )

        # Assert
        mock_bucket.assert_called_once_with(bucket_name)
        mock_blob.upload_from_string.assert_called_once()
        uploaded_data = mock_blob.upload_from_string.call_args[1]["data"]
        self.assertIn("key1,key2", uploaded_data)
        self.assertIn("value1,value2", uploaded_data)
        self.assertIn("value3,value4", uploaded_data)
        self.assertEqual(
            mock_blob.upload_from_string.call_args[1]["content_type"],
            "text/csv",
        )

    @patch("bigquery_advanced_utils.storage.CloudStorageClient.bucket")
    def test_upload_dict_to_gcs_invalid_format(self, mock_bucket):
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
