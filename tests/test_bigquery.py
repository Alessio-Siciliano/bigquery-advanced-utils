""" Unit tests for bigquery_advanced_utils/bigquery.py """

import unittest
from unittest.mock import patch, MagicMock
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import NotFound
from google.auth.exceptions import RefreshError
from google.cloud.bigquery.job import QueryJobConfig
from google.cloud.bigquery import AccessEntry, Client
from bigquery_advanced_utils.bigquery import BigQueryClient


@patch("google.cloud.bigquery.Client.get_table")
class TestCheckTableExistence(unittest.TestCase):
    """Test check_table_existence method."""

    @classmethod
    def setUpClass(cls):
        cls.test_project = "test_project"
        cls.test_dataset = "test_dataset"
        cls.test_table = "test_table"
        cls.client = BigQueryClient()

    def test_handling_errors(self, mock_get_table):
        # Simulate various scenario
        error_scenarios = [
            (
                NotFound("Table not found."),
                False,
                (f"{self.test_project}.{self.test_dataset}.{self.test_table}"),
                True,
            ),
            (
                RefreshError("Credentials issue."),
                False,
                (f"{self.test_project}.{self.test_dataset}.{self.test_table}"),
                True,
            ),
            (
                ValueError("Invalid table path"),
                ValueError,
                (f"{self.test_project}.{self.test_dataset}.{self.test_table}"),
                True,
            ),
            (
                ValueError("The first parameter must be in the format"),
                ValueError,
                ("wrong.project"),
                False,
            ),
            (
                ValueError(
                    "You must pass 2 positional arguments or use keyword arguments."
                ),
                ValueError,
                ("wrong", "project", "dataset", "table"),
                False,
            ),
            (
                ValueError("You must provide project, dataset, and table."),
                ValueError,
                ("", "dataset_id", "table_id"),
                False,
            ),
        ]

        for error, expected_result, inputs, call_get_table in error_scenarios:
            if issubclass(type(error), Exception):
                mock_get_table.side_effect = error
            else:
                mock_get_table.return_value = MagicMock()
                mock_get_table.side_effect = None

            if isinstance(expected_result, type) and issubclass(
                expected_result, Exception
            ):
                with self.assertRaises(expected_result):
                    if isinstance(inputs, tuple):
                        self.client.check_table_existence(*inputs)
                    else:
                        self.client.check_table_existence(inputs)

            else:
                result = self.client.check_table_existence(inputs)
                self.assertEqual(result, expected_result)

            if call_get_table:
                mock_get_table.assert_called_once_with(inputs)

            # Reset the mock
            mock_get_table.reset_mock()

    def test_two_positional_arguments(self, mock_get_table):
        mock_get_table.return_value = MagicMock()

        # Mock project_id because it's dynamic
        self.client.project = "mock_project_id"

        result = self.client.check_table_existence(
            self.test_dataset, self.test_table
        )

        self.assertTrue(result)
        mock_get_table.assert_called_once_with(
            f"mock_project_id.{self.test_dataset}.{self.test_table}"
        )

    def test_three_positional_arguments(self, mock_get_table):
        mock_get_table.return_value = MagicMock()

        result = self.client.check_table_existence(
            self.test_project, self.test_dataset, self.test_table
        )

        self.assertTrue(result)
        mock_get_table.assert_called_once_with(
            f"{self.test_project}.{self.test_dataset}.{self.test_table}"
        )

    def test_zero_positional_arguments(self, mock_get_table):
        mock_get_table.return_value = MagicMock()

        self.client.project = "mock_project_id"

        result = self.client.check_table_existence(
            project_id="mock_project_id",
            dataset_id=self.test_dataset,
            table_id=self.test_table,
        )

        self.assertTrue(result)
        mock_get_table.assert_called_once_with(
            f"mock_project_id.{self.test_dataset}.{self.test_table}"
        )


@patch("google.cloud.bigquery.Client.query")
class TestSimulateQuery(unittest.TestCase):

    def setUp(self):
        self.client._location = "EU"
        return super().setUp()

    @classmethod
    def setUpClass(cls):
        cls.test_project = "test_project"
        cls.test_dataset = "test_dataset"
        cls.test_table = "test_table"
        cls.client = BigQueryClient()
        cls.query = "SELECT * FROM `test_table`"

    def test_simulate_query(self, mock_query):
        """Test that initialization errors are handled correctly."""
        mock_query.side_effect = Exception(
            "Error occurred during query simulation"
        )

        # Reset the instance for the test
        BigQueryClient._instance = None

        with self.assertRaises(Exception) as context:
            self.client.simulate_query(self.query)
        self.assertEqual(
            str(context.exception), "Error occurred during query simulation"
        )
        # Confronta gli attributi di QueryJobConfig invece degli oggetti stessi
        job_config = QueryJobConfig(dry_run=True, use_query_cache=False)
        called_args = mock_query.call_args[1]
        self.assertEqual(called_args["query"], self.query)
        self.assertEqual(called_args["job_config"].dry_run, job_config.dry_run)
        self.assertEqual(
            called_args["job_config"].use_query_cache,
            job_config.use_query_cache,
        )
        self.assertEqual(called_args["project"], self.client.project)
        self.assertEqual(called_args["location"], self.client.location)

        BigQueryClient._instance = None

    def test_query_job_result_success(self, mock_query):
        """Test that query_job.result() is called successfully."""
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = None
        mock_query_job.schema = "schema"
        mock_query_job.referenced_tables = ["table1", "table2"]
        mock_query_job.total_bytes_processed = 12345
        mock_query.return_value = mock_query_job

        result = self.client.simulate_query(self.query)
        mock_query_job.result.assert_called_once()

        self.assertEqual(
            result,
            {
                "schema": mock_query_job.schema,
                "referenced_tables": mock_query_job.referenced_tables,
                "total_bytes_processed": mock_query_job.total_bytes_processed,
            },
        )

        called_args = mock_query.call_args[1]
        self.assertEqual(called_args["query"], self.query)
        self.assertTrue(isinstance(called_args["job_config"], QueryJobConfig))
        self.assertEqual(called_args["job_config"].dry_run, True)
        self.assertEqual(called_args["job_config"].use_query_cache, False)
        self.assertEqual(called_args["project"], self.client.project)
        self.assertEqual(called_args["location"], self.client.location)

    def test_query_job_result_error(self, mock_query):
        """Test that an error in query_job.result() is handled correctly."""
        mock_query_job = MagicMock()
        mock_query_job.result.side_effect = Exception("Query job failed")
        mock_query.return_value = mock_query_job

        with self.assertRaises(Exception) as context:
            self.client.simulate_query(self.query)

        self.assertEqual(str(context.exception), "Query job failed")
        mock_query_job.result.assert_called_once()

        called_args = mock_query.call_args[1]
        self.assertEqual(called_args["query"], self.query)
        self.assertTrue(isinstance(called_args["job_config"], QueryJobConfig))
        self.assertEqual(called_args["job_config"].dry_run, True)
        self.assertEqual(called_args["job_config"].use_query_cache, False)
        self.assertEqual(called_args["project"], self.client.project)
        self.assertEqual(called_args["location"], self.client.location)


class TestAddPermission(unittest.TestCase):

    def setUp(self):
        # Mock del costruttore __init__ di BigQueryClient per evitare il caricamento delle credenziali
        patch("google.cloud.bigquery.Client.__init__", lambda x: None).start()

        # Creazione di una versione mockata del Client
        self.client = BigQueryClient()
        self.bindings = []
        self.entries = []

    def tearDown(self):
        # Interrompi il patching dopo il test
        patch.stopall()

    def test_add_table_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset.table"

        self.client._add_permission(
            is_table=True,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        expected_binding = {"role": role, "members": {f"user:{user_email}"}}
        self.assertIn(expected_binding, self.bindings)
        self.assertEqual(len(self.entries), 0)

    def test_add_dataset_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset"

        self.client._add_permission(
            is_table=False,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        expected_entry = AccessEntry(
            role=role,
            entity_type="userByEmail",
            entity_id=user_email,
        )
        self.assertIn(expected_entry, self.entries)
        self.assertEqual(len(self.bindings), 0)

    def test_add_existing_table_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset.table"
        self.bindings.append({"role": role, "members": {f"user:{user_email}"}})

        self.client._add_permission(
            is_table=True,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        self.assertEqual(len(self.bindings), 1)
        self.assertEqual(len(self.entries), 0)

    def test_add_existing_dataset_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset"
        existing_entry = AccessEntry(
            role=role,
            entity_type="userByEmail",
            entity_id=user_email,
        )
        self.entries.append(existing_entry)

        self.client._add_permission(
            is_table=False,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        self.assertEqual(len(self.entries), 1)
        self.assertEqual(len(self.bindings), 0)


class TestRemovePermission(unittest.TestCase):

    def setUp(self):
        # Mock del costruttore __init__ di BigQueryClient per evitare il caricamento delle credenziali
        patch("google.cloud.bigquery.Client.__init__", lambda x: None).start()

        # Creazione di una versione mockata del Client
        self.client = BigQueryClient()
        self.bindings = []
        self.entries = []

    def tearDown(self):
        # Interrompi il patching dopo il test
        patch.stopall()

    def test_remove_table_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset.table"
        self.bindings = [{"role": role, "members": {f"user:{user_email}"}}]

        self.client._remove_permission(
            is_table=True,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        expected_binding = {"role": role, "members": set()}
        self.assertIn(expected_binding, self.bindings)
        self.assertEqual(len(self.entries), 0)

    def test_remove_dataset_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset"
        existing_entry = AccessEntry(
            role=role,
            entity_type="userByEmail",
            entity_id=user_email,
        )
        self.entries.append(existing_entry)

        self.client._remove_permission(
            is_table=False,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        self.assertNotIn(existing_entry, self.entries)
        self.assertEqual(len(self.bindings), 0)

    def test_remove_nonexistent_table_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset.table"
        self.bindings = [{"role": role, "members": set()}]

        self.client._remove_permission(
            is_table=True,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        expected_binding = {"role": role, "members": set()}
        self.assertIn(expected_binding, self.bindings)
        self.assertEqual(len(self.entries), 0)

    def test_remove_nonexistent_dataset_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset"
        existing_entry = AccessEntry(
            role=role,
            entity_type="userByEmail",
            entity_id="another_user@example.com",
        )
        self.entries.append(existing_entry)

        self.client._remove_permission(
            is_table=False,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        self.assertIn(existing_entry, self.entries)
        self.assertEqual(len(self.entries), 1)
        self.assertEqual(len(self.bindings), 0)


class TestUpdatePermission(unittest.TestCase):

    def setUp(self):
        # Mock del costruttore __init__ di BigQueryClient per evitare il caricamento delle credenziali
        patch("google.cloud.bigquery.Client.__init__", lambda x: None).start()

        # Creazione di una versione mockata del Client
        self.client = BigQueryClient()
        self.bindings = []
        self.entries = []

    def tearDown(self):
        # Interrompi il patching dopo il test
        patch.stopall()

    def test_update_table_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset.table"
        self.bindings = [
            {"role": "roles/bigquery.admin", "members": {f"user:{user_email}"}}
        ]

        self.client._update_permission(
            is_table=True,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        expected_binding = {"role": role, "members": {f"user:{user_email}"}}
        self.assertIn(expected_binding, self.bindings)
        self.assertEqual(len(self.bindings), 2)
        self.assertEqual(len(self.entries), 0)

    def test_update_dataset_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset"
        existing_entry = AccessEntry(
            role="roles/bigquery.admin",
            entity_type="userByEmail",
            entity_id=user_email,
        )
        self.entries.append(existing_entry)

        self.client._update_permission(
            is_table=False,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        new_entry = AccessEntry(
            role=role,
            entity_type="userByEmail",
            entity_id=user_email,
        )
        self.assertIn(new_entry, self.entries)
        self.assertEqual(len(self.entries), 1)
        self.assertEqual(len(self.bindings), 0)

    def test_update_nonexistent_table_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset.table"

        self.client._update_permission(
            is_table=True,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        expected_binding = {"role": role, "members": {f"user:{user_email}"}}
        self.assertIn(expected_binding, self.bindings)
        self.assertEqual(len(self.entries), 0)

    def test_update_nonexistent_dataset_permission(self):
        user_email = "test@example.com"
        role = "roles/bigquery.dataViewer"
        resource_id = "project.dataset"

        self.client._update_permission(
            is_table=False,
            resource_id=resource_id,
            user_email=user_email,
            role=role,
            bindings=self.bindings,
            entries=self.entries,
        )

        new_entry = AccessEntry(
            role=role,
            entity_type="userByEmail",
            entity_id=user_email,
        )
        self.assertIn(new_entry, self.entries)
        self.assertEqual(len(self.entries), 1)
        self.assertEqual(len(self.bindings), 0)


class MockBindingsList(list):
    def append(self, item):
        print(f"Mock append called with item={item}")
        super().append(item)


class TestManageRoles(unittest.TestCase):
    def setUp(self):
        # Mock del costruttore __init__ di BigQueryClient per evitare il caricamento delle credenziali
        patch("google.cloud.bigquery.Client.__init__", lambda x: None).start()

        # Mock dei metodi di BigQueryClient
        self.mock_get_table = patch(
            "google.cloud.bigquery.Client.get_table"
        ).start()
        self.mock_get_iam_policy = patch(
            "google.cloud.bigquery.Client.get_iam_policy"
        ).start()
        self.mock_set_iam_policy = patch(
            "google.cloud.bigquery.Client.set_iam_policy"
        ).start()

        # Creazione di una versione mockata del Client
        self.client = BigQueryClient()
        self.mock_table = MagicMock()
        self.mock_policy = MagicMock()
        self.mock_policy.bindings = MockBindingsList()

        # Mock dei ritorni dei metodi
        self.mock_get_table.return_value = self.mock_table
        self.mock_get_iam_policy.return_value = self.mock_policy

    def tearDown(self):
        # Interrompi il patching dopo il test
        patch.stopall()

    def test_add_permission_to_table(self):
        resource_id = "project.dataset.table"
        user_permissions = [
            {
                "user_email": "test@example.com",
                "role": "roles/bigquery.dataViewer",
            }
        ]
        action = "ADD"

        self.client.manage_roles(resource_id, user_permissions, action)
        expected_binding = {
            "role": "roles/bigquery.dataViewer",
            "members": {"user:test@example.com"},
        }

        # Verifica che append sia stato chiamato con expected_binding
        self.assertIn(expected_binding, self.mock_policy.bindings)

        # Verifica che set_iam_policy sia stato chiamato
        self.mock_set_iam_policy.assert_called_once_with(
            self.mock_table, self.mock_policy
        )

    @patch("google.cloud.bigquery.Client.get_dataset")
    @patch("google.cloud.bigquery.Client.update_dataset")
    def test_remove_permission_from_dataset(
        self, mock_update_dataset, mock_get_dataset
    ):
        mock_dataset = MagicMock()
        mock_dataset.access_entries = [
            AccessEntry(
                role="roles/bigquery.dataViewer",
                entity_type="userByEmail",
                entity_id="test@example.com",
            )
        ]

        mock_get_dataset.return_value = mock_dataset

        resource_id = "project.dataset"
        user_permissions = [
            {
                "user_email": "test@example.com",
                "role": "roles/bigquery.dataViewer",
            }
        ]
        action = "REMOVE"

        self.client.manage_roles(resource_id, user_permissions, action)

        self.assertEqual(len(mock_dataset.access_entries), 0)
        mock_update_dataset.assert_called_once_with(
            mock_dataset, ["access_entries"]
        )

    @patch("google.cloud.bigquery.Client.get_iam_policy")
    @patch("google.cloud.bigquery.Client.set_iam_policy")
    def test_update_permission_on_table(
        self, mock_set_iam_policy, mock_get_iam_policy
    ):
        mock_policy = MagicMock()
        mock_policy.bindings = [
            {
                "role": "roles/bigquery.admin",
                "members": {"user:test@example.com"},
            }
        ]

        mock_get_iam_policy.return_value = mock_policy

        resource_id = "project.dataset.table"
        user_permissions = [
            {
                "user_email": "test@example.com",
                "role": "roles/bigquery.dataViewer",
            }
        ]
        action = "UPDATE"

        self.client.manage_roles(resource_id, user_permissions, action)

        expected_binding = {
            "role": "roles/bigquery.dataViewer",
            "members": {"user:test@example.com"},
        }
        self.assertIn(expected_binding, mock_policy.bindings)
        self.assertEqual(len(mock_policy.bindings), 2)
        mock_set_iam_policy.assert_called_once_with(
            self.mock_table, mock_policy
        )

    @patch("google.cloud.bigquery.Client.get_dataset")
    @patch("google.cloud.bigquery.Client.update_dataset")
    def test_add_permission_to_dataset(
        self, mock_update_dataset, mock_get_dataset
    ):
        mock_dataset = MagicMock()
        mock_dataset.access_entries = []

        mock_get_dataset.return_value = mock_dataset

        resource_id = "project.dataset"
        user_permissions = [
            {
                "user_email": "test@example.com",
                "role": "roles/bigquery.dataViewer",
            }
        ]
        action = "ADD"

        self.client.manage_roles(resource_id, user_permissions, action)

        new_entry = AccessEntry(
            role="roles/bigquery.dataViewer",
            entity_type="userByEmail",
            entity_id="test@example.com",
        )
        entries = [
            entry
            for entry in mock_dataset.access_entries
            if entry.role == new_entry.role
            and entry.entity_type == new_entry.entity_type
            and entry.entity_id == new_entry.entity_id
        ]

        self.assertEqual(len(entries), 1)
        self.assertEqual(len(mock_dataset.access_entries), 1)
        mock_update_dataset.assert_called_once_with(
            mock_dataset, ["access_entries"]
        )

    def test_invalid_action(self):
        """Test that invalid action raises ValueError."""
        resource_id = "project.dataset.table"
        user_permissions = [
            {
                "user_email": "test@example.com",
                "role": "roles/bigquery.dataViewer",
            }
        ]
        invalid_action = "INVALID"
        with self.assertRaises(ValueError) as context:
            self.client.manage_roles(
                resource_id, user_permissions, invalid_action
            )
            self.assertEqual(
                str(context.exception),
                f"Invalid action: {invalid_action.upper()}",
            )

    @patch("google.cloud.bigquery.Client.get_table")
    @patch("google.cloud.bigquery.Client.get_iam_policy")
    @patch("bigquery_advanced_utils.bigquery.BigQueryClient._add_permission")
    def test_exception_handling_in_manage_roles(
        self,
        mock_add_permission,
        mock_get_iam_policy,
        mock_get_table,
    ):
        """Test that exceptions are handled correctly and logged."""
        resource_id = "project.dataset.table"
        user_permissions = [
            {
                "user_email": "test@example.com",
                "role": "roles/bigquery.dataViewer",
            }
        ]
        action = "ADD"

        # mock_policy = MagicMock()
        # mock_policy.bindings = []
        # mock_get_iam_policy.return_value = mock_policy

        mock_add_permission.side_effect = Exception("Test exception")

        with self.assertLogs(level="ERROR") as log:
            with self.assertRaises(Exception) as context:
                self.client.manage_roles(resource_id, user_permissions, action)

            self.assertEqual(str(context.exception), "Test exception")
            self.assertIn(
                "ERROR:root:Error processing permission for user "
                "'test@example.com' on 'project.dataset.table': Test exception",
                log.output,
            )


@patch("google.cloud.bigquery.Client.extract_table")
class TestExportDataToStorage(unittest.TestCase):

    def setUp(self):
        # Mock del costruttore __init__ di BigQueryClient per evitare il caricamento delle credenziali
        patch("google.cloud.bigquery.Client.__init__", lambda x: None).start()

        # Creazione di una versione mockata del Client
        self.client = BigQueryClient()

    def tearDown(self):
        # Interrompi il patching dopo il test
        patch.stopall()

    def test_export_data_to_storage_csv(self, mock_extract_table):
        """Test exporting data to CSV format."""
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_extract_table.return_value = mock_job

        self.client.export_data_to_storage(
            project_id="test_project",
            dataset_id="test_dataset",
            table_id="test_table",
            destination="gs://bucket_name/path/to/file.csv",
            output_file_format="CSV",
            compression="NONE",
        )

        called_args = mock_extract_table.call_args[1]
        self.assertEqual(
            called_args["source"], "test_project.test_dataset.test_table"
        )
        self.assertEqual(
            called_args["destination_uris"],
            "gs://bucket_name/path/to/file.csv",
        )
        self.assertEqual(called_args["job_config"].destination_format, "CSV")
        self.assertEqual(called_args["job_config"].compression, None)
        self.assertEqual(called_args["project"], None)
        self.assertEqual(called_args["location"], None)
        mock_job.result.assert_called_once()

    def test_export_data_to_storage_json_with_compression(
        self, mock_extract_table
    ):
        """Test exporting data to JSON format with GZIP compression."""
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_extract_table.return_value = mock_job

        self.client.export_data_to_storage(
            project_id="test_project",
            dataset_id="test_dataset",
            table_id="test_table",
            destination="gs://bucket_name/path/to/file.json",
            output_file_format="JSON",
            compression="GZIP",
        )

        called_args = mock_extract_table.call_args[1]
        self.assertEqual(
            called_args["source"], "test_project.test_dataset.test_table"
        )
        self.assertEqual(
            called_args["destination_uris"],
            "gs://bucket_name/path/to/file.json",
        )
        self.assertEqual(called_args["job_config"].destination_format, "JSON")
        self.assertEqual(called_args["job_config"].compression, "GZIP")
        self.assertEqual(called_args["project"], None)
        self.assertEqual(called_args["location"], None)
        mock_job.result.assert_called_once()

    def test_export_data_to_storage_invalid_format(self, mock_extract_table):
        """Test exporting data with an invalid file format."""
        with self.assertRaises(ValueError):
            self.client.export_data_to_storage(
                project_id="test_project",
                dataset_id="test_dataset",
                table_id="test_table",
                destination="gs://bucket_name/path/to/file.invalid",
                output_file_format="INVALID",
                compression="NONE",
            )

    def test_export_data_to_storage_google_cloud_error(
        self, mock_extract_table
    ):
        """Test handling Google Cloud Error during export."""
        mock_extract_table.side_effect = GoogleCloudError(
            "Test Google Cloud Error"
        )

        with self.assertRaises(GoogleCloudError):
            self.client.export_data_to_storage(
                project_id="test_project",
                dataset_id="test_dataset",
                table_id="test_table",
                destination="gs://bucket_name/path/to/file.csv",
                output_file_format="CSV",
                compression="NONE",
            )

    def test_export_data_to_storage_unexpected_error(self, mock_extract_table):
        """Test handling unexpected errors during export."""
        mock_extract_table.side_effect = Exception("Test Unexpected Error")

        with self.assertRaises(Exception):
            self.client.export_data_to_storage(
                project_id="test_project",
                dataset_id="test_dataset",
                table_id="test_table",
                destination="gs://bucket_name/path/to/file.csv",
                output_file_format="CSV",
                compression="NONE",
            )

    def test_export_data_to_storage_generate_file_name(
        self, mock_extract_table
    ):
        """Test exporting data when destination does not specify a file or folder."""
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_extract_table.return_value = mock_job

        self.client.export_data_to_storage(
            project_id="test_project",
            dataset_id="test_dataset",
            table_id="test_table",
            destination="gs://bucket_name",
            output_file_format="CSV",
            compression="NONE",
        )
        called_args = mock_extract_table.call_args[1]
        expected_destination = "gs://bucket_name/test_table.csv"
        self.assertEqual(
            called_args["source"], "test_project.test_dataset.test_table"
        )
        self.assertEqual(called_args["destination_uris"], expected_destination)
        self.assertEqual(called_args["job_config"].destination_format, "CSV")
        self.assertEqual(called_args["job_config"].compression, None)
        self.assertEqual(called_args["project"], None)
        self.assertEqual(called_args["location"], None)

    def test_export_data_to_storage_bucket_with_slash(
        self, mock_extract_table
    ):
        """Test exporting data when destination is just a bucket with a slash."""
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_extract_table.return_value = mock_job

        with self.assertLogs(level="INFO") as log:

            self.client.export_data_to_storage(
                project_id="test_project",
                dataset_id="test_dataset",
                table_id="test_table",
                destination="gs://bucket_name/",
                output_file_format="CSV",
                compression="NONE",
            )

            called_args = mock_extract_table.call_args[1]
            expected_destination = "gs://bucket_name/test_table.csv"
            self.assertEqual(
                called_args["source"], "test_project.test_dataset.test_table"
            )
            self.assertEqual(
                called_args["destination_uris"], expected_destination
            )
            self.assertEqual(
                called_args["job_config"].destination_format, "CSV"
            )
            self.assertEqual(called_args["job_config"].compression, None)
            self.assertEqual(called_args["project"], None)
            self.assertEqual(called_args["location"], None)
            mock_job.result.assert_called_once()

            self.assertIn(
                "INFO:root:Destination was just a bucket with slash, creating file: 'gs://bucket_name/test_table.csv'",
                log.output,
            )


if __name__ == "__main__":
    unittest.main()
