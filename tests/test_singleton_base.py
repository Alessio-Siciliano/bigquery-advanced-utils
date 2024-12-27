# type: ignore
import unittest
from unittest.mock import patch, MagicMock
import logging
from bigquery_advanced_utils.core import (
    SingletonBase,
)


class TestSingletonBase(unittest.TestCase):
    @patch.object(logging, "debug")
    @patch.object(logging, "info")
    @patch.object(logging, "error")
    def test_singleton_behavior(self, mock_error, mock_info, mock_debug):
        class MySingleton(SingletonBase):
            def __init__(self):
                pass

            def _initialize(self):
                # Simulate some initialization logic
                pass

        instance1 = MySingleton()
        instance2 = MySingleton()

        self.assertIs(instance1, instance2)  # Verify single instance
        self.assertTrue(mock_debug.called)
        self.assertTrue(mock_info.called)
        self.assertFalse(mock_error.called)  # No error during initialization

    @patch.object(SingletonBase, "__init__")
    def test_subclass_initialization(self, mock_initialize):
        class MySingleton(SingletonBase):
            def __new__(cls):
                instance = super().__new__(cls)
                return instance

            def __init__(self):
                # Simulate custom initialization logic
                super().__init__()

        MySingleton()
        self.assertTrue(mock_initialize.called)


if __name__ == "__main__":
    unittest.main()
