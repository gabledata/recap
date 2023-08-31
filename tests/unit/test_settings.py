import os
import tempfile
from importlib import reload
from unittest.mock import patch

from pydantic import AnyUrl

import recap.settings


class TestSettings:
    @classmethod
    def setup_class(cls):
        # Create a temporary .env file for the test
        cls.temp_file = tempfile.NamedTemporaryFile(delete=False)
        cls.temp_file_name = cls.temp_file.name

    @classmethod
    def teardown_class(cls):
        # Delete the temporary .env file
        os.unlink(cls.temp_file_name)

    def test_set_and_unset_config(self):
        with patch.dict(os.environ, {"RECAP_CONFIG": self.temp_file_name}, clear=False):
            # Reload after patching to reset CONFIG_FILE
            reload(recap.settings)
            from recap.settings import RecapSettings, set_config, unset_config

            # Test set_config
            set_config("recap_systems__test_key", "foo://test_value")
            assert RecapSettings().systems.get("test_key") == AnyUrl("foo://test_value")

            # Test unset_config
            unset_config("recap_systems__test_key")
            assert RecapSettings().systems.get("test_key") is None
