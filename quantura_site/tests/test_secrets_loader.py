import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def load_secrets_loader(relative_path: str, module_name: str):
    module_path = ROOT / relative_path / "secrets_loader.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class SecretsLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.loaders = [
            load_secrets_loader("functions", "firebase_secrets_loader_test"),
            load_secrets_loader("functions_legacy_vercel", "vercel_secrets_loader_test"),
        ]

    def tearDown(self) -> None:
        for loader in self.loaders:
            loader._lookup_secret.cache_clear()

    def test_unset_sentinel_is_treated_as_missing(self) -> None:
        with patch.dict(os.environ, {"SOCIAL_WEBHOOK_X": "__unset__"}, clear=False):
            for loader in self.loaders:
                loader._lookup_secret.cache_clear()
                self.assertEqual(loader.get_secret("SOCIAL_WEBHOOK_X"), "")

    def test_unset_primary_value_falls_back_to_valid_alias(self) -> None:
        env = {
            "TWITTER_BEARER_TOKEN": "__UNSET__",
            "X_BEARER_TOKEN": "configured-token",
        }
        with patch.dict(os.environ, env, clear=False):
            for loader in self.loaders:
                loader._lookup_secret.cache_clear()
                self.assertEqual(loader.get_secret("TWITTER_BEARER_TOKEN"), "configured-token")

    def test_configured_secret_is_preserved(self) -> None:
        with patch.dict(os.environ, {"SOCIAL_WEBHOOK_X": "https://example.test/hook"}, clear=False):
            for loader in self.loaders:
                loader._lookup_secret.cache_clear()
                self.assertEqual(loader.get_secret("SOCIAL_WEBHOOK_X"), "https://example.test/hook")


if __name__ == "__main__":
    unittest.main()
