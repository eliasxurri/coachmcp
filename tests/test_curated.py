import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]


def load_handler():
    environment = {
        "ATHENA_WORKGROUP": "test-workgroup",
        "ATHENA_DATABASE": "test-database",
        "WATERMARK_TABLE": "test-watermark",
        "LOOKBACK_DAYS": "3",
    }
    spec = importlib.util.spec_from_file_location(
        "coachmcp_curated_handler", ROOT / "lambda-curado" / "handler.py"
    )
    module = importlib.util.module_from_spec(spec)
    with (
        patch.dict(os.environ, environment),
        patch("boto3.client", return_value=MagicMock()),
        patch("boto3.resource", return_value=MagicMock()),
    ):
        spec.loader.exec_module(module)
    return module


class CuratedTests(unittest.TestCase):
    def setUp(self):
        self.handler = load_handler()

    def test_idempotency_uses_match_and_player(self):
        self.assertIn("c.match_id = r.match_id", self.handler.SQL_TEMPLATE)
        self.assertIn("c.puuid = r.puuid", self.handler.SQL_TEMPLATE)

    def test_rejects_invalid_lookback(self):
        with self.assertRaisesRegex(ValueError, "entre 1 y 3650"):
            self.handler.lambda_handler({"lookback_days": 0}, MagicMock())
        with self.assertRaisesRegex(ValueError, "entero"):
            self.handler.lambda_handler({"lookback_days": True}, MagicMock())

    def test_stops_athena_before_lambda_timeout(self):
        self.handler.athena = MagicMock()
        self.handler.athena.start_query_execution.return_value = {
            "QueryExecutionId": "query-1"
        }
        self.handler.athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }
        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = 9_000

        with self.assertRaisesRegex(TimeoutError, "timeout"):
            self.handler.lambda_handler({"lookback_days": 30}, context)

        self.handler.athena.stop_query_execution.assert_called_once_with(
            QueryExecutionId="query-1"
        )


if __name__ == "__main__":
    unittest.main()
