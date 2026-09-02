import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parents[1]


def load_handler():
    environment = {
        "BUCKET_NAME": "test-bucket",
        "WATERMARK_TABLE": "test-watermark",
        "API_KEY_PARAM": "/test/riot-key",
        "GLUE_DATABASE": "test_database",
        "GLUE_TABLE": "matches_raw",
        "SUMMONERS": "Elias#000,Otro#LAS",
    }
    spec = importlib.util.spec_from_file_location(
        "coachmcp_ingestion_handler", ROOT / "lambda" / "handler.py"
    )
    module = importlib.util.module_from_spec(spec)
    with (
        patch.dict(os.environ, environment),
        patch("boto3.client", return_value=MagicMock()),
        patch("boto3.resource", return_value=MagicMock()),
    ):
        spec.loader.exec_module(module)
    return module


class BackfillEventTests(unittest.TestCase):
    def setUp(self):
        self.handler = load_handler()

    def test_validates_and_normalizes_backfill_event(self):
        self.assertEqual(
            self.handler.validar_evento_backfill(
                {"player": " Elias#000 ", "start": "80", "count": "40"}
            ),
            ("Elias#000", 80, 40),
        )

    def test_rejects_unconfigured_player_and_invalid_ranges(self):
        with self.assertRaisesRegex(ValueError, "SUMMONERS"):
            self.handler.validar_evento_backfill({"player": "NoExiste#000"})
        with self.assertRaisesRegex(ValueError, "start"):
            self.handler.validar_evento_backfill(
                {"player": "Elias#000", "start": -1}
            )
        with self.assertRaisesRegex(ValueError, "count"):
            self.handler.validar_evento_backfill(
                {"player": "Elias#000", "count": 81}
            )

    def test_full_page_returns_next_cursor(self):
        self.handler.procesar_jugador = MagicMock(
            return_value={
                "ids_obtenidos": 80,
                "nuevas": 75,
                "ya_existentes": 5,
                "errores": 0,
            }
        )

        result = self.handler.ejecutar_backfill(
            {"player": "Elias#000", "start": 160, "count": 80}, "key"
        )

        self.assertEqual(result["next_start"], 240)
        self.assertFalse(result["complete"])
        self.assertFalse(result["retry_required"])
        self.handler.procesar_jugador.assert_called_once_with(
            "Elias#000",
            "key",
            start=160,
            count=80,
            max_matches=80,
            actualizar_watermark=False,
        )

    def test_partial_page_marks_backfill_complete(self):
        self.handler.procesar_jugador = MagicMock(
            return_value={
                "ids_obtenidos": 12,
                "nuevas": 12,
                "ya_existentes": 0,
                "errores": 0,
            }
        )

        result = self.handler.ejecutar_backfill(
            {"player": "Elias#000", "start": 80, "count": 80}, "key"
        )

        self.assertTrue(result["complete"])
        self.assertIsNone(result["next_start"])

    def test_failed_page_requires_same_cursor_retry(self):
        self.handler.procesar_jugador = MagicMock(
            return_value={
                "ids_obtenidos": 80,
                "nuevas": 79,
                "ya_existentes": 0,
                "errores": 1,
            }
        )

        result = self.handler.ejecutar_backfill(
            {"player": "Elias#000", "start": 80, "count": 80}, "key"
        )

        self.assertTrue(result["retry_required"])
        self.assertEqual(result["next_start"], 80)
        self.assertFalse(result["complete"])

    def test_lambda_defaults_to_incremental_mode(self):
        self.handler.get_api_key = MagicMock(return_value="key")
        self.handler.ejecutar_incremental = MagicMock(return_value={"ok": True})

        self.assertEqual(self.handler.lambda_handler({}, None), {"ok": True})
        self.handler.ejecutar_incremental.assert_called_once_with("key")
        self.handler.dynamodb.Table.return_value.delete_item.assert_called_once()
        delete_args = self.handler.dynamodb.Table.return_value.delete_item.call_args.kwargs
        self.assertEqual(
            delete_args["Key"], {"puuid": self.handler.INCREMENTAL_LOCK_KEY}
        )
        self.assertEqual(delete_args["ConditionExpression"], "#owner = :owner")
        self.assertEqual(delete_args["ExpressionAttributeNames"], {"#owner": "owner"})

    def test_backfill_uses_its_own_lock(self):
        self.handler.get_api_key = MagicMock(return_value="key")
        self.handler.ejecutar_backfill = MagicMock(return_value={"ok": True})

        result = self.handler.lambda_handler(
            {"mode": "backfill", "player": "Elias#000"}, None
        )

        self.assertEqual(result, {"ok": True})
        put_args = self.handler.dynamodb.Table.return_value.put_item.call_args.kwargs
        delete_args = self.handler.dynamodb.Table.return_value.delete_item.call_args.kwargs
        self.assertEqual(
            put_args["Item"]["puuid"], self.handler.BACKFILL_LOCK_KEY
        )
        self.assertEqual(
            delete_args["Key"], {"puuid": self.handler.BACKFILL_LOCK_KEY}
        )


class S3IdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.handler = load_handler()
        self.match = {
            "metadata": {"matchId": "LA2_123"},
            "info": {
                "gameCreation": 1_756_771_200_000,
                "gameDuration": 1800,
                "gameMode": "CLASSIC",
                "queueId": 420,
            },
        }

    def test_new_object_uses_conditional_put(self):
        self.handler.s3 = MagicMock()

        _, created = self.handler.guardar_partida("puuid", self.match)

        self.assertTrue(created)
        self.assertEqual(
            self.handler.s3.put_object.call_args.kwargs["IfNoneMatch"], "*"
        )

    def test_existing_object_is_not_an_error(self):
        error = ClientError(
            {
                "Error": {"Code": "PreconditionFailed"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            },
            "PutObject",
        )
        self.handler.s3 = MagicMock()
        self.handler.s3.put_object.side_effect = error

        _, created = self.handler.guardar_partida("puuid", self.match)

        self.assertFalse(created)


class ExecutionLockTests(unittest.TestCase):
    def test_rejects_overlapping_ingestion(self):
        handler = load_handler()
        error = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
        )
        handler.dynamodb.Table.return_value.put_item.side_effect = error

        with self.assertRaisesRegex(RuntimeError, "otra ejecución"):
            handler.adquirir_bloqueo(None)


if __name__ == "__main__":
    unittest.main()
