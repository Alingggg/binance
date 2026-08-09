from datetime import datetime, timezone
import uuid

import psycopg
from psycopg.types.json import Jsonb
import requests

from .config import BINANCE_KLINE_PARAMS, BINANCE_KLINE_URL, DB_URL


def extract_and_load_bronze(run_id: str, task_try_number: int):
    response = requests.get(
        BINANCE_KLINE_URL,
        params=BINANCE_KLINE_PARAMS,
        timeout=10,
    )
    response.raise_for_status()

    raw_payload = response.json()

    if not isinstance(raw_payload, list) or not raw_payload:
        raise ValueError("Expected a non-empty list of Binance candles")

    if any(
        not isinstance(candle, list) or len(candle) != 12
        for candle in raw_payload
    ):
        raise ValueError("One or more Binance candles have an invalid structure")

    extraction_id = uuid.uuid4()

    bronze_run_record = {
        "extraction_id": extraction_id,
        "airflow_dag_run_id": run_id,
        "task_try_number": task_try_number,
        "symbol": BINANCE_KLINE_PARAMS["symbol"],
        "candle_interval": BINANCE_KLINE_PARAMS["interval"],
        "extracted_at": datetime.now(timezone.utc),
        "raw_payload": Jsonb(raw_payload),
    }

    with psycopg.connect(DB_URL) as connection:
        with connection.cursor() as cursor:
            insert_bronze_run_sql = """
                INSERT INTO bronze_binance_api_runs (
                    extraction_id,
                    airflow_dag_run_id,
                    task_try_number,
                    symbol,
                    candle_interval,
                    extracted_at,
                    raw_payload
                )
                VALUES (
                    %(extraction_id)s,
                    %(airflow_dag_run_id)s,
                    %(task_try_number)s,
                    %(symbol)s,
                    %(candle_interval)s,
                    %(extracted_at)s,
                    %(raw_payload)s
                )
            """
            cursor.execute(insert_bronze_run_sql, bronze_run_record)

    return extraction_id
