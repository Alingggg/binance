from datetime import datetime, timezone
import json
import uuid

import requests

from .config import (
    BINANCE_KLINE_PARAMS,
    BINANCE_KLINE_URL,
    get_warehouse_client,
)


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

    bronze_run_record = [
        extraction_id,
        run_id,
        task_try_number,
        BINANCE_KLINE_PARAMS["symbol"],
        BINANCE_KLINE_PARAMS["interval"],
        datetime.now(timezone.utc),
        json.dumps(raw_payload, separators=(",", ":")),
    ]

    client = get_warehouse_client()
    try:
        client.insert(
            "bronze_binance_api_runs",
            [bronze_run_record],
            column_names=[
                "extraction_id",
                "airflow_dag_run_id",
                "task_try_number",
                "symbol",
                "candle_interval",
                "extracted_at",
                "raw_payload",
            ],
        )
    finally:
        client.close()

    return extraction_id
