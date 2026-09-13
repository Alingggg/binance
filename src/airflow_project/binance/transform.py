from datetime import datetime, timezone
from decimal import Decimal
import json

import pandas as pd

from .config import (
    BINANCE_KLINE_COLUMNS,
    NUMERIC_KLINE_COLUMNS,
    get_warehouse_client,
)


SILVER_COLUMNS = [
    "symbol",
    "candle_interval",
    "open_time",
    "close_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "is_closed",
    "source_extraction_id",
    "loaded_at",
]


def transform_bronze_payload(bronze_run):
    klines_df = pd.DataFrame(
        json.loads(bronze_run["raw_payload"]),
        columns=BINANCE_KLINE_COLUMNS,
    ).drop(columns="ignore")

    klines_df["symbol"] = bronze_run["symbol"]
    klines_df["candle_interval"] = bronze_run["candle_interval"]

    klines_df["open_time"] = pd.to_datetime(klines_df["open_time"], unit="ms", utc=True)
    klines_df["close_time"] = pd.to_datetime(klines_df["close_time"], unit="ms", utc=True)

    for column in NUMERIC_KLINE_COLUMNS:
        klines_df[column] = klines_df[column].map(Decimal)
    klines_df["number_of_trades"] = klines_df["number_of_trades"].astype(int)

    extracted_at = pd.to_datetime(bronze_run["extracted_at"], utc=True)
    klines_df["is_closed"] = klines_df["close_time"] < extracted_at
    klines_df["source_extraction_id"] = bronze_run["extraction_id"]
    klines_df["loaded_at"] = datetime.now(timezone.utc)

    return klines_df

def transform_and_load_silver(extraction_id):
    client = get_warehouse_client()
    try:
        result = client.query(
            """
            SELECT symbol, candle_interval, extraction_id, extracted_at, raw_payload
            FROM bronze_binance_api_runs
            WHERE extraction_id = {extraction_id:UUID}
            """,
            parameters={"extraction_id": str(extraction_id)},
        )
        bronze_run = next(iter(result.named_results()), None)
        if bronze_run is None:
            raise ValueError(f"Bronze extraction {extraction_id} was not found")

        client.insert_df(
            "silver_crypto_klines",
            transform_bronze_payload(bronze_run)[SILVER_COLUMNS],
        )
    finally:
        client.close()
