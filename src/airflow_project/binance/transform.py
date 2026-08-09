from datetime import datetime, timezone
from decimal import Decimal
import pandas as pd
import psycopg
from psycopg.rows import dict_row

from .config import BINANCE_KLINE_COLUMNS, DB_URL, NUMERIC_KLINE_COLUMNS

def transform_bronze_payload(bronze_run):
    klines_df = pd.DataFrame(
        bronze_run["raw_payload"],
        columns=BINANCE_KLINE_COLUMNS,
    ).drop(columns="ignore")

    klines_df["symbol"] = bronze_run["symbol"]
    klines_df["candle_interval"] = bronze_run["candle_interval"]

    klines_df["open_time"] = pd.to_datetime(klines_df["open_time"], unit="ms", utc=True)
    klines_df["close_time"] = pd.to_datetime(klines_df["close_time"], unit="ms", utc=True)

    for column in NUMERIC_KLINE_COLUMNS:
        klines_df[column] = klines_df[column].map(
            lambda value: Decimal(str(value))
        )
    klines_df["number_of_trades"] = klines_df["number_of_trades"].astype(int)

    klines_df["is_closed"] = (klines_df["close_time"] < bronze_run["extracted_at"])
    klines_df["source_extraction_id"] = bronze_run["extraction_id"]
    klines_df["loaded_at"] = datetime.now(timezone.utc)

    return klines_df

def transform_and_load_silver(extraction_id):
    with psycopg.connect(DB_URL, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            select_bronze_run_sql = """
                SELECT symbol, candle_interval, extraction_id, extracted_at, raw_payload
                FROM bronze_binance_api_runs
                WHERE extraction_id = %s
            """

            cursor.execute(select_bronze_run_sql, (extraction_id,))
            bronze_run = cursor.fetchone()

            silver_rows = transform_bronze_payload(bronze_run).to_dict(
                orient="records"
            )

            upsert_silver_klines_sql = """
                INSERT INTO silver_crypto_klines (
                    symbol,
                    candle_interval,
                    open_time,
                    close_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    quote_asset_volume,
                    number_of_trades,
                    taker_buy_base_asset_volume,
                    taker_buy_quote_asset_volume,
                    is_closed,
                    source_extraction_id,
                    loaded_at
                )
                VALUES (
                    %(symbol)s,
                    %(candle_interval)s,
                    %(open_time)s,
                    %(close_time)s,
                    %(open_price)s,
                    %(high_price)s,
                    %(low_price)s,
                    %(close_price)s,
                    %(volume)s,
                    %(quote_asset_volume)s,
                    %(number_of_trades)s,
                    %(taker_buy_base_asset_volume)s,
                    %(taker_buy_quote_asset_volume)s,
                    %(is_closed)s,
                    %(source_extraction_id)s,
                    %(loaded_at)s
                )
                ON CONFLICT (symbol, candle_interval, open_time) DO UPDATE SET
                    close_time = EXCLUDED.close_time,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    quote_asset_volume = EXCLUDED.quote_asset_volume,
                    number_of_trades = EXCLUDED.number_of_trades,
                    taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
                    taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
                    is_closed = EXCLUDED.is_closed,
                    source_extraction_id = EXCLUDED.source_extraction_id,
                    loaded_at = EXCLUDED.loaded_at
            """
            cursor.executemany(upsert_silver_klines_sql, silver_rows)