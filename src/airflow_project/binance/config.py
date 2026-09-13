import os
import clickhouse_connect

def get_warehouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "warehouse-clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DB", "warehouse"),
        username=os.getenv("CLICKHOUSE_USER", "airflow"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "airflow"),
        connect_timeout=10,
    )

BINANCE_KLINE_URL = "https://data-api.binance.vision/api/v3/klines"
BINANCE_KLINE_PARAMS = {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "limit": 10,
}

BINANCE_KLINE_COLUMNS = [
    "open_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

NUMERIC_KLINE_COLUMNS = [
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_asset_volume",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]
