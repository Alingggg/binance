CREATE TABLE IF NOT EXISTS warehouse.silver_crypto_klines
(
    symbol                         LowCardinality(String),
    candle_interval                LowCardinality(String),
    open_time                      DateTime64(3, 'UTC'),
    close_time                     DateTime64(3, 'UTC'),
    open_price                     Decimal(30, 12),
    high_price                     Decimal(30, 12),
    low_price                      Decimal(30, 12),
    close_price                    Decimal(30, 12),
    volume                         Decimal(38, 12),
    quote_asset_volume             Decimal(38, 12),
    number_of_trades               UInt64,
    taker_buy_base_asset_volume    Decimal(38, 12),
    taker_buy_quote_asset_volume   Decimal(38, 12),
    is_closed                      Bool,
    source_extraction_id           UUID,
    loaded_at                      DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, candle_interval, open_time);
