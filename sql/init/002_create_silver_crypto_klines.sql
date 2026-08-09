CREATE TABLE IF NOT EXISTS silver_crypto_klines (
    symbol                         TEXT            NOT NULL,
    candle_interval                TEXT            NOT NULL,
    open_time                      TIMESTAMPTZ     NOT NULL,
    close_time                     TIMESTAMPTZ     NOT NULL,
    open_price                     NUMERIC(30, 12) NOT NULL,
    high_price                     NUMERIC(30, 12) NOT NULL,
    low_price                      NUMERIC(30, 12) NOT NULL,
    close_price                    NUMERIC(30, 12) NOT NULL,
    volume                         NUMERIC(38, 12) NOT NULL,
    quote_asset_volume             NUMERIC(38, 12) NOT NULL,
    number_of_trades               BIGINT          NOT NULL,
    taker_buy_base_asset_volume    NUMERIC(38, 12) NOT NULL,
    taker_buy_quote_asset_volume   NUMERIC(38, 12) NOT NULL,
    is_closed                      BOOLEAN         NOT NULL,
    source_extraction_id           UUID            NOT NULL,
    loaded_at                      TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT silver_crypto_klines_pk
        PRIMARY KEY (symbol, candle_interval, open_time),
    CONSTRAINT silver_crypto_klines_source_extraction_fk
        FOREIGN KEY (source_extraction_id)
        REFERENCES bronze_binance_api_runs (extraction_id)
        ON DELETE CASCADE,
    CONSTRAINT silver_crypto_klines_symbol_chk
        CHECK (btrim(symbol) <> ''),
    CONSTRAINT silver_crypto_klines_interval_chk
        CHECK (btrim(candle_interval) <> ''),
    CONSTRAINT silver_crypto_klines_time_order_chk
        CHECK (close_time >= open_time),
    CONSTRAINT silver_crypto_klines_price_range_chk
        CHECK (
            high_price >= low_price
            AND high_price >= open_price
            AND high_price >= close_price
            AND low_price <= open_price
            AND low_price <= close_price
        ),
    CONSTRAINT silver_crypto_klines_nonnegative_chk
        CHECK (
            open_price >= 0
            AND high_price >= 0
            AND low_price >= 0
            AND close_price >= 0
            AND volume >= 0
            AND quote_asset_volume >= 0
            AND number_of_trades >= 0
            AND taker_buy_base_asset_volume >= 0
            AND taker_buy_quote_asset_volume >= 0
        )
);

CREATE INDEX IF NOT EXISTS silver_crypto_klines_source_extraction_id_idx
    ON silver_crypto_klines (source_extraction_id);

CREATE INDEX IF NOT EXISTS silver_crypto_klines_closed_time_idx
    ON silver_crypto_klines (symbol, candle_interval, open_time)
    WHERE is_closed = TRUE;
