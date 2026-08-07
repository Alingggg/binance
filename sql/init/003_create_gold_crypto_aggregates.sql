CREATE OR REPLACE VIEW gold_crypto_aggregates AS
WITH bucketed AS (
    SELECT
        symbol,
        candle_interval,
        open_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        number_of_trades,
        date_bin(
            INTERVAL '5 minutes',
            open_time,
            TIMESTAMPTZ '1970-01-01 00:00:00+00'
        ) AS window_start
    FROM silver_crypto_klines
    WHERE is_closed = TRUE
      AND candle_interval = '1m'
),
complete_windows AS (
    SELECT
        symbol,
        candle_interval,
        window_start,
        (array_agg(open_price ORDER BY open_time ASC))[1] AS open_price,
        MAX(high_price) AS high_price,
        MIN(low_price) AS low_price,
        (array_agg(close_price ORDER BY open_time DESC))[1] AS close_price,
        SUM(volume) AS total_volume,
        SUM(number_of_trades) AS total_trades
    FROM bucketed
    GROUP BY
        symbol,
        candle_interval,
        window_start
    HAVING COUNT(*) = 5
)
SELECT
    symbol,
    candle_interval,
    window_start,
    5::SMALLINT AS window_minutes,
    open_price,
    high_price,
    low_price,
    close_price,
    total_volume,
    total_trades,
    close_price - open_price AS price_change,
    CASE
        WHEN open_price = 0 THEN NULL
        ELSE ((close_price - open_price) / open_price) * 100
    END AS price_change_pct
FROM complete_windows;
