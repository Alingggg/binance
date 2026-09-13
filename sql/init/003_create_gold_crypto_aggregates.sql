CREATE OR REPLACE VIEW warehouse.gold_crypto_aggregates AS
SELECT
    symbol,
    candle_interval,
    toStartOfInterval(open_time, INTERVAL 5 MINUTE) AS window_start,
    toUInt8(5) AS window_minutes,
    argMin(open_price, open_time) AS open_price,
    max(high_price) AS high_price,
    min(low_price) AS low_price,
    argMax(close_price, open_time) AS close_price,
    sum(volume) AS total_volume,
    sum(number_of_trades) AS total_trades,
    close_price - open_price AS price_change,
    if(open_price = 0, NULL, ((close_price - open_price) / open_price) * 100)
        AS price_change_pct
FROM warehouse.silver_crypto_klines FINAL
WHERE is_closed = true
  AND candle_interval = '1m'
GROUP BY
    symbol,
    candle_interval,
    window_start
HAVING count() = 5;
