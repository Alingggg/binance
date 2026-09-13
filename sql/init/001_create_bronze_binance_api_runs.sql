CREATE TABLE IF NOT EXISTS warehouse.bronze_binance_api_runs
(
    extraction_id      UUID,
    airflow_dag_run_id String,
    task_try_number    UInt32,
    symbol             LowCardinality(String),
    candle_interval    LowCardinality(String),
    extracted_at       DateTime64(3, 'UTC'),
    raw_payload        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(extracted_at)
ORDER BY (symbol, candle_interval, extracted_at, extraction_id);
