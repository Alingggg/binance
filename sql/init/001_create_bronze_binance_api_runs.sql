CREATE TABLE IF NOT EXISTS bronze_binance_api_runs (
    extraction_id       UUID        NOT NULL,
    airflow_dag_run_id  TEXT        NOT NULL,
    task_try_number     INTEGER     NOT NULL,
    symbol              TEXT        NOT NULL,
    candle_interval     TEXT        NOT NULL,
    extracted_at        TIMESTAMPTZ NOT NULL,
    raw_payload         JSONB       NOT NULL,

    CONSTRAINT bronze_binance_api_runs_pk
        PRIMARY KEY (extraction_id),
    CONSTRAINT bronze_binance_api_runs_task_try_number_chk
        CHECK (task_try_number > 0),
    CONSTRAINT bronze_binance_api_runs_symbol_chk
        CHECK (btrim(symbol) <> ''),
    CONSTRAINT bronze_binance_api_runs_interval_chk
        CHECK (btrim(candle_interval) <> ''),
    CONSTRAINT bronze_binance_api_runs_payload_chk
        CHECK (
            CASE
                WHEN jsonb_typeof(raw_payload) = 'array'
                    THEN jsonb_array_length(raw_payload) > 0
                ELSE FALSE
            END
        )
);

CREATE INDEX IF NOT EXISTS bronze_binance_api_runs_extracted_at_idx
    ON bronze_binance_api_runs (extracted_at);

CREATE INDEX IF NOT EXISTS bronze_binance_api_runs_airflow_lineage_idx
    ON bronze_binance_api_runs (airflow_dag_run_id, task_try_number);
