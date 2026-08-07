# Binance Market Data ELT Pipeline with Apache Airflow

## 1. Project objective

Build a scheduled batch pipeline that retrieves Binance Spot candlestick data, preserves each raw API response in PostgreSQL, transforms the response into typed OHLCV records, and exposes time-windowed price summaries for analysis.

The project follows an ELT design:

1. **Extract** recent candlestick data from the Binance Spot REST API.
2. **Load** the complete response into a bronze PostgreSQL table.
3. **Transform** the stored response into validated silver records.
4. **Expose** gold-level time-windowed aggregations through SQL.
5. **Validate** the resulting data before completing the DAG run.

The initial implementation is intended for local learning and development. It is not a production deployment design.

## 2. Learning objectives

The completed project should demonstrate the following concepts:

- Airflow DAG structure, schedules, task dependencies, retries, and logs
- Docker Compose service discovery and container networking
- Airflow Connections for database credentials
- PostgreSQL `JSONB`, constraints, indexes, and transactions
- Bronze, silver, and gold data modeling
- Idempotent upserts for overlapping source data
- pandas-based validation and transformation
- SQL time-window aggregation
- Manual validation of Python functions, database writes, and DAG runs
- The distinction between live polling and historical backfills

## 3. Initial scope

Use the following settings for the first working version:

| Setting | Value |
| --- | --- |
| Trading pair | `BTCUSDT` |
| Binance candle interval | `1m` |
| Airflow schedule | Every five minutes |
| Candles requested per run | 10 |
| Time zone | UTC |
| Warehouse | PostgreSQL |

Each scheduled run retrieves the latest ten one-minute candles. Consecutive requests intentionally overlap. The overlap is required to practice unique keys, upserts, retry safety, and updates to an unfinished candle.

Additional symbols, historical extraction, dashboards, streaming, Spark, and table partitioning are outside the initial scope.

## 4. Technology stack

- Apache Airflow for orchestration
- Airflow's official Docker Compose quick-start with `CeleryExecutor`
- A custom Airflow image for installing project dependencies
- Python and `requests` for HTTP extraction
- pandas for response validation and transformation
- PostgreSQL for bronze, silver, and gold data
- `psycopg` for database access

The custom Airflow image should extend the same Airflow version used by Docker Compose and install dependencies from `pyproject.toml`. Rebuild the image whenever runtime dependencies change.

Required runtime dependencies:

```text
pandas
psycopg[binary]
requests
```

The `psycopg` package is sufficient for ordinary Python database functions. Add `apache-airflow-providers-postgres` only if the implementation uses Airflow's `PostgresHook` or PostgreSQL-specific operators.

## 5. Local service architecture

Use separate PostgreSQL services for Airflow metadata and pipeline data:

```text
Airflow components ─────── postgres:5432
                         Airflow metadata only

Airflow worker ────────── warehouse-postgres:5432
                         Bronze, silver, and gold data

Windows host ─────────── localhost:5433
                         Published warehouse port
```

The pipeline must never create bronze, silver, or gold objects in Airflow's metadata database.

Create an Airflow Connection named `binance_warehouse` with the following container-side settings:

| Field | Value |
| --- | --- |
| Host | `warehouse-postgres` |
| Port | `5432` |
| Database | `warehouse` |
| User | `postgres` |
| Password | Supplied through local environment configuration |

Code running inside an Airflow container must use `warehouse-postgres:5432`, not `localhost:5433`. The published `localhost:5433` address is only for tools running directly on the Windows host.

## 6. Binance API contract

Use Binance's public [Kline/Candlestick endpoint](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market#klinecandlestick-data):

```text
GET https://data-api.binance.vision/api/v3/klines
```

Initial request parameters:

```python
params = {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "limit": 10,
}
```

The market-data-only endpoint does not require an API key. Use an HTTP timeout and call `raise_for_status()` before reading the response body.

Each returned candle is an array with exactly 12 positions:

| Position | Meaning |
| --- | --- |
| 0 | Open time |
| 1 | Open price |
| 2 | High price |
| 3 | Low price |
| 4 | Close price |
| 5 | Base asset volume |
| 6 | Close time |
| 7 | Quote asset volume |
| 8 | Number of trades |
| 9 | Taker buy base asset volume |
| 10 | Taker buy quote asset volume |
| 11 | Ignored field |

Validate the outer response and every candle before loading bronze:

- The response must be a non-empty list.
- Every candle must be a list with exactly 12 positions.
- Open and close timestamps must be integers.
- Price and volume fields must be convertible to decimal values.
- The response must not contain an API error object.

The complete response must be preserved in bronze before any field is renamed, cast, filtered, or aggregated.

## 7. Data architecture

```text
Binance Spot API
       │
       ▼
bronze_binance_api_runs
Complete JSON response and extraction metadata
       │
       ▼
silver_crypto_klines
Typed, validated, deduplicated one-minute candles
       │
       ▼
gold_crypto_aggregates
SQL view containing closed-candle time windows
```

### 7.1 Bronze table

Create `bronze_binance_api_runs` with the following logical schema:

| Column | Suggested type | Purpose |
| --- | --- | --- |
| `extraction_id` | `UUID` | Primary key for the source extraction |
| `airflow_dag_run_id` | `TEXT` | Airflow DAG run lineage |
| `task_try_number` | `INTEGER` | Airflow task-attempt lineage |
| `symbol` | `TEXT` | Requested trading pair |
| `candle_interval` | `TEXT` | Requested Binance interval |
| `extracted_at` | `TIMESTAMPTZ` | UTC extraction timestamp |
| `raw_payload` | `JSONB` | Complete API response |

Required constraints:

- `extraction_id` is the primary key.
- Symbol and interval values are not empty.
- `raw_payload` is a non-empty JSON array.
- `task_try_number` is positive.

The bronze table is append-only. A new successful API request receives a new `extraction_id`, including a request performed by a later task attempt. Airflow lineage fields make those attempts distinguishable.

### 7.2 Silver table

Create `silver_crypto_klines` with the following columns:

```text
symbol
candle_interval
open_time
close_time
open_price
high_price
low_price
close_price
volume
quote_asset_volume
number_of_trades
taker_buy_base_asset_volume
taker_buy_quote_asset_volume
is_closed
source_extraction_id
loaded_at
```

Use the following key:

```text
PRIMARY KEY (symbol, candle_interval, open_time)
```

Use `NUMERIC`, not floating-point database types, for prices and volumes. Use `TIMESTAMPTZ` for timestamps and `BIGINT` for the number of trades.

`source_extraction_id` must reference the bronze extraction that last inserted or updated the silver row. Compute `is_closed` by comparing the candle's close time with the UTC extraction time. A later overlapping extraction should change it to `TRUE` after the candle closes.

Required database constraints:

- `close_time >= open_time`
- `high_price >= low_price`
- `high_price >= open_price` and `high_price >= close_price`
- `low_price <= open_price` and `low_price <= close_price`
- Prices, volumes, and trade counts are not negative

### 7.3 Gold view

Create `gold_crypto_aggregates` as a PostgreSQL view during database setup. The initial view should aggregate only rows where `is_closed = TRUE` and should emit a window only after all five expected one-minute candles are present.

Begin with five-minute windows and expose:

```text
symbol
candle_interval
window_start
window_minutes
open_price
high_price
low_price
close_price
total_volume
total_trades
price_change
price_change_pct
```

For every window:

- `open_price` is the first candle's open when ordered by `open_time`.
- `high_price` is the maximum high.
- `low_price` is the minimum low.
- `close_price` is the last candle's close when ordered by `open_time`.
- `total_volume` is the sum of base asset volume.
- `total_trades` is the sum of trade counts.
- `price_change` is close minus open.
- `price_change_pct` is `(close - open) / open * 100`.

The SQL must explicitly order the first and last values. It must not rely on insertion order.

A view requires no per-run Airflow task because it always reflects the current silver table. A later exercise may replace it with a physical table maintained by an idempotent `build_aggregations` task.

## 8. DAG design

Create one DAG named `binance_crypto_elt`:

```text
extract_and_load_bronze
          │
          ▼
transform_and_load_silver
          │
          ▼
check_data_quality
```

Initial DAG configuration:

```python
schedule="*/5 * * * *"
catchup=False
max_active_runs=1
```

Use a fixed, time-zone-aware UTC `start_date`. Configure task retries with a reasonable delay. The task retry policy should handle temporary network or database failures without creating duplicate silver rows.

### 8.1 `extract_and_load_bronze`

Responsibilities:

1. Read the symbol, interval, and limit from DAG configuration.
2. Request Binance klines with a timeout.
3. Validate the HTTP response and candle structure.
4. Generate a UUID `extraction_id`.
5. Record the UTC extraction time, Airflow DAG run ID, and task try number.
6. Insert the complete response and metadata into bronze in one database transaction.
7. Return only `extraction_id` through XCom.

The HTTP request and bronze insert belong in one task. This prevents a raw response from being transferred through a worker-local temporary file. The bronze insert completes before any transformation begins, preserving the ELT boundary.

### 8.2 `transform_and_load_silver`

Responsibilities:

1. Receive `extraction_id` from the upstream task.
2. Read the matching bronze payload from PostgreSQL.
3. Convert each 12-position array into a named record.
4. Convert millisecond timestamps into time-zone-aware UTC datetimes.
5. Convert prices and volumes without introducing floating-point database values.
6. Calculate `is_closed` using the extraction timestamp.
7. Validate the transformed DataFrame.
8. Upsert the rows into silver in one database transaction.

Use the silver primary key as the conflict target. On conflict, update candle values, `is_closed`, `source_extraction_id`, and `loaded_at`.

Do not pass the DataFrame through XCom. XCom should contain only the extraction identifier and other small metadata.

### 8.3 `check_data_quality`

Fail the DAG when any required condition is violated:

- The bronze row exists for the current `extraction_id`.
- Silver contains at least one row in the current extraction's open-time range.
- Required timestamps and prices are not null.
- Price-range constraints hold.
- Prices, volumes, and trade counts are not negative.
- No duplicate silver primary keys exist.
- Every silver `source_extraction_id` references bronze.
- Every complete five-minute window from the current range is visible in gold.

Checks should raise clear exceptions that identify the failed rule and relevant extraction ID.

## 9. Database initialization and migrations

Place first-time schema files under `sql/init/` so PostgreSQL executes them when the warehouse data volume is initially created:

```text
sql/init/
├── 001_create_bronze_binance_api_runs.sql
├── 002_create_silver_crypto_klines.sql
└── 003_create_gold_crypto_aggregates.sql
```

PostgreSQL initialization scripts execute only against an empty data directory. Editing an initialization file does not update an existing warehouse volume.

Use the following rule:

- Initial bootstrap: versioned files under `sql/init/`
- Subsequent schema changes: explicit versioned migrations

Do not depend on deleting the database volume as the normal method for applying schema changes.

## 10. Project structure

```text
airflow-project/
├── dags/
│   └── binance_crypto_elt.py
├── src/
│   └── airflow_project/
│       └── binance/
│           ├── __init__.py
│           ├── extract.py
│           ├── transform.py
│           └── validate.py
├── sql/
│   └── init/
│       ├── 001_create_bronze_binance_api_runs.sql
│       ├── 002_create_silver_crypto_klines.sql
│       └── 003_create_gold_crypto_aggregates.sql
├── docker-compose.yaml
├── Dockerfile
├── pyproject.toml
└── uv.lock
```

Keep the DAG file limited to orchestration concerns: configuration, task definitions, dependencies, retry policy, and scheduling. Place reusable extraction, transformation, loading, and validation logic under `src/airflow_project/binance/`.

All non-Airflow functions should be directly importable and runnable without starting the scheduler. This allows them to be checked manually from a notebook or a small Python script before the DAG is created.

The file names describe the main responsibility of each module:

- `extract.py` requests and validates the Binance response, then inserts it into bronze.
- `transform.py` reads the bronze payload, converts it into typed records, then upserts them into silver.
- `validate.py` contains reusable data-quality checks.

Loading simply means writing data to a destination. This pipeline loads twice: the raw response is loaded into bronze, then the transformed records are loaded into silver. There is no need for a separate `load.py` or a separate `load` task at this project size. Use the two task names `extract_and_load_bronze` and `transform_and_load_silver` so both database writes are explicit.

## 11. Implementation phases

### Phase 1 — Validate the environment

- Build the custom Airflow image.
- Initialize and start the Compose environment.
- Confirm that all required services are healthy.
- Confirm that the Airflow worker can resolve `warehouse-postgres`.
- Create and test the `binance_warehouse` Airflow Connection.

### Phase 2 — Explore and validate the API

- Execute the Binance request from a normal Python module or notebook.
- Inspect the array-based response.
- Confirm UTC timestamp conversion.
- Confirm decimal conversion of price and volume fields.
- Save or inspect one representative response for repeatable manual checks.

### Phase 3 — Create the data model

- Create bronze and silver tables.
- Create the initial five-minute gold view.
- Insert a representative response into bronze manually.
- Transform and upsert it into silver.
- Query silver chronologically and inspect the gold result.

### Phase 4 — Implement reusable Python functions

- Implement API extraction and validation.
- Implement bronze insertion.
- Implement payload transformation.
- Implement silver upsert logic.
- Implement data-quality functions.
- Run the functions manually and inspect their database results before creating the DAG.

### Phase 5 — Build the DAG

- Define the three tasks and dependencies.
- Configure the schedule, start date, retries, and task timeouts.
- Run each task individually.
- Trigger the complete DAG manually.
- Verify bronze, silver, and gold results.
- Enable the schedule only after the manual run succeeds.

### Phase 6 — Prove reliability

- Allow several scheduled runs to overlap at the candle level.
- Confirm that silver does not accumulate duplicate keys.
- Confirm that an unfinished candle is updated and later marked closed.
- Retry and clear tasks to observe retry behavior.
- Confirm that failures produce useful logs and quality-check messages.

## 12. Manual validation checklist

Before moving the functions into the DAG:

- Run the Binance request and confirm it returns a non-empty list of 12-position candles.
- Run the bronze insert and query the row by `extraction_id`.
- Run the transformation and inspect the column names, UTC timestamps, decimal values, and `is_closed` result.
- Run the silver upsert and query the affected candles in chronological order.
- Reprocess overlapping candles and confirm the silver row count does not grow for existing primary keys.
- Confirm that an updated candle changes values and `source_extraction_id`.
- Query the gold view and manually compare one complete five-minute window.
- Confirm that unfinished candles and incomplete five-minute windows are excluded from gold.

After creating the DAG:

- Confirm that Airflow parses the DAG without import errors.
- Confirm the expected task IDs and dependencies in the Airflow UI.
- Run each task individually during development.
- Trigger a full manual DAG run.
- Confirm that XCom stores only the extraction identifier, not the payload or DataFrame.
- Retry or clear a task and confirm the silver upsert remains idempotent.

## 13. Live polling and historical backfills

The initial DAG is a live polling pipeline. A request containing only `symbol`, `interval`, and `limit` always retrieves the most recent candles. Re-running an older Airflow DAG run therefore does not reproduce the historical data interval associated with that run.

Historical extraction must be implemented separately after the live pipeline is reliable:

- Supply Binance `startTime` and `endTime` parameters.
- Derive boundaries from Airflow's data interval.
- Respect the endpoint result limit and paginate until the interval is complete.
- Reuse the same bronze and silver loading functions.
- Preserve idempotency through the silver primary key.
- Prefer a separately triggered backfill DAG or an explicit historical mode rather than silently changing the live DAG's semantics.

Do not describe the initial `limit=10` design as a backfill implementation.

## 14. Optional extensions

Complete the initial project before adding any of the following:

- Add `ETHUSDT` and `BNBUSDT`.
- Use Airflow dynamic task mapping for multiple symbols.
- Add fifteen-minute and hourly gold windows.
- Replace the gold view with an incrementally maintained table.
- Add a database migration tool.
- Create a dashboard from the gold layer.
- Add historical backfill support.
- Partition a substantially larger silver table by date.
- Build a separate large historical-data exercise with Spark.

WebSockets and streaming platforms are intentionally excluded from this scheduled batch project.

## 15. Definition of done

The initial project is complete when all of the following conditions are satisfied:

- Airflow retrieves `BTCUSDT` candles every five minutes.
- Every successful extraction is preserved in bronze with lineage metadata.
- Silver contains correctly typed, UTC-normalized candle records.
- Reprocessing overlapping candles does not create duplicates.
- An unfinished candle can be updated and later marked closed.
- The five-minute gold view uses ordered open and close values.
- Unfinished candles and incomplete five-minute windows are excluded from the initial gold results.
- Invalid data causes a clear quality-check failure.
- Extraction, transformation, and loading logic pass the manual validation checklist.
- The DAG parses successfully and completes a manual end-to-end run.
- Several scheduled runs complete without violating database constraints or silver uniqueness.
