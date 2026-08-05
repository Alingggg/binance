# Binance Airflow ELT Practice Project

## Project goal

Build a cryptocurrency market-data pipeline that uses Apache Airflow to:

1. **Extract** recent candlestick data from the Binance Spot API.
2. **Load** the raw API response into PostgreSQL.
3. **Transform** it into clean, queryable OHLCV records.
4. Produce your own time-based aggregations to observe price changes.

This is an **ELT** project because the raw response is stored before it is transformed.

## Initial scope

Start with these settings:

- Trading pair: `BTCUSDT`
- Binance candle interval: `1m`
- Airflow schedule: every 5 minutes
- Candles requested per run: 10
- Database: PostgreSQL

Each DAG run retrieves the most recent ten one-minute candles. Consecutive requests overlap, which lets you practice upserts and idempotency.

After the BTC pipeline works, you can add other pairs such as `ETHUSDT` and `BNBUSDT`.

## Suggested tools

- Apache Airflow for orchestration
- Python and `requests` for extraction
- PostgreSQL for bronze, silver, and gold tables
- pandas for flattening and cleaning the API response
- SQL for aggregations
- `pytest` for testing Python code
- Docker Compose for the local environment

Spark is unnecessary for the initial project because each request contains very little data. Use it later in a separate exercise with a much larger historical dataset.

## Binance API request

Use Binance's public [Kline/Candlestick endpoint](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market#klinecandlestick-data):

```text
GET /api/v3/klines
```

Example request:

```text
https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=10
```

The market-data-only endpoint does not require an API key. See Binance's [Market Data Only documentation](https://developers.binance.com/en/docs/products/spot/faqs/market_data_only).

Build the request using Python parameters rather than storing one long URL:

```python
import requests

BASE_URL = "https://data-api.binance.vision/api/v3/klines"

params = {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "limit": 10,
}

response = requests.get(BASE_URL, params=params, timeout=30)
response.raise_for_status()
raw_klines = response.json()
```

Binance returns each candle as an array rather than an object. Its important positions are:

```text
0  open time
1  open price
2  high price
3  low price
4  close price
5  volume
6  close time
7  quote asset volume
8  number of trades
9  taker buy base asset volume
10 taker buy quote asset volume
11 unused field
```

Preserve the complete response in bronze before converting these positions into named columns.

## Pipeline flow

```text
Binance Spot API
       ↓
Extract recent candles
       ↓
Load raw response into PostgreSQL (bronze)
       ↓
Flatten and clean candles with pandas (silver)
       ↓
Aggregate candles with SQL (gold)
       ↓
Run data-quality checks
```

## Airflow DAG

Create one DAG named `binance_crypto_elt`.

```text
extract_klines
      ↓
load_raw_klines
      ↓
transform_klines
      ↓
load_silver_klines
      ↓
build_aggregations
      ↓
check_data_quality
```

Schedule it every five minutes:

```python
schedule="*/5 * * * *"
```

Also begin with:

```python
catchup=False
max_active_runs=1
```

## Task responsibilities

### 1. `extract_klines`

- Request the ten most recent one-minute candles.
- Use an HTTP timeout.
- Fail if the response is unsuccessful.
- Verify that the result is a non-empty list.
- Generate a unique `run_id`.
- Record `extracted_at` in UTC.
- Save the raw JSON response to a shared or temporary file.

Do not pass the complete response through XCom. Pass only small metadata such as the file path, `run_id`, symbol, and extraction timestamp.

### 2. `load_raw_klines`

- Read the JSON file.
- Insert one row into `bronze_binance_api_runs`.
- Store the complete response in a PostgreSQL `JSONB` column.
- Do not flatten, cast, or aggregate it yet.

Suggested columns:

```text
run_id
symbol
candle_interval
extracted_at
raw_payload
```

Use `run_id` as the primary key.

### 3. `transform_klines`

- Read the raw payload from the bronze table.
- Convert each Binance array into a named record.
- Use pandas to create a DataFrame.
- Convert millisecond timestamps into UTC datetimes.
- Convert price and volume strings into numeric values.
- Add the symbol, interval, and source run ID.

This transformation occurs after loading bronze, so the pipeline remains ELT.

### 4. `load_silver_klines`

Load the cleaned DataFrame into `silver_crypto_klines`.

Suggested columns:

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
source_run_id
loaded_at
```

Use this unique key:

```text
(symbol, candle_interval, open_time)
```

Because requests overlap, use an upsert instead of a plain insert. A candle returned before it closes may change, so a later run should update the existing row.

### 5. `build_aggregations`

Use SQL to build a `gold_crypto_aggregates` table or view. Choose the aggregation yourself so you can compare how the results change.

Possible windows:

- Five-minute price summaries
- Fifteen-minute price summaries
- Hourly price summaries

Possible output fields:

```text
symbol
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
created_at
```

For each window:

- `open_price` is the first candle's open.
- `high_price` is the maximum high.
- `low_price` is the minimum low.
- `close_price` is the last candle's close.
- `total_volume` is the sum of volume.
- `price_change` is close minus open.
- `price_change_pct` is `(close - open) / open * 100`.

Start with one aggregation window. Add the others only after it works.

### 6. `check_data_quality`

Fail the DAG when an important rule is violated:

- A bronze row exists for the current run.
- At least one silver candle exists for the requested period.
- Required timestamps and prices are not null.
- `high_price >= low_price`.
- `high_price` is not lower than open or close.
- `low_price` is not higher than open or close.
- Prices and volumes are not negative.
- `number_of_trades` is not negative.
- No duplicate silver unique keys exist.

## Suggested project structure

```text
binance-elt/
├── dags/
│   └── binance_crypto_elt.py
├── include/
│   └── binance/
│       ├── __init__.py
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       └── validate.py
├── sql/
│   ├── create_tables.sql
│   ├── build_aggregations.sql
│   └── quality_checks.sql
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── fixtures/
│       └── binance_klines_response.json
├── data/
├── docker-compose.yaml
└── requirements.txt
```

Airflow only needs to discover the DAG file under `dags/`. The `include/binance/` structure is an organizational choice for keeping normal Python logic outside the DAG definition.

The files should have these responsibilities:

- `extract.py`: API request logic
- `transform.py`: pandas transformations
- `load.py`: PostgreSQL inserts and upserts
- `validate.py`: API and DataFrame validation
- `binance_crypto_elt.py`: Airflow tasks, scheduling, dependencies, and retries

Keep the DAG file thin. Extraction and transformation functions should be runnable and testable without starting a complete DAG run.

## Testing approach

### Unit tests

- Save one real Binance response as a fixture.
- Mock the HTTP request in extraction tests.
- Test an empty API response.
- Test a malformed candle with missing positions.
- Test timestamp and numeric conversions.
- Test the expected DataFrame columns and types.

### Database and SQL tests

- Load a known fixture into a test bronze table.
- Run the pandas transformation and silver load.
- Reprocess the same candles and verify that duplicates are not created.
- Change one candle and confirm that the upsert updates it.
- Run your aggregation SQL and compare its result with a manually calculated result.

### Airflow tests

- Confirm that Airflow parses the DAG without import errors.
- Run tasks individually while developing them.
- Trigger the full DAG manually before enabling the schedule.
- Inspect task logs and PostgreSQL row counts.
- Let the schedule run several times and confirm that new candles appear.

## Implementation milestones

### Milestone 1 — Explore the API

- Run the request from a normal Python file or notebook.
- Inspect the array-based response.
- Convert timestamps and numeric strings manually.
- Save one response as a test fixture.

### Milestone 2 — Set up the database

- Create bronze and silver tables.
- Insert a sample raw response into bronze.
- Transform it with pandas and load it into silver.
- Query the clean candles in chronological order.

### Milestone 3 — Build the Airflow DAG

- Define the tasks and dependencies.
- Run each task individually.
- Trigger the complete pipeline manually.
- Confirm that bronze and silver receive data.

### Milestone 4 — Make the pipeline reliable

- Add retries and an HTTP timeout.
- Add unique keys and upserts.
- Add data-quality checks.
- Re-run the same task and confirm that it does not create duplicates.

### Milestone 5 — Build your aggregation

- Create one SQL aggregation window.
- Let the DAG run for enough time to collect data.
- Compare successive aggregation results.
- Verify the calculations manually using a small sample.

### Milestone 6 — Optional improvements

- Add `ETHUSDT` and `BNBUSDT`.
- Use Airflow dynamic task mapping for multiple symbols.
- Add more aggregation windows.
- Create a simple dashboard from the gold table.
- Backfill older candles using `startTime` and `endTime`.
- Partition a larger silver table by date.

## Definition of done

The initial project is complete when:

- Airflow requests Binance data every five minutes.
- Every extraction is preserved in bronze.
- Silver contains clean and correctly typed candles.
- Overlapping requests do not create duplicates.
- An unfinished candle can be updated by a later run.
- Your gold query correctly aggregates a chosen time window.
- Invalid data causes the quality-check task to fail clearly.
- Extraction and transformation logic have automated tests.

## Recommended learning order

1. Binance response structure
2. pandas DataFrame creation and type conversion
3. DAG structure and task dependencies
4. Airflow task logs and retries
5. Airflow Connections
6. PostgreSQL `JSONB`
7. Idempotent upserts
8. SQL time-based aggregation
9. Data-quality checks
10. Scheduling and backfills

Avoid WebSockets, streaming platforms, dashboards, and Spark until the scheduled batch pipeline works reliably.
