# Watt ETW Forecasting And Optimization Report

This report explains what the project currently does, how the forecasting and battery suggestions are built, what each file is responsible for, and where to make changes when we want to improve the system.

The short version: Watt ETW is becoming a Greek electricity-market decision tool. It has one finished user-facing workflow today: upload HEnEx-style Day-Ahead Market data, describe a battery fleet, and get a feasible charge/discharge schedule with KPIs and explanations. In parallel, the repo contains the foundation for a richer price forecasting pipeline: parse historical HEnEx data, join it with weather and TTF gas data, train one model per delivery hour, and use the forecasts as future price inputs for optimization.

## 1. Big Picture

The project has two related layers:

1. Battery optimization MVP
   - Input: uploaded DAM price file plus battery fleet settings.
   - Output: suggested action per interval: charge, discharge, or idle.
   - Output also includes expected profit, revenue, charging cost, degradation cost, cycles, final SOC, and a plain-language explanation for each action.

2. Forecasting pipeline
   - Input: historical HEnEx market data, weather, TTF gas prices, and optionally renewable asset weather matched from RAE Geoportal locations.
   - Output: predicted MCP, meaning Market Clearing Price, for each hour of a target day.
   - Intended use: feed predicted prices into the optimizer when the real DAM price file is not yet available, or compare a forecast schedule against actual prices.

The current frontend and API use the optimization MVP. The forecasting code exists as Python modules, but it is not yet wired into the FastAPI endpoints or React UI.

## 2. End-To-End Data Flow

Current app flow:

```text
React UI
  -> user enters battery specs
  -> user uploads CSV/XLSX DAM file
  -> POST /api/market-data/validate
  -> backend parses and validates market rows
  -> user selects delivery datemade by ribbon X discharge
  -> POST /api/optimize
  -> backend aggregates fleet and runs optimizer
  -> React shows KPIs, charts, schedule, explanations
  -> optional POST /api/export-schedule
  -> backend returns CSV schedule
```

Forecasting flow:

```text
HEnEx historical XLSX files
  -> henex_parser.py
  -> tidy price/supply DataFrame

Open-Meteo weather
  -> weather_fetcher.py
  -> hourly weather DataFrame

TTF gas prices
  -> ttf_fetcher.py
  -> daily gas price DataFrame

Optional RAE renewable assets
  -> rae_geoportal.py
  -> asset coordinates and capacities
  -> weather_fetcher.fetch_renewable_weather_features()
  -> capacity-weighted weather by technology

All sources
  -> feature_builder.py
  -> one row per date/hour
  -> price_forecaster.py
  -> 24 LightGBM models, one per hour
  -> forecasted MCP
  -> optimizer.py can use these predicted prices as MarketRow inputs
```

## 3. What The Suggestions Mean

The suggestions are not yet produced by a mathematical optimization solver. They are produced by a deterministic heuristic in `watt_etw/optimizer.py`.

For every market interval, the optimizer decides:

- `charge`: buy energy from the market and store it in the battery.
- `discharge`: sell stored energy back into the market.
- `idle`: do nothing.

The decision is based on:

- The current DAM price.
- Low and high price thresholds calculated from the day price distribution.
- Future prices later in the same selected day.
- Battery capacity, power, ramp, availability, efficiency, SOC limits, degradation cost, and optional cycle limit.
- Whether the trade still looks profitable after losses and degradation cost.

The optimizer tracks state of charge, or SOC, interval by interval. It never intentionally charges and discharges at the same time. It also keeps SOC inside the battery's min/max limits.

## 4. How The Optimizer Works

The entry point is:

```python
optimize_schedule(rows: list[MarketRow], fleet: AggregatedFleet) -> OptimizationResult
```

### 4.1 Market intervals

The optimizer first calculates the interval length from the difference between consecutive timestamps. If the data is hourly, `interval_hours` is `1.0`. If the data is 15-minute data, `interval_hours` is `0.25`.

### 4.2 Price thresholds

`_price_thresholds()` sorts all prices for the selected day:

- Low threshold: roughly the 30th percentile.
- High threshold: roughly the 70th percentile.

If the spread between the high and low thresholds is too small compared with degradation cost and efficiency losses, the function widens the thresholds around the average price. This prevents cycling when prices are flat or unprofitable.

### 4.3 Efficiency treatment

Battery round-trip efficiency is split into two symmetric parts:

```python
charge_eff = sqrt(round_trip_efficiency)
discharge_eff = sqrt(round_trip_efficiency)
```

For example, a 90 percent round-trip efficient battery has about 94.87 percent charge efficiency and 94.87 percent discharge efficiency. This lets the schedule lose energy partly when charging and partly when discharging.

### 4.4 Charging rule

The optimizer charges when:

- The current price is at or below the low threshold.
- There is a future high price that can justify buying now.
- The expected future sale value, after efficiency losses, beats the current price plus degradation cost.
- The battery has room below max SOC.
- The optional daily throughput limit has remaining room.

It charges no more than:

- Fleet power limit times interval length.
- Remaining SOC capacity adjusted for charge efficiency.
- Remaining throughput/cycle allowance, if configured.

### 4.5 Discharging rule

The optimizer discharges when:

- The current price is at or above the high threshold, or
- The current price is higher than a future low price by more than degradation cost and SOC is above the initial SOC.

It discharges no more than:

- Fleet power limit times interval length.
- Energy available above min SOC adjusted for discharge efficiency.
- Remaining throughput/cycle allowance, if configured.

### 4.6 Final SOC repair

After the first pass, if the battery ends below its initial SOC, `_restore_final_soc()` adds enough charging at the cheapest interval to restore the required final SOC. This is a simple way to avoid schedules that make money only by emptying the battery by the end of the day.

Important limitation: this repair updates KPI accounting and the selected schedule row, but it does not recompute every later SOC row after the inserted charge. That may be acceptable for the MVP, but it is a place to improve if we want stricter physical consistency.

### 4.7 KPIs

The optimizer returns:

- `expected_profit`: discharge revenue minus charging cost minus degradation cost.
- `discharge_revenue`: energy sold times market price.
- `charging_cost`: energy bought times market price.
- `degradation_cost`: throughput times degradation cost per MWh.
- `charged_mwh` and `discharged_mwh`.
- Average buy and sell prices.
- `equivalent_cycles`: throughput divided by twice fleet capacity.
- Final SOC, fleet capacity, and fleet power.

### 4.8 Explanations

`watt_etw/explanations.py` converts each action into a user-friendly sentence. Examples:

- Charge because the price is low and capacity is available.
- Discharge because the price is high and stored energy is available.
- Idle because SOC is near a bound.
- Idle because the spread is not attractive after losses and degradation.

These explanations are displayed in the schedule table in the frontend.

## 5. How Forecasting Works

Forecasting is handled by `watt_etw/forecasting/price_forecaster.py`.

The design is to train 24 separate models:

- One model for hour 0.
- One model for hour 1.
- ...
- One model for hour 23.

This is useful because Greek DAM prices often have different behavior by hour. Midday prices can be shaped by solar production, evening prices by demand ramps, and night prices by lower demand and different thermal constraints.

### 5.1 Target

The target column is:

```text
mcp_eur_mwh
```

That is the Market Clearing Price in EUR/MWh.

### 5.2 Features

The model can use:

- Price history: `mcp_lag1h`, `mcp_lag2h`, `mcp_lag24h`, `mcp_lag48h`, `mcp_lag168h`.
- Rolling price statistics: previous 24-hour mean and standard deviation.
- Supply mix: sell volume, gas, hydro, renewables, lignite, imports.
- Calendar: day of week, month, hour, weekend flag, Greek holiday flag.
- Weather: temperature, radiation, wind, cloud cover, humidity, precipitation.
- Gas: TTF spot/futures proxy plus 1-day and 7-day lags.

The forecaster only uses feature columns that actually exist in the DataFrame. Missing expected columns are logged and skipped.

### 5.3 Training

`PriceForecaster.train(features_df)`:

1. Sorts rows by date and hour.
2. Selects available feature columns.
3. Splits train/test by date, keeping the last `test_days` as holdout.
4. For each hour, filters to rows for that hour only.
5. Trains a LightGBM regressor for that hour.
6. Calculates MAE, RMSE, and MAPE on the held-out period.

This is time-series aware because it does not shuffle future rows into the training period.

### 5.4 Prediction

`PriceForecaster.predict(features_df, target_date)`:

1. Finds the 24 rows for the target date.
2. For each hour, selects that hour's row.
3. Runs the corresponding hourly model.
4. Floors negative predictions to zero.
5. Returns a dictionary like:

```python
{0: 92.31, 1: 88.44, ..., 23: 101.27}
```

### 5.5 Saving and loading

The forecaster writes:

- `meta.json` with feature columns, trained hours, and LightGBM params.
- `hour_00.pkl` through `hour_23.pkl` for trained models.

Models live by default under:

```text
models/price_forecaster/
```

### 5.6 Current integration status

The forecast module is not yet exposed through the API. There is currently no endpoint like `/api/forecast`, and the frontend does not show forecasted prices. To use forecasts in the app, we would add an API route that loads or trains a model, builds target-date features, predicts MCP, converts predictions to `MarketRow` objects, and sends them into `optimize_schedule()`.

Also note that `lightgbm`, `yfinance`, and `holidays` are used by optional forecasting/data code but are not listed in `pyproject.toml` dependencies at the time of this report. The current `pyproject.toml` lists FastAPI, pandas, requests, python-multipart, and uvicorn, plus pytest/httpx for dev tests.

## 6. File-By-File Explanation

### Root files

#### `README.md`

Brief project intro and run instructions. It describes the MVP user flow:

```text
Landing Page -> Battery Fleet Setup -> Import DAM CSV/XLSX -> Validate -> Optimize -> Dashboard
```

It also lists the backend API endpoints and frontend Vite commands.

#### `pyproject.toml`

Python project metadata and dependencies. It sets:

- Package name: `watt-etw`.
- Python version: `>=3.11`.
- Runtime dependencies used by the API and current data code.
- Dev dependencies for tests.
- Pytest configuration so tests are discovered from `tests/` and the repo root is on `pythonpath`.

### Core package

#### `watt_etw/__init__.py`

Package-level docstring and exported module names. It marks the package as battery fleet optimization for HEnEx-style day-ahead data.

#### `watt_etw/battery_fleet.py`

Defines the battery input model and aggregation logic.

Key classes:

- `BatterySpec`: one user-entered battery.
- `AggregatedFleet`: the combined fleet passed to the optimizer.

Important behavior:

- Accepts multiple possible frontend/user field names, such as `capacity`, `max_capacity`, `capacity_mwh`, `efficiency`, and even the misspelled `effieciency`.
- Converts min capacity in MWh into min SOC percent when needed.
- Normalizes availability if the user enters `0.95` instead of `95`.
- Validates that capacity, power, efficiency, availability, SOC bounds, degradation, and cycle limits are sensible.
- Aggregates multiple batteries by summing capacity/power and capacity-weighting efficiency, availability, SOC percentages, and degradation cost.

Where to change:

- Add more battery fields here if the UI should support richer physical constraints.
- Change aggregation if different battery types should be optimized separately instead of merged into one virtual fleet.

#### `watt_etw/market_import.py`

Handles uploaded market files for the MVP.

It supports:

- CSV files.
- XLSX/XLSM files.
- Simple tables with a timestamp and price column.
- Tables with date plus period/hour/MTU columns.
- HEnEx Results Summary workbooks, including hourly and 15-minute formats.

Key classes:

- `MarketRow`: timestamp, price, and extra market context.
- `ValidationResult`: valid flag, errors, warnings, detected dates, interval, row count, price summary, rows, and original columns.

Important behavior:

- Detects column names using aliases and hints.
- Parses prices with comma or dot decimal separators.
- Preserves extra columns in `MarketRow.extra`.
- Sorts rows by timestamp.
- Warns about skipped invalid rows, duplicate timestamps, and missing intervals.
- For HEnEx summary workbooks, reads XLSX internals directly with `zipfile` and XML parsing, so it does not rely on `openpyxl`.

Where to change:

- Add column aliases if a new data provider uses different names.
- Improve HEnEx workbook parsing if sheet layouts change.
- Add stronger validation for 15-minute products, DST days, or duplicate handling.

#### `watt_etw/optimizer.py`

Creates the battery schedule and KPIs.

Key classes:

- `ScheduleRow`: one row per market interval.
- `OptimizationResult`: status, KPIs, and schedule.

Important behavior:

- Uses a price-threshold heuristic, not a full LP/MILP solver.
- Tracks SOC through time.
- Applies efficiency losses and degradation cost.
- Supports max cycles per day through a throughput limit.
- Creates explanations for each row.
- Restores final SOC if the first-pass schedule ends below initial SOC.

Where to change:

- Replace the heuristic with a linear optimization model if we need globally optimal schedules.
- Add constraints like grid export limits, reserve commitments, imbalance penalties, or forecast uncertainty buffers.
- Improve final SOC repair so later SOC rows are recomputed after inserted charging.

#### `watt_etw/explanations.py`

Small helper that turns optimizer decisions into readable reasons. The frontend shows these sentences in the schedule table.

Where to change:

- Make explanations more specific, for example including threshold values, future peak prices, or SOC headroom.
- Add separate explanations for forecast-driven schedules versus actual-DAM schedules.

### API

#### `watt_etw/api/main.py`

FastAPI app for the current MVP.

Endpoints:

- `GET /api/health`: simple health check.
- `POST /api/market-data/validate`: accepts uploaded CSV/XLSX file, parses it with `load_market_file()`, returns validation output and parsed rows.
- `POST /api/optimize`: receives batteries, market rows, and optional selected date. It builds `BatterySpec`s, aggregates the fleet, filters rows to the selected date, and runs `optimize_schedule()`.
- `POST /api/export-schedule`: receives schedule rows and returns a CSV file.

Where to change:

- Add forecast endpoints here.
- Add persistent storage if users should save projects.
- Add authentication if this becomes more than a local/internal tool.

### Historical data and feature engineering

#### `watt_etw/data/henex_parser.py`

Parses historical HEnEx DAM Results Summary XLSX files into a tidy hourly table.

Output columns include:

- `date`, `hour`
- `mcp_eur_mwh`
- `sell_total_mwh`
- `gas_mwh`
- `hydro_mwh`
- `res_mwh`
- `lignite_mwh`
- `imports_mwh`

Important behavior:

- Uses file names to infer trading dates.
- Reads XLSX internals through XML.
- Assumes `sheet1.xml` is the Results Summary sheet.
- Scans labels like Market Clearing Price, Total Sell Trades, Gas, Hydro, Renewables, Lignite, and Imports.
- Caches parsed results to parquet through `load_or_parse()`.

Where to change:

- Extend parser for 15-minute historical files if needed.
- Add more HEnEx sections as features, such as BESS, demand, exports, or interconnector flows.

#### `watt_etw/data/weather_fetcher.py`

Fetches weather from Open-Meteo and caches it under `data/external/weather/`.

It now supports:

- A single coordinate, originally Athens by default.
- Arbitrary asset coordinates.
- Solar variables such as shortwave radiation, direct normal irradiance, diffuse radiation, and global tilted irradiance.
- Wind variables at multiple heights.
- Atmospheric variables such as temperature, humidity, pressure, cloud cover, visibility, precipitation, snowfall, snow depth, and weather code.

Important functions:

- `fetch()`: fetch weather for one coordinate and date range.
- `fetch_for_assets()`: fetch weather for every asset in an asset DataFrame.
- `fetch_renewable_weather_features()`: fetch per-asset weather and aggregate it by technology.
- `aggregate_by_technology()`: capacity-weighted aggregation by date, hour, and technology.

The aggregation creates columns like:

```text
wind_asset_count
wind_capacity_mw
wind_wind_speed_10m
wind_wind_speed_120m
wind_global_tilted_irradiance
```

Where to change:

- Add or remove Open-Meteo variables.
- Change weighting logic if we prefer generation-weighted, region-weighted, or equal-weighted features.
- Add batching or rate limiting for many asset locations.

#### `watt_etw/data/rae_geoportal.py`

Fetches and normalizes renewable asset locations from the RAE Geoportal WFS service.

Key ideas:

- RAE Geoportal exposes GeoServer vector layers.
- The code fetches layers as GeoJSON.
- Each GeoJSON feature is converted into a `RenewableAsset`.
- For complex geometries, it calculates a simple representative coordinate by averaging coordinate pairs.
- It tries to detect capacity in MW using field-name hints like capacity, power, MW, and Greek words for installed power.

Default layers include:

- Wind projects.
- Wind turbines.
- Hydro.
- Hybrid/island projects.

Where to change:

- Add PV layers when the correct RAE layer names are known.
- Improve representative coordinates using real geometry centroids if we add a geometry library.
- Make capacity detection layer-specific if generic hints are not accurate enough.

#### `watt_etw/data/ttf_fetcher.py`

Fetches Dutch TTF natural gas front-month futures prices from Yahoo Finance via `yfinance`.

Important behavior:

- Uses ticker `TTF=F`.
- Caches one CSV per year under `data/external/ttf_gas/`.
- Fills weekends and holidays by carrying forward/backfilling the nearest available price.
- Returns daily `date`, `ttf_eur_mwh`.

Where to change:

- Replace Yahoo Finance with a more official gas price provider.
- Add additional fuels or carbon prices.
- Add intraday gas data if the model needs it.

#### `watt_etw/features/feature_builder.py`

Joins market, weather, and gas into the feature matrix used for forecasting.

Important behavior:

- Normalizes date columns.
- Sorts prices by date/hour.
- Builds MCP lags: 1h, 2h, 24h, 48h, 168h.
- Builds previous 24-hour rolling mean and standard deviation.
- Adds calendar features.
- Adds Greek holiday flag if the optional `holidays` package is installed.
- Builds TTF lags: 1 day and 7 days.
- Merges prices with weather by date/hour.
- Merges gas by date.
- Preserves extra weather columns, including technology-level renewable weather columns.
- Optionally writes the final DataFrame to parquet.

Where to change:

- Add new model features.
- Change lag windows.
- Add better holiday/calendar logic.
- Add features that are known before the delivery day versus features that are only known after the market clears.

### Forecasting

#### `watt_etw/forecasting/price_forecaster.py`

Trains and uses hourly LightGBM models for MCP forecasting.

Important classes:

- `EvalMetrics`: MAE, RMSE, MAPE, and sample count.
- `ForecastResult`: target date and predictions by hour.
- `PriceForecaster`: model manager for training, prediction, saving, loading, and evaluation.

Important behavior:

- Uses a fixed expected feature list but skips unavailable columns.
- Trains one model per hour.
- Uses the last `test_days` as holdout.
- Saves models as pickle files plus metadata.
- Produces top feature importances per hour internally, though `ForecastResult.to_dict()` currently only returns target date and predictions.

Where to change:

- Add hyperparameter tuning.
- Include prediction intervals or uncertainty.
- Expose feature importance in API responses.
- Train a single global model with hour as a feature if we decide that is simpler.

### Frontend

#### `frontend/src/main.jsx`

React single-page app for the MVP.

Screens:

- Landing screen.
- Fleet setup.
- DAM data import and validation.
- Results dashboard.

Important behavior:

- Stores batteries, validation output, selected date, optimization result, loading state, and errors in React state.
- Uploads files to `/api/market-data/validate`.
- Sends batteries and parsed market rows to `/api/optimize`.
- Exports schedules through `/api/export-schedule`.
- Draws simple SVG line charts for price and SOC.
- Draws bar-style battery actions.
- Shows a schedule table with explanations.

Where to change:

- Add a forecast screen.
- Add richer charting.
- Add editing of battery SOC/degradation/cycle settings.
- Add comparison between actual price schedule and forecasted schedule.

#### `frontend/src/styles.css`

CSS for the React app.

It styles:

- Top navigation.
- Landing section.
- Battery cards.
- Upload dropzone.
- Validation preview.
- KPI tiles.
- Charts.
- Schedule table.
- Mobile layout.

#### `frontend/vite.config.js`

Vite configuration. It runs the React dev server on port `5173` and proxies `/api` calls to the FastAPI backend at `http://127.0.0.1:8000`.

#### `frontend/package.json`

Frontend dependencies and scripts.

Scripts:

- `npm run dev`
- `npm run build`
- `npm run preview`

Dependencies include React, Vite, TypeScript, and Lucide icons.

### Tests

#### `tests/test_battery_fleet.py`

Tests battery aggregation and validation.

It checks:

- Multiple batteries aggregate correctly.
- User-facing field aliases are accepted.
- Invalid batteries are rejected.

#### `tests/test_market_import.py`

Tests uploaded market file parsing.

It checks:

- CSV with timestamp and price.
- Negative prices.
- CSV with date and period.
- Missing price column rejection.
- Missing interval warning.
- Optional real HEnEx files if present locally.
- Repo HEnEx workbook parsing if the data file exists.

#### `tests/test_optimizer.py`

Tests the scheduling heuristic.

It checks:

- Low then high prices produce charge and discharge actions.
- Flat prices do not cycle profitably.
- SOC bounds are respected.
- No row charges and discharges simultaneously.

#### `tests/test_api.py`

Tests FastAPI endpoints.

It checks:

- Validation endpoint accepts a simple CSV.
- Optimize endpoint returns KPIs and schedule.
- Export endpoint returns CSV.

#### `tests/test_renewable_matching.py`

Tests the newer renewable weather matching code.

It checks:

- RAE GeoJSON parsing extracts latitude, longitude, and capacity.
- Technology-level aggregation uses capacity weights.

For example, in the active test, two wind assets with capacities 10 MW and 30 MW are averaged so the larger asset contributes three times as much to the resulting weather feature.

## 7. Current State And Gaps

What is working now:

- Battery fleet validation and aggregation.
- CSV/XLSX DAM upload parsing.
- HEnEx Results Summary parsing for MVP uploads.
- Heuristic charge/discharge scheduling.
- KPIs and explanations.
- CSV schedule export.
- Basic frontend workflow.
- Historical HEnEx parser for hourly feature data.
- Weather and TTF data fetchers.
- Feature matrix builder.
- LightGBM forecasting class.
- New renewable asset/weather matching primitives.

What is not fully integrated yet:

- Forecasting is not exposed in the API.
- Forecasting is not shown in the frontend.
- `PriceForecaster` does not have tests yet.
- Optional dependencies for forecasting/data helpers are not all declared in `pyproject.toml`.
- Renewable asset fetching depends on RAE layer availability and may need PV layer names.
- The optimizer is heuristic, not globally optimal.
- Final SOC repair should be improved if strict schedule consistency is required.

## 8. Where To Make Common Changes

### Change battery inputs

Edit:

- `frontend/src/main.jsx`
- `watt_etw/battery_fleet.py`
- `tests/test_battery_fleet.py`

Example changes:

- Add initial SOC input.
- Add max SOC input.
- Add degradation cost input.
- Add max cycles per day input.

### Change market file parsing

Edit:

- `watt_etw/market_import.py`
- `tests/test_market_import.py`

Example changes:

- Accept another timestamp format.
- Add a new price column alias.
- Parse additional HEnEx workbook sections.

### Change optimization logic

Edit:

- `watt_etw/optimizer.py`
- `watt_etw/explanations.py`
- `tests/test_optimizer.py`

Example changes:

- Make thresholds configurable.
- Add an actual optimization solver.
- Add export/import limits.
- Add penalties for ending away from target SOC.

### Change forecast features

Edit:

- `watt_etw/features/feature_builder.py`
- `watt_etw/forecasting/price_forecaster.py`
- Feature tests, which should be added.

Example changes:

- Add load forecast.
- Add RES forecast.
- Add interconnector flows.
- Add carbon price.
- Add rolling weekly statistics.

### Add forecast endpoint

Edit:

- `watt_etw/api/main.py`
- `watt_etw/forecasting/price_forecaster.py` if response shape changes.
- `frontend/src/main.jsx` to call the endpoint.
- `tests/test_api.py`.

Possible endpoint:

```text
POST /api/forecast
```

Possible response:

```json
{
  "target_date": "2026-04-30",
  "predictions": {
    "0": 92.31,
    "1": 88.44
  }
}
```

### Add forecast-to-optimization flow

The clean approach:

1. Forecast MCP for a date.
2. Convert each hourly prediction into a `MarketRow`.
3. Run `optimize_schedule()` with the forecast rows.
4. Display the schedule as "forecast-based".
5. When actual DAM arrives, upload it and compare actual versus forecast schedule.

## 9. Suggested Next Improvements

Recommended next steps:

1. Add missing optional dependencies explicitly, probably under a `forecasting` optional dependency group.
2. Add tests for `feature_builder.py`.
3. Add tests for `PriceForecaster` using a small synthetic DataFrame.
4. Add an API endpoint to load a trained forecast model and return predictions.
5. Add a frontend forecast page.
6. Improve optimizer final SOC repair or replace the heuristic with a linear optimization model.
7. Decide whether the product should optimize on actual DAM prices, forecasted prices, or both.
8. Add PV asset layer support in `rae_geoportal.py`.

## 10. Mental Model To Keep In Your Head

Think of the project as a chain:

```text
Data -> Features -> Forecast -> Prices -> Battery Schedule -> KPIs and Explanation
```

Right now, the app starts at "Prices" because the user uploads prices directly. The forecasting work builds the earlier part of the chain so the system can eventually produce its own price view before actual prices are uploaded.

The battery suggestion is therefore only as good as the price signal it receives. If prices are actual DAM prices, the output is an ex-post optimal-ish operating suggestion under the heuristic. If prices are forecasts, the output becomes a forward-looking operating plan, and uncertainty becomes important.

The most important design decision ahead is whether we want Watt ETW to be:

- A battery optimizer for already-published DAM prices.
- A forecasting system for future DAM prices.
- A full decision-support system that forecasts prices, optimizes battery schedules, and later compares forecasted decisions against actual outcomes.

The codebase is already pointed toward the third option, but the API and frontend currently expose mainly the first option.
