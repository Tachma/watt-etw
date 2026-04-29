"""LightGBM-based day-ahead electricity price forecaster.

One LightGBM model is trained per delivery hour (0-23), capturing the
distinct intraday price patterns (e.g. solar valley at noon, evening ramp).

Training:
  - Uses all rows in the feature matrix except the last `test_days` (held out).
  - TimeSeriesSplit cross-validation for hyper-parameter selection.
  - Models saved as JSON to models/price_forecaster/hour_{h}.lgb.

Prediction:
  - Given a target date, look up that date's feature row for each hour.
  - Returns {hour: predicted_mcp_eur_mwh} for hours 0-23.
"""
from __future__ import annotations

import json
import logging
import pickle
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Core features used for training. Per-technology RES weather columns
# (e.g. wind_wind_speed_120m) are picked up dynamically at train() time
# via _RES_FEATURE_PREFIXES so the model uses whatever the asset/weather
# pipeline produced without a hard-coded list.
_FEATURE_COLS = [
    "mcp_lag1h", "mcp_lag2h", "mcp_lag24h", "mcp_lag48h", "mcp_lag168h",
    "mcp_rolling_mean_24h", "mcp_rolling_std_24h",
    "sell_total_mwh", "gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh", "imports_mwh",
    "day_of_week", "month", "hour_of_day", "is_weekend", "is_holiday_gr",
    "temperature_2m", "shortwave_radiation", "wind_speed_10m",
    "cloud_cover", "relative_humidity_2m", "precipitation",
    "ttf_eur_mwh", "ttf_lag1d", "ttf_lag7d",
    "eua_eur_t", "eua_lag1d", "eua_lag7d",
]
_RES_FEATURE_PREFIXES = ("wind_", "solar_", "hydro_", "hybrid_", "wind_turbine_")
_TARGET_COL = "mcp_eur_mwh"

_DEFAULT_LGB_PARAMS: dict[str, Any] = {
    "objective": "regression",
    "metric": "rmse",
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_child_samples": 20,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "lambda_l1": 0.1,
    "lambda_l2": 0.1,
    "verbose": -1,
    "n_estimators": 500,
}


@dataclass
class EvalMetrics:
    mae: float
    rmse: float
    mape: float
    n_samples: int

    def to_dict(self) -> dict:
        return {"mae": self.mae, "rmse": self.rmse, "mape": self.mape, "n_samples": self.n_samples}


@dataclass
class ForecastResult:
    target_date: date
    predictions: dict[int, float]          # {hour: mcp_eur_mwh}
    feature_importance: dict[int, dict[str, float]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "target_date": self.target_date.isoformat(),
            "predictions": self.predictions,
        }


class PriceForecaster:
    """24 per-hour LightGBM models for day-ahead MCP forecasting."""

    def __init__(
        self,
        model_dir: str | Path = "models/price_forecaster",
        test_days: int = 30,
        lgb_params: dict[str, Any] | None = None,
    ):
        self.model_dir = Path(model_dir)
        self.test_days = test_days
        self.lgb_params = lgb_params or _DEFAULT_LGB_PARAMS.copy()
        self._models: dict[int, Any] = {}  # hour → fitted LGBMRegressor
        self._feature_cols: list[str] = []

    # ------------------------------------------------------------------ #
    # Training                                                             #
    # ------------------------------------------------------------------ #

    def train(self, features_df: pd.DataFrame) -> dict[int, EvalMetrics]:
        """Train one model per hour. Returns test-set metrics per hour."""
        try:
            import lightgbm as lgb
        except ImportError as e:
            raise ImportError("lightgbm is required: pip install lightgbm") from e

        df = features_df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["date", "hour"]).reset_index(drop=True)

        # Determine which feature columns are actually present.
        # Includes the explicit core list plus any RES-prefixed columns produced
        # by weather_fetcher.fetch_renewable_weather_features().
        core_present = [c for c in _FEATURE_COLS if c in df.columns]
        res_present = [
            c for c in df.columns
            if c.startswith(_RES_FEATURE_PREFIXES) and c not in core_present
            and c not in {"date", "hour", _TARGET_COL}
        ]
        self._feature_cols = core_present + res_present
        missing = set(_FEATURE_COLS) - set(core_present)
        if missing:
            logger.warning("Missing feature columns (will be skipped): %s", missing)
        if res_present:
            logger.info("Using %d per-tech RES weather features", len(res_present))

        # Train/test split on dates (never shuffle time series)
        cutoff_date = df["date"].max() - pd.Timedelta(days=self.test_days)
        train_df = df[df["date"] <= cutoff_date]
        test_df = df[df["date"] > cutoff_date]

        logger.info(
            "Training on %d rows (%s → %s), testing on %d rows",
            len(train_df), train_df["date"].min().date(), train_df["date"].max().date(),
            len(test_df),
        )

        metrics: dict[int, EvalMetrics] = {}

        for hour in range(24):
            tr = train_df[train_df["hour"] == hour].dropna(
                subset=self._feature_cols + [_TARGET_COL]
            )
            te = test_df[test_df["hour"] == hour].dropna(
                subset=self._feature_cols + [_TARGET_COL]
            )

            X_tr, y_tr = tr[self._feature_cols].values, tr[_TARGET_COL].values
            X_te, y_te = te[self._feature_cols].values, te[_TARGET_COL].values

            if len(X_tr) < 30:
                logger.warning("Hour %d: only %d training samples, skipping", hour, len(X_tr))
                continue

            model = lgb.LGBMRegressor(**self.lgb_params)
            model.fit(
                X_tr, y_tr,
                eval_set=[(X_te, y_te)] if len(X_te) > 0 else None,
                callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)]
                if len(X_te) > 0 else [lgb.log_evaluation(-1)],
            )
            self._models[hour] = model

            if len(y_te) > 0:
                preds = model.predict(X_te)
                mae = float(np.mean(np.abs(preds - y_te)))
                rmse = float(np.sqrt(np.mean((preds - y_te) ** 2)))
                # MAPE: exclude prices < 5 EUR/MWh to avoid near-zero division
                meaningful = np.abs(y_te) >= 5
                if meaningful.any():
                    mape = float(np.mean(np.abs((preds[meaningful] - y_te[meaningful]) / y_te[meaningful])) * 100)
                else:
                    mape = float("nan")
                metrics[hour] = EvalMetrics(mae=mae, rmse=rmse, mape=mape, n_samples=len(y_te))
                logger.info("Hour %02d → MAE=%.2f  RMSE=%.2f  MAPE=%.1f%%", hour, mae, rmse, mape)
            else:
                metrics[hour] = EvalMetrics(mae=float("nan"), rmse=float("nan"), mape=float("nan"), n_samples=0)

        logger.info("Training complete. %d models trained.", len(self._models))
        return metrics

    # ------------------------------------------------------------------ #
    # Prediction                                                           #
    # ------------------------------------------------------------------ #

    def predict(self, features_df: pd.DataFrame, target_date: date) -> ForecastResult:
        """Predict MCP for all 24 hours of `target_date`.

        `features_df` must contain a row for each hour of target_date with
        all required feature columns already populated (lags, weather, TTF).
        """
        if not self._models:
            raise RuntimeError("No models loaded. Call train() or load() first.")

        df = features_df.copy()
        df["date"] = pd.to_datetime(df["date"])
        day = df[df["date"].dt.date == target_date]

        predictions: dict[int, float] = {}
        importance: dict[int, dict[str, float]] = {}

        for hour in range(24):
            row = day[day["hour"] == hour]
            if hour not in self._models:
                predictions[hour] = float("nan")
                continue

            model = self._models[hour]
            cols = self._feature_cols or _FEATURE_COLS
            available = [c for c in cols if c in row.columns]

            if row.empty or row[available].isna().all(axis=None):
                predictions[hour] = float("nan")
                continue

            X = row[available].fillna(0).values
            pred = float(model.predict(X)[0])
            predictions[hour] = round(max(pred, 0.0), 4)

            # Feature importance (gain)
            imp = dict(zip(available, model.feature_importances_))
            importance[hour] = {k: round(float(v), 2) for k, v in
                                 sorted(imp.items(), key=lambda x: -x[1])[:10]}

        return ForecastResult(
            target_date=target_date,
            predictions=predictions,
            feature_importance=importance,
        )

    # ------------------------------------------------------------------ #
    # Persist                                                              #
    # ------------------------------------------------------------------ #

    def save(self, model_dir: str | Path | None = None) -> None:
        """Save all per-hour models and metadata to disk."""
        d = Path(model_dir or self.model_dir)
        d.mkdir(parents=True, exist_ok=True)

        meta = {
            "feature_cols": self._feature_cols,
            "hours_trained": sorted(self._models.keys()),
            "lgb_params": self.lgb_params,
        }
        (d / "meta.json").write_text(json.dumps(meta, indent=2))

        for hour, model in self._models.items():
            with open(d / f"hour_{hour:02d}.pkl", "wb") as fh:
                pickle.dump(model, fh)

        logger.info("Saved %d models to %s", len(self._models), d)

    def load(self, model_dir: str | Path | None = None) -> None:
        """Load per-hour models from disk."""
        d = Path(model_dir or self.model_dir)
        meta_path = d / "meta.json"
        if not meta_path.exists():
            raise FileNotFoundError(f"No model metadata at {meta_path}")

        meta = json.loads(meta_path.read_text())
        self._feature_cols = meta.get("feature_cols", _FEATURE_COLS)

        self._models = {}
        for hour in meta.get("hours_trained", range(24)):
            pkl = d / f"hour_{hour:02d}.pkl"
            if pkl.exists():
                with open(pkl, "rb") as fh:
                    self._models[hour] = pickle.load(fh)

        logger.info("Loaded %d models from %s", len(self._models), d)

    # ------------------------------------------------------------------ #
    # Evaluate                                                             #
    # ------------------------------------------------------------------ #

    def evaluate(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Run predictions on the last `test_days` of features_df.

        Returns a DataFrame with actual vs. predicted MCP per date/hour,
        plus error metrics.
        """
        df = features_df.copy()
        df["date"] = pd.to_datetime(df["date"])
        cutoff = df["date"].max() - pd.Timedelta(days=self.test_days)
        test_df = df[df["date"] > cutoff].copy()

        rows = []
        for _, row in test_df.iterrows():
            hour = int(row["hour"])
            if hour not in self._models:
                continue
            model = self._models[hour]
            cols = self._feature_cols or _FEATURE_COLS
            available = [c for c in cols if c in test_df.columns]
            X = row[available].fillna(0).values.reshape(1, -1)
            pred = float(model.predict(X)[0])
            rows.append({
                "date": row["date"].date(),
                "hour": hour,
                "actual": row[_TARGET_COL],
                "predicted": round(max(pred, 0.0), 4),
                "error": round(pred - row[_TARGET_COL], 4),
                "abs_error": round(abs(pred - row[_TARGET_COL]), 4),
            })

        return pd.DataFrame(rows)
