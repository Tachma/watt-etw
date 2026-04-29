"""LightGBM-based 15-minute day-ahead electricity price forecaster.

Single global model with `mtu`/`hour_of_day`/`quarter_of_hour` exposed as
features, rather than 96 per-MTU models. This keeps every training row in
one model so it can learn cross-MTU patterns and avoids data starvation
on individual quarters.

Training:
  - Holds out the last `test_days` of features as a temporal test set.
  - Time-series safe: rows are sorted, never shuffled into the future.
  - Saves to models/price_forecaster/model.pkl + meta.json.

Prediction:
  - Given a target date, looks up that date's 96 feature rows (one per MTU)
    and returns {mtu: predicted_mcp_eur_mwh}.
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

# Core features always included if present. Per-tech RES weather columns
# (wind_*, solar_*, hydro_*, hybrid_*) are picked up dynamically at train()
# so the model uses whatever the asset/weather pipeline produced.
_CORE_FEATURES = [
    # Price history
    "mcp_lag1", "mcp_lag4", "mcp_lag96", "mcp_lag192", "mcp_lag672",
    "mcp_rolling_mean_96", "mcp_rolling_std_96",
    # Supply mix
    "sell_total_mwh", "gas_mwh", "hydro_mwh", "res_mwh", "lignite_mwh", "imports_mwh",
    # Calendar
    "day_of_week", "month", "hour_of_day", "quarter_of_hour", "mtu_of_day",
    "is_weekend", "is_holiday_gr",
    # Athens baseline weather
    "temperature_2m", "shortwave_radiation", "wind_speed_10m",
    "cloud_cover", "relative_humidity_2m", "precipitation",
    "wind_speed_80m", "wind_speed_120m", "direct_normal_irradiance",
    "diffuse_radiation", "global_tilted_irradiance",
    # Gas
    "ttf_eur_mwh", "ttf_lag1d", "ttf_lag7d",
    # Carbon
    "eua_eur_t", "eua_lag1d", "eua_lag7d",
    # ADMIE forecasts
    "load_forecast_mw", "res_forecast_mw",
    "net_load_forecast_mw", "load_res_ratio",
    # ENTSO-E outage proxies (planned + forced unavailable MW)
    "mw_unavailable_planned", "mw_unavailable_forced", "mw_unavailable_total",
    # Peak-hour helpers
    "temp_dev_from_climatology", "net_load_vs_daily_max", "mcp_range_24h",
]
_RES_FEATURE_PREFIXES = ("wind_", "solar_", "hydro_", "hybrid_", "wind_turbine_")
_TARGET_COL = "mcp_eur_mwh"
_KEY_COLS = {"date", "mtu", "hour", "quarter", _TARGET_COL}

_DEFAULT_LGB_PARAMS: dict[str, Any] = {
    "objective": "regression",
    "metric": "rmse",
    "learning_rate": 0.04,
    "num_leaves": 127,
    "min_child_samples": 50,
    "feature_fraction": 0.85,
    "bagging_fraction": 0.85,
    "bagging_freq": 5,
    "lambda_l1": 0.1,
    "lambda_l2": 0.1,
    "verbose": -1,
    "n_estimators": 1500,
}


@dataclass
class EvalMetrics:
    mae: float
    rmse: float
    mape: float
    n_samples: int

    def to_dict(self) -> dict:
        return {"mae": self.mae, "rmse": self.rmse, "mape": self.mape,
                "n_samples": self.n_samples}


@dataclass
class ForecastResult:
    target_date: date
    predictions: dict[int, float]   # {mtu: mcp_eur_mwh}
    feature_importance: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "target_date": self.target_date.isoformat(),
            "predictions": {str(k): v for k, v in self.predictions.items()},
        }


class PriceForecaster:
    """Single global LightGBM for 15-min MCP forecasting."""

    def __init__(
        self,
        model_dir: str | Path = "models/price_forecaster",
        test_days: int = 30,
        lgb_params: dict[str, Any] | None = None,
        peak_hours: tuple[int, ...] = (16, 17, 18, 19),
        peak_weight: float = 1.0,
    ):
        self.model_dir = Path(model_dir)
        self.test_days = test_days
        self.lgb_params = lgb_params or _DEFAULT_LGB_PARAMS.copy()
        # peak_weight=1.0 disables weighting (default). Set >1.0 (e.g. 3.0)
        # to bias the fit toward evening-peak rows.
        self.peak_hours = tuple(peak_hours)
        self.peak_weight = float(peak_weight)
        self._model: Any = None
        self._feature_cols: list[str] = []

    # ------------------------------------------------------------------ #
    # Feature selection
    # ------------------------------------------------------------------ #

    def _select_features(self, df: pd.DataFrame) -> list[str]:
        core_present = [c for c in _CORE_FEATURES if c in df.columns]
        res_present = [
            c for c in df.columns
            if c.startswith(_RES_FEATURE_PREFIXES)
            and c not in core_present
            and c not in _KEY_COLS
        ]
        missing = set(_CORE_FEATURES) - set(core_present)
        if missing:
            logger.warning("Missing core features (skipped): %s", sorted(missing))
        if res_present:
            logger.info("Using %d per-tech RES weather features", len(res_present))
        return core_present + res_present

    # ------------------------------------------------------------------ #
    # Training
    # ------------------------------------------------------------------ #

    def train(self, features_df: pd.DataFrame) -> EvalMetrics:
        try:
            import lightgbm as lgb
        except ImportError as e:
            raise ImportError("lightgbm is required: pip install lightgbm") from e

        df = features_df.copy()
        df["date"] = pd.to_datetime(df["date"])
        if "mtu" not in df.columns:
            raise ValueError("features_df must contain `mtu` (15-min schema)")
        df = df.sort_values(["date", "mtu"]).reset_index(drop=True)

        candidate_cols = self._select_features(df)
        if not candidate_cols:
            raise ValueError("No usable feature columns in features_df")

        # Drop columns that are 100% NaN (typically failed weather fetches).
        # LightGBM tolerates partial NaN natively, but a fully-missing column
        # is wasted dimensionality.
        all_nan = [c for c in candidate_cols if df[c].isna().all()]
        if all_nan:
            logger.warning("Dropping %d all-NaN feature columns: %s",
                            len(all_nan), all_nan[:8] + (["…"] if len(all_nan) > 8 else []))
        self._feature_cols = [c for c in candidate_cols if c not in all_nan]

        # Train/test split on dates (never shuffle time series). Drop only on
        # the target — let LightGBM handle missing values inside features.
        cutoff_date = df["date"].max() - pd.Timedelta(days=self.test_days)
        train_df = df[df["date"] <= cutoff_date].dropna(subset=[_TARGET_COL])
        test_df = df[df["date"] > cutoff_date].dropna(subset=[_TARGET_COL])

        logger.info(
            "Train: %d rows (%s → %s), Test: %d rows (%s → %s)",
            len(train_df),
            train_df["date"].min().date() if not train_df.empty else None,
            train_df["date"].max().date() if not train_df.empty else None,
            len(test_df),
            test_df["date"].min().date() if not test_df.empty else None,
            test_df["date"].max().date() if not test_df.empty else None,
        )

        if len(train_df) < 1000:
            raise ValueError(f"Too few training rows: {len(train_df)}")

        X_tr = train_df[self._feature_cols].values
        y_tr = train_df[_TARGET_COL].values
        X_te = test_df[self._feature_cols].values
        y_te = test_df[_TARGET_COL].values

        # Optional sample weighting: bias the fit toward peak hours where
        # MAE is largest. peak_weight==1.0 → no change.
        weights = None
        if self.peak_weight != 1.0 and "hour" in train_df.columns:
            is_peak = train_df["hour"].isin(self.peak_hours).values
            weights = np.where(is_peak, self.peak_weight, 1.0)
            logger.info(
                "Peak weighting: %d/%d rows in peak hours %s @ %.2f×",
                int(is_peak.sum()), len(is_peak), self.peak_hours, self.peak_weight,
            )

        model = lgb.LGBMRegressor(**self.lgb_params)
        model.fit(
            X_tr, y_tr,
            sample_weight=weights,
            eval_set=[(X_te, y_te)] if len(X_te) > 0 else None,
            callbacks=[lgb.early_stopping(75, verbose=False), lgb.log_evaluation(0)]
            if len(X_te) > 0 else [lgb.log_evaluation(0)],
        )
        self._model = model

        # Holdout metrics
        if len(y_te) > 0:
            preds = model.predict(X_te)
            mae = float(np.mean(np.abs(preds - y_te)))
            rmse = float(np.sqrt(np.mean((preds - y_te) ** 2)))
            meaningful = np.abs(y_te) >= 5
            mape = (
                float(np.mean(np.abs((preds[meaningful] - y_te[meaningful])
                                      / y_te[meaningful])) * 100)
                if meaningful.any() else float("nan")
            )
            n = len(y_te)
        else:
            mae = rmse = mape = float("nan")
            n = 0

        logger.info("Holdout — MAE=%.2f  RMSE=%.2f  MAPE=%.1f%%  (n=%d)",
                    mae, rmse, mape, n)
        return EvalMetrics(mae=mae, rmse=rmse, mape=mape, n_samples=n)

    # ------------------------------------------------------------------ #
    # Prediction
    # ------------------------------------------------------------------ #

    def predict(self, features_df: pd.DataFrame, target_date: date) -> ForecastResult:
        """Predict MCP for all 96 MTUs of `target_date`."""
        if self._model is None:
            raise RuntimeError("No model loaded. Call train() or load() first.")

        df = features_df.copy()
        df["date"] = pd.to_datetime(df["date"])
        day = df[df["date"].dt.date == target_date].sort_values("mtu")

        cols = self._feature_cols
        available = [c for c in cols if c in day.columns]

        predictions: dict[int, float] = {}
        if day.empty:
            for mtu in range(96):
                predictions[mtu] = float("nan")
            return ForecastResult(target_date=target_date, predictions=predictions)

        X = day[available].fillna(0).values
        preds = self._model.predict(X)

        for mtu, pred in zip(day["mtu"].astype(int).values, preds):
            predictions[int(mtu)] = round(max(float(pred), 0.0), 4)

        # Top 20 feature importances by gain
        imp = dict(zip(available, self._model.feature_importances_))
        top = dict(sorted(imp.items(), key=lambda x: -x[1])[:20])
        feature_importance = {k: round(float(v), 2) for k, v in top.items()}

        return ForecastResult(
            target_date=target_date,
            predictions=predictions,
            feature_importance=feature_importance,
        )

    # ------------------------------------------------------------------ #
    # Persist
    # ------------------------------------------------------------------ #

    def save(self, model_dir: str | Path | None = None) -> None:
        d = Path(model_dir or self.model_dir)
        d.mkdir(parents=True, exist_ok=True)

        meta = {
            "feature_cols": self._feature_cols,
            "lgb_params": self.lgb_params,
            "peak_hours": list(self.peak_hours),
            "peak_weight": self.peak_weight,
            "schema": "15min",
        }
        (d / "meta.json").write_text(json.dumps(meta, indent=2))

        with open(d / "model.pkl", "wb") as fh:
            pickle.dump(self._model, fh)

        # Clear out stale per-hour pickles from the legacy hourly model.
        for stale in d.glob("hour_*.pkl"):
            try:
                stale.unlink()
            except OSError:
                pass

        logger.info("Saved model to %s", d)

    def load(self, model_dir: str | Path | None = None) -> None:
        d = Path(model_dir or self.model_dir)
        meta_path = d / "meta.json"
        model_path = d / "model.pkl"
        if not meta_path.exists() or not model_path.exists():
            raise FileNotFoundError(
                f"Model artifacts missing at {d} (expected meta.json + model.pkl)"
            )

        meta = json.loads(meta_path.read_text())
        if meta.get("schema") != "15min":
            raise ValueError(
                f"Saved model schema is {meta.get('schema')!r}, expected '15min'. "
                "Retrain with the current pipeline."
            )
        self._feature_cols = meta.get("feature_cols", [])
        self.peak_hours = tuple(meta.get("peak_hours", self.peak_hours))
        self.peak_weight = float(meta.get("peak_weight", self.peak_weight))

        with open(model_path, "rb") as fh:
            self._model = pickle.load(fh)

        logger.info("Loaded model from %s (%d features)", d, len(self._feature_cols))

    # ------------------------------------------------------------------ #
    # Evaluate
    # ------------------------------------------------------------------ #

    def evaluate(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Run predictions on the last `test_days` of features_df."""
        if self._model is None:
            raise RuntimeError("No model loaded. Call train() or load() first.")

        df = features_df.copy()
        df["date"] = pd.to_datetime(df["date"])
        cutoff = df["date"].max() - pd.Timedelta(days=self.test_days)
        test_df = df[df["date"] > cutoff].copy()
        test_df = test_df.dropna(subset=[_TARGET_COL])

        cols = self._feature_cols
        available = [c for c in cols if c in test_df.columns]
        X = test_df[available].fillna(0).values
        preds = self._model.predict(X)

        out = pd.DataFrame({
            "date": test_df["date"].dt.date.values,
            "mtu": test_df["mtu"].astype(int).values,
            "actual": test_df[_TARGET_COL].values,
            "predicted": np.maximum(preds, 0.0).round(4),
        })
        out["error"] = (out["predicted"] - out["actual"]).round(4)
        out["abs_error"] = out["error"].abs()
        return out
