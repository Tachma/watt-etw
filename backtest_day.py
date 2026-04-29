"""End-to-end backtest for a single day.

Picks a random day from the feature matrix, forecasts lambda with the trained
LightGBM model (withholding the actual price), runs the MILP optimizer on
those predicted prices, then compares predicted vs actual clearing prices and
prints the resulting dispatch plan.

Usage:
    python backtest_day.py                        # random day from full dataset
    python backtest_day.py --date 2025-11-15      # specific day
    python backtest_day.py --from-test            # random day from held-out test set
    python backtest_day.py --capacity 100 --ramp 50  # custom battery
"""
from __future__ import annotations

import argparse
import random
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

FEATURES_CACHE = "data/processed/features.parquet"
MODEL_DIR      = "models/price_forecaster"
TEST_DAYS      = 30   # must match train_forecaster.py


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-day backtest")
    parser.add_argument("--date", help="ISO date to backtest (default: random)")
    parser.add_argument("--from-test", action="store_true",
                        help="Pick random day from held-out test window only")
    parser.add_argument("--capacity",    type=float, default=50.0,  help="Battery capacity MWh")
    parser.add_argument("--min-capacity",type=float, default=5.0,   help="Min SOC MWh")
    parser.add_argument("--efficiency",  type=float, default=0.90,  help="Round-trip efficiency")
    parser.add_argument("--availability",type=float, default=1.00,  help="Availability 0-1")
    parser.add_argument("--ramp",        type=float, default=25.0,  help="Ramp MW")
    args = parser.parse_args()

    # ------------------------------------------------------------------ #
    # 1. Load features                                                     #
    # ------------------------------------------------------------------ #
    if not Path(FEATURES_CACHE).exists():
        sys.exit(f"[ERROR] Feature cache not found: {FEATURES_CACHE}\n"
                 "        Run: python train_forecaster.py")
    if not (Path(MODEL_DIR) / "model.pkl").exists():
        sys.exit(f"[ERROR] Model not found at {MODEL_DIR}\n"
                 "        Run: python train_forecaster.py")

    print(f"Loading features from {FEATURES_CACHE} ...", flush=True)
    features = pd.read_parquet(FEATURES_CACHE)
    features["date"] = pd.to_datetime(features["date"])

    # ------------------------------------------------------------------ #
    # 2. Choose target day                                                 #
    # ------------------------------------------------------------------ #
    all_days = sorted(features["date"].dt.date.unique())
    max_date = max(all_days)
    cutoff   = max_date - pd.Timedelta(days=TEST_DAYS).to_pytimedelta()

    if args.date:
        target = date.fromisoformat(args.date)
        if target not in all_days:
            sys.exit(f"[ERROR] {target} not in feature matrix (range: {all_days[0]} - {all_days[-1]})")
    elif args.from_test:
        pool = [d for d in all_days if d > cutoff]
        if not pool:
            sys.exit("[ERROR] No test days found.")
        target = random.choice(pool)
    else:
        # Exclude the very first ~7 days (lag features are stale there)
        pool = [d for d in all_days if d > all_days[6]]
        target = random.choice(pool)

    in_test = target > cutoff
    print(f"\nTarget day : {target}  ({'held-out test set' if in_test else 'training window'})")

    # ------------------------------------------------------------------ #
    # 3. Forecast prices (lambda)                                          #
    # ------------------------------------------------------------------ #
    from watt_etw.forecasting.price_forecaster import PriceForecaster

    fc = PriceForecaster(model_dir=MODEL_DIR, test_days=TEST_DAYS)
    fc.load()

    result = fc.predict(features, target)
    predicted_prices = [result.predictions.get(mtu, float("nan")) for mtu in range(96)]

    # ------------------------------------------------------------------ #
    # 4. Actual prices for comparison                                      #
    # ------------------------------------------------------------------ #
    day_rows = features[features["date"].dt.date == target].sort_values("mtu")
    actual_prices = day_rows["mcp_eur_mwh"].tolist()

    if len(actual_prices) != 96:
        print(f"[WARN] Only {len(actual_prices)} rows for {target} — padding with NaN")
        actual_prices += [float("nan")] * (96 - len(actual_prices))

    # ------------------------------------------------------------------ #
    # 5. Forecast accuracy                                                 #
    # ------------------------------------------------------------------ #
    pred_arr   = np.array(predicted_prices, dtype=float)
    actual_arr = np.array(actual_prices,    dtype=float)
    valid      = ~np.isnan(pred_arr) & ~np.isnan(actual_arr)

    mae  = float(np.mean(np.abs(pred_arr[valid] - actual_arr[valid])))
    rmse = float(np.sqrt(np.mean((pred_arr[valid] - actual_arr[valid]) ** 2)))
    meaningful = np.abs(actual_arr[valid]) >= 5
    mape = (float(np.mean(np.abs((pred_arr[valid][meaningful]
                                  - actual_arr[valid][meaningful])
                                  / actual_arr[valid][meaningful])) * 100)
            if meaningful.any() else float("nan"))

    print(f"\n{'-'*58}")
    print(f"  Forecast accuracy for {target}")
    print(f"{'-'*58}")
    print(f"  MAE  : {mae:.2f} EUR/MWh")
    print(f"  RMSE : {rmse:.2f} EUR/MWh")
    print(f"  MAPE : {mape:.1f}%  (MTUs with |price| >= 5 EUR/MWh)")
    print(f"  Predicted range : {pred_arr[valid].min():.1f} - {pred_arr[valid].max():.1f} EUR/MWh")
    print(f"  Actual range    : {actual_arr[valid].min():.1f} - {actual_arr[valid].max():.1f} EUR/MWh")

    # ------------------------------------------------------------------ #
    # 6. Run MILP optimizer on predicted prices                            #
    # ------------------------------------------------------------------ #
    from watt_etw.battery_fleet import AggregatedFleet
    from watt_etw.milp_optimizer import optimize_fleet

    fleet = AggregatedFleet(
        name                  = "Backtest Battery",
        capacity_mwh          = args.capacity,
        power_mw              = args.ramp,
        round_trip_efficiency = args.efficiency,
        availability_pct      = args.availability * 100,
        ramp_mw               = args.ramp,
        initial_soc_mwh       = args.capacity * 0.5,
        min_soc_mwh           = args.min_capacity,
        max_soc_mwh           = args.capacity,
        degradation_cost_eur_mwh = 0.0,
        max_cycles_per_day    = None,
        battery_count         = 1,
    )

    print(f"\n{'-'*58}")
    print(f"  Battery: {args.capacity} MWh | ramp {args.ramp} MW | "
          f"eff {args.efficiency:.0%} | avail {args.availability:.0%}")
    print(f"{'-'*58}")
    print("  Running MILP optimizer on predicted prices ...", flush=True)

    opt = optimize_fleet(fleet, predicted_prices)

    print(f"  Optimizer status : {opt.status}")
    print(f"  Revenue (predicted prices) : {opt.revenue_eur:+.2f} EUR")

    # Revenue if we had used actual prices
    actual_revenue = sum(
        row.lambda_eur_mwh * (row.discharge_mwh - row.charge_mwh)
        for row in opt.schedule
        for ap in [actual_arr[(row.hour - 1) * 4 + (row.quarter - 1)]]
        if not np.isnan(ap)
    )
    # Re-evaluate the same schedule against actual clearing prices
    actual_revenue_reeval = 0.0
    for row in opt.schedule:
        idx = (row.hour - 1) * 4 + (row.quarter - 1)
        actual_lam = actual_arr[idx] if not np.isnan(actual_arr[idx]) else row.lambda_eur_mwh
        actual_revenue_reeval += actual_lam * (row.discharge_mwh - row.charge_mwh)

    print(f"  Revenue (actual  prices)   : {actual_revenue_reeval:+.2f} EUR")

    # ------------------------------------------------------------------ #
    # 7. Print schedule                                                    #
    # ------------------------------------------------------------------ #
    print(f"\n{'-'*58}")
    print(f"  {'Time':>5}  {'Pred €':>7}  {'Real €':>7}  {'Action':>10}  "
          f"{'Chg MW':>7}  {'Dis MW':>7}  {'SOC MWh':>8}")
    print(f"{'-'*58}")

    for row in opt.schedule:
        idx = (row.hour - 1) * 4 + (row.quarter - 1)
        h   = (idx * 15) // 60
        m   = (idx * 15) % 60
        ts  = f"{h:02d}:{m:02d}"
        ap  = actual_arr[idx]
        ap_str = f"{ap:7.2f}" if not np.isnan(ap) else "    n/a"

        charge_mw    = round(row.charge_mwh / 0.25, 2)
        discharge_mw = round(row.discharge_mwh / 0.25, 2)

        if row.is_discharging:
            action = "DISCHARGE"
        elif row.charge_mwh > 1e-4:
            action = "CHARGE"
        else:
            action = "hold"

        print(f"  {ts:>5}  {row.lambda_eur_mwh:7.2f}  {ap_str}  "
              f"{action:>10}  {charge_mw:7.2f}  {discharge_mw:7.2f}  {row.soc_mwh:8.2f}")

    print(f"{'-'*58}")
    print(f"  KPIs  total charged   : {opt.kpis['total_charged_mwh']:.2f} MWh")
    print(f"        total discharged: {opt.kpis['total_discharged_mwh']:.2f} MWh")
    print(f"        final SOC       : {opt.kpis['final_soc_mwh']:.2f} MWh")


if __name__ == "__main__":
    main()
