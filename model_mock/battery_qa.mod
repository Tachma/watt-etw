# =============================================================================
# Battery Quantity Adjustment - AMPL Model
# =============================================================================
# Maximizes arbitrage revenue of a single battery facing exogenous prices
# lambda[h,s] over a 24-hour horizon with 15-minute resolution.
#
# Indices:
#   h = 1..24   (hour of day)
#   s = 1..4    (quarter within the hour)
#
# Variables:
#   d[h,s]  >= 0   MWh discharged in quarter (h,s)
#   c[h,s]  >= 0   MWh absorbed (charged) in quarter (h,s)
#   E[h,s]  >= 0   State of Charge at end of quarter (h,s)  [MWh]
#   z[h,s]  binary 1 if discharging, 0 if charging
# =============================================================================

# ---------- SETS ----------
set HOURS    := 1..24;
set QUARTERS := 1..4;

# ---------- PARAMETERS ----------
param lambda      {HOURS, QUARTERS};   # price [EUR/MWh]
param E_max       > 0;                  # max capacity [MWh]
param E_min       >= 0;                 # min capacity [MWh]
param eta         >= 0, <= 1;           # round-trip efficiency
param availability >= 0, <= 1;          # availability factor
param ramp        >= 0;                 # ramp rate [MW]
param E0          >= 0;                 # initial SoC [MWh]
param dt          := 0.25;              # quarter-hour time step [h]

# Per-quarter energy throughput limit (MWh) = power_limit_MW * dt
param Q_max := availability * E_max * dt;

# Square root of efficiency for symmetric loss split
param sqrt_eta := sqrt(eta);

# ---------- VARIABLES ----------
var d {HOURS, QUARTERS} >= 0;           # discharge [MWh]
var c {HOURS, QUARTERS} >= 0;           # charge    [MWh]
var E {HOURS, QUARTERS} >= 0;           # state of charge [MWh]
var z {HOURS, QUARTERS} binary;         # 1 = discharging, 0 = charging

# ---------- OBJECTIVE ----------
maximize Revenue:
    sum {h in HOURS, s in QUARTERS} lambda[h,s] * (d[h,s] - c[h,s]);

# ---------- CONSTRAINTS ----------

# State of Charge balance with sqrt(eta) loss split
# For first quarter of first hour, link to initial SoC E0
subject to SoC_first:
    E[1,1] = E0 + sqrt_eta * c[1,1] - d[1,1] / sqrt_eta;

# Within an hour: link s to s-1
subject to SoC_within_hour {h in HOURS, s in QUARTERS: s > 1}:
    E[h,s] = E[h,s-1] + sqrt_eta * c[h,s] - d[h,s] / sqrt_eta;

# Across hours: link (h,1) to (h-1,4)
subject to SoC_across_hour {h in HOURS: h > 1}:
    E[h,1] = E[h-1,4] + sqrt_eta * c[h,1] - d[h,1] / sqrt_eta;

# Capacity bounds
subject to Cap_min {h in HOURS, s in QUARTERS}:
    E[h,s] >= E_min;

subject to Cap_max {h in HOURS, s in QUARTERS}:
    E[h,s] <= E_max;

# Charge upper bound (active only when z=0)
subject to Charge_limit {h in HOURS, s in QUARTERS}:
    c[h,s] <= Q_max * (1 - z[h,s]);

# Discharge upper bound (active only when z=1)
subject to Discharge_limit {h in HOURS, s in QUARTERS}:
    d[h,s] <= Q_max * z[h,s];

# Ramp constraints on charge (in MW: divide MWh by dt)
# Within hour
subject to Ramp_c_up_within {h in HOURS, s in QUARTERS: s > 1}:
    (c[h,s] - c[h,s-1]) / dt <= ramp;
subject to Ramp_c_dn_within {h in HOURS, s in QUARTERS: s > 1}:
    (c[h,s] - c[h,s-1]) / dt >= -ramp;

# Across hours
subject to Ramp_c_up_across {h in HOURS: h > 1}:
    (c[h,1] - c[h-1,4]) / dt <= ramp;
subject to Ramp_c_dn_across {h in HOURS: h > 1}:
    (c[h,1] - c[h-1,4]) / dt >= -ramp;

# Ramp constraints on discharge
subject to Ramp_d_up_within {h in HOURS, s in QUARTERS: s > 1}:
    (d[h,s] - d[h,s-1]) / dt <= ramp;
subject to Ramp_d_dn_within {h in HOURS, s in QUARTERS: s > 1}:
    (d[h,s] - d[h,s-1]) / dt >= -ramp;

subject to Ramp_d_up_across {h in HOURS: h > 1}:
    (d[h,1] - d[h-1,4]) / dt <= ramp;
subject to Ramp_d_dn_across {h in HOURS: h > 1}:
    (d[h,1] - d[h-1,4]) / dt >= -ramp;
