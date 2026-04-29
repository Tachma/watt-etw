# Battery Quantity Adjustment - Standalone AMPL Test

Run the model directly in AMPL with mocked Greek-style price data — no Python needed.

## Files

| File | Purpose |
|------|---------|
| `battery_qa.mod` | The mathematical model (variables, constraints, objective) |
| `battery_qa.dat` | Mocked input parameters (prices, capacity, efficiency, ramp, etc.) |
| `battery_qa.run` | Run script — loads model + data, solves, prints results |

## How to run

### Option 1: From the command line

```bash
cd /path/to/this/folder
ampl battery_qa.run
```

### Option 2: From the AMPL IDE (interactive)

1. Open the AMPL IDE
2. Set the working directory to this folder: `cd '/path/to/this/folder';`
3. Then: `include battery_qa.run;`

### Option 3: From the AMPL command line (interactive)

```
ampl: model battery_qa.mod;
ampl: data battery_qa.dat;
ampl: option solver gurobi;
ampl: solve;
ampl: display Revenue, c, d, E, z;
```

## Solver

The run script uses **Gurobi** by default. If you don't have a Gurobi license,
edit `battery_qa.run` and switch to a free MILP solver:

```ampl
# option solver gurobi;          # comment out this
option solver cbc;                # use this (free, ships with AMPL CE)
# option solver highs;            # or this (free, faster than cbc)
```

CBC and HiGHS both ship with AMPL Community Edition and handle this MILP
without any license.

## Mocked scenario

- **Battery**: 10 MWh / 4-hour Li-ion (Megapack-class)
- **E_min / E_max**: 1.0 / 10.0 MWh
- **Efficiency**: 92% round-trip
- **Availability**: 98%
- **Ramp**: 20 MW (effectively non-binding)
- **Initial SoC**: 1.0 MWh (start at minimum)
- **Price profile**: Synthetic 24h Greek-style wholesale (~38-180 EUR/MWh)
  - cheapest at 02:00-04:00 → battery should charge here
  - peak at 19:00-20:00 → battery should discharge here

## Expected output

You should see something like:

```
================================================================
  RESULT SUMMARY
================================================================
  Solve status       : solved
  Objective (revenue): 1595.89 EUR
================================================================

  Total charged       :   23.666 MWh
  Total discharged    :   21.773 MWh
  Round-trip loss     :    1.893 MWh  (expected ~ 1.893 for eta=0.92)
  Gross revenue       :  3956.24 EUR
  Charging cost       :  2360.35 EUR
  Net profit          :  1595.89 EUR
  Final SoC           :    1.000 MWh

================================================================
  DETAILED SCHEDULE (active quarters only)
================================================================
  Hr  Q   lambda    charge    dischg       SoC   mode
  -- --  -------  --------  --------  --------  -----
   1  1    43.50     2.450     0.000     3.350    CHG
   ...
   9  1    98.50     0.000     1.282     8.663    DIS
   ...
  20  1   178.50     0.000     1.282     8.663    DIS
  20  4   183.00     0.000     2.450     1.000    DIS
```

## Modifying the inputs

Edit `battery_qa.dat` to test different scenarios:

- **Tighter ramp**: change `param ramp := 20.0;` to `param ramp := 2.0;`
  → battery will charge/discharge more slowly, lower revenue
- **Lower efficiency**: change `param eta := 0.92;` to `param eta := 0.70;`
  → flow batteries / older Li-ion behavior, lower revenue
- **Different price shape**: edit the `param lambda` table — try a flat profile
  (no arbitrage opportunity) and verify the solution is c=d=0 everywhere
- **Lower availability**: `param availability := 0.50;` halves the per-quarter
  energy throughput, simulating a partially-degraded fleet

## Sanity checks the model should pass

1. **Energy conservation**: `Total charged - Total discharged ≈ (1-eta) × Total charged`
2. **Mutex**: no quarter has both `c[h,s] > 0` and `d[h,s] > 0`
3. **Capacity**: `E[h,s]` always between `E_min` and `E_max`
4. **Arbitrage logic**: charging happens at low `lambda`, discharging at high `lambda`
5. **Flat-price test**: if all `lambda[h,s]` are equal, optimal Revenue should be 0
   (no arbitrage, and any cycling would lose energy to round-trip loss)
