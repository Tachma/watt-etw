import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import {
  BatteryCharging,
  MousePointer2,
  Play,
  Plus,
  Trash2
} from "lucide-react";
import "./styles.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
const HOUR_LABELS = Array.from({ length: 24 }, (_, hour) => `${String(hour).padStart(2, "0")}:00`);
const INTERVAL_HOURS = 0.25;
const DEFAULT_MIN_DISCHARGE_PRICE = 3;

async function parseJsonResponse(response, fallbackMessage) {
  const text = await response.text();
  if (!text) {
    if (response.ok) return {};
    throw new Error(
      `${fallbackMessage} (HTTP ${response.status}${response.statusText ? ` ${response.statusText}` : ""}). ` +
      `Is the backend running on port 8000? Run: uvicorn watt_etw.api.main:app --reload`
    );
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`${fallbackMessage}: server returned non-JSON body — ${text.slice(0, 200)}`);
  }
}

const defaultBattery = (index = 1) => ({
  name: `Battery ${index}`,
  capacity: 50,
  min_capacity: 5,
  efficiency: 0.9,
  availability: 100,
  ramp: 25
});

const defaultEconomicsInputs = {
  capex_per_mwh_energy: 300000,
  realization_ratio: 0.75,
  availability: 0.97,
  annual_degradation: 0.02,
  opex_fixed_pct: 0.02,
  opex_var_eur_per_mwh: 2,
  augmentation_pct: 0.012,
  wacc: 0.08,
  lifetime_years: 12,
  salvage_pct: 0.07,
  tax_rate: 0.24,
  depreciation_years: 10,
  grant_eur: 0,
  grid_connection_eur: 0
};

function App() {
  const [step, setStep] = useState("landing");
  const [batteries, setBatteries] = useState([defaultBattery()]);
  const [result, setResult] = useState(null);
  const [econResult, setEconResult] = useState(null);
  const [econInputs, setEconInputs] = useState(defaultEconomicsInputs);
  const [econBusy, setEconBusy] = useState(false);
  const [econError, setEconError] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function loadEconomics(overrides) {
    if (!result) return;
    setEconBusy(true);
    setEconError("");
    const inputs = { ...econInputs, ...(overrides || {}) };
    setEconInputs(inputs);

    const totalEnergy = batteries.reduce((sum, b) => sum + Number(b.capacity || 0), 0);
    const totalPower = batteries.reduce((sum, b) => sum + Number(b.ramp || 0), 0);
    const k = result.kpis || {};

    try {
      const response = await fetch(`${API_BASE}/api/economics`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          energy_capacity_mwh: totalEnergy,
          power_capacity_mw: totalPower,
          daily_revenue_eur: Number(k.expected_profit ?? result.revenue_eur ?? 0),
          daily_throughput_mwh: Number(k.total_discharged_mwh ?? 0),
          ...inputs
        })
      });
      const data = await parseJsonResponse(response, "Economics calculation failed");
      if (!response.ok) throw new Error(data.detail || "Economics calculation failed.");
      setEconResult(data);
    } catch (err) {
      setEconError(err.message || "Economics calculation failed.");
    } finally {
      setEconBusy(false);
    }
  }

  async function optimize() {
    setBusy(true);
    setError("");
    try {
      const batteryPayload = batteries.map((battery, index) =>
        normalizeBatteryPayload({ ...defaultBattery(index + 1), ...battery })
      );

      const response = await fetch(`${API_BASE}/api/optimize-arbitrage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          batteries: batteryPayload,
          date: "2026-04-29"
        })
      });
      const data = await parseJsonResponse(response, "Optimization failed");
      if (!response.ok) throw new Error(data.detail || "Optimization failed.");
      setResult(data);
      setEconResult(null);
      setEconError("");
      setStep("results");
    } catch (err) {
      setError(err.message || "Optimization failed.");
    } finally {
      setBusy(false);
    }
  }

  async function exportSchedule() {
    if (!result) return;
    const response = await fetch(`${API_BASE}/api/export-schedule`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ schedule: result.schedule })
    });
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "battery_schedule.csv";
    link.click();
    URL.revokeObjectURL(url);
  }

  return (
    <main>
      <header className="topbar">
        <div>
          <span className="mark"><BatteryCharging size={18} /> Watt ETW</span>
          <h1>Battery Fleet Optimization</h1>
        </div>
        <nav aria-label="Workflow">
          <button className={step === "fleet" ? "active" : ""} onClick={() => setStep("fleet")}>Fleet</button>
          <button className={step === "results" ? "active" : ""} disabled={!result} onClick={() => setStep("results")}>Results</button>
        </nav>
      </header>

      {error && <div className="alert">{error}</div>}

      {step === "landing" && <Landing onStart={() => setStep("fleet")} />}

      {step === "fleet" && (
        <section>
          <div className="sectionHead">
            <div>
              <h2>Choose Battery Fleet Setup</h2>
            </div>
            <button onClick={() => setBatteries([...batteries, defaultBattery(batteries.length + 1)])}>
              <Plus size={16} /> Add Battery
            </button>
          </div>
          <div className="batteryGrid">
            {batteries.map((battery, index) => (
              <BatteryForm
                key={index}
                battery={battery}
                onChange={(next) => {
                  const copy = [...batteries];
                  copy[index] = next;
                  setBatteries(copy);
                }}
                onRemove={() => setBatteries(batteries.filter((_, i) => i !== index))}
                canRemove={batteries.length > 1}
              />
            ))}
          </div>
          <div className="actions">
            <button className="primary" disabled={busy} onClick={optimize}>
              <Play size={18} /> {busy ? "Optimizing..." : "Optimize"}
            </button>
          </div>
        </section>
      )}

      {step === "results" && result && (
        <Results
          result={result}
          rows={result.schedule || []}
          exportSchedule={exportSchedule}
          econ={{
            result: econResult,
            inputs: econInputs,
            setInputs: setEconInputs,
            load: loadEconomics,
            busy: econBusy,
            error: econError
          }}
        />
      )}
    </main>
  );
}

function Landing({ onStart }) {
  return (
    <section className="landing">
      <div className="hero">
        <div className="heroCopy">
          <h2>
            A{" "}
            <span className="cursorWord" aria-label="click">
              <MousePointer2 className="cursorShadow" size={52} strokeWidth={2.55} />
              <MousePointer2 className="cursorIcon cursorIconBack" size={48} strokeWidth={2.45} />
              <MousePointer2 className="cursorIcon cursorIconFront" size={48} strokeWidth={2.45} />
            </span>{" "}
            Away From Battery Arbitrage
          </h2>
          <p className="heroTagline">Battery Arbitrage Workflow</p>
          <div className="heroActions">
            <button className="primary" onClick={onStart}>
              <Play size={18} /> Start Optimization
            </button>
          </div>
        </div>
        <MarketPreview />
      </div>
    </section>
  );
}

function MarketPreview() {
  return (
    <div className="marketPreview workflowPreview" aria-label="Top-down workflow">
      <div className="workflowHead">
        <strong>Battery Arbitrage Journey</strong>
      </div>
      <div className="workflowStack" role="list">
        <div className="workflowStep" role="listitem">Configure Battery Fleet</div>
        <div className="workflowArrow" aria-hidden="true">↓</div>
        <div className="workflowStep" role="listitem">Battery Arbitrage Results</div>
      </div>
    </div>
  );
}

function normalizeBatteryPayload(battery) {
  return {
    name: battery.name || "Battery",
    capacity: numericValue(battery.capacity, battery.capacity_mwh, battery.max_capacity, battery.max_capacity_mwh),
    min_capacity: numericValue(battery.min_capacity, battery.min_capacity_mwh),
    efficiency: numericValue(battery.efficiency, battery.effieciency, battery.round_trip_efficiency),
    availability: numericValue(battery.availability, battery.availability_pct),
    ramp: numericValue(battery.ramp, battery.ramp_mw, battery.power_mw)
  };
}

function numericValue(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== "") {
      return Number(value);
    }
  }
  return undefined;
}

function BatteryForm({ battery, onChange, onRemove, canRemove }) {
  const fields = [
    ["capacity", "Capacity MWh"],
    ["min_capacity", "Min Capacity MWh"],
    ["efficiency", "Efficiency"],
    ["availability", "Availability %"],
    ["ramp", "Ramp MW"]
  ];
  return (
    <article className="panel">
      <div className="panelTitle">
        <input value={battery.name} onChange={(e) => onChange({ ...battery, name: e.target.value })} />
        {canRemove && <button className="icon" onClick={onRemove}><Trash2 size={16} /></button>}
      </div>
      <div className="formGrid">
        {fields.map(([key, label]) => (
          <label key={key}>
            <span>{label}</span>
            <input
              type="number"
              step="0.01"
              value={battery[key]}
              onChange={(e) => onChange({ ...battery, [key]: e.target.value })}
            />
          </label>
        ))}
      </div>
    </article>
  );
}

function Results({ result, rows, exportSchedule, econ }) {
  const [activeTab, setActiveTab] = useState("market");
  const k = result.kpis || {};
  const schedule = result.schedule || [];
  return (
    <section>
      <div className="sectionHead">
        <div>
          <h2>Results Dashboard</h2>
        </div>
        <button onClick={exportSchedule}>Export CSV</button>
      </div>

      <div className="resultTabs" role="tablist" aria-label="Result views">
        <button
          className={activeTab === "market" ? "active" : ""}
          role="tab"
          aria-selected={activeTab === "market"}
          onClick={() => setActiveTab("market")}
        >
          Market Signals
        </button>
        <button
          className={activeTab === "dashboard" ? "active" : ""}
          role="tab"
          aria-selected={activeTab === "dashboard"}
          onClick={() => setActiveTab("dashboard")}
        >
          Economics
        </button>
        <button
          className={activeTab === "bidding" ? "active" : ""}
          role="tab"
          aria-selected={activeTab === "bidding"}
          onClick={() => setActiveTab("bidding")}
        >
          Expected Bidding
        </button>
        <button
          className={activeTab === "investment" ? "active" : ""}
          role="tab"
          aria-selected={activeTab === "investment"}
          onClick={() => setActiveTab("investment")}
        >
          Investment
        </button>
      </div>

      {activeTab === "market" && (
        <div className="chartStack" role="tabpanel">
          <div className="panel"><h3>DAM Prices</h3><Sparkline rows={rows} /></div>
          <div className="panel"><h3>Battery Actions</h3><ActionBars schedule={schedule} /></div>
          <SignalExplanation rows={rows} schedule={schedule} />
        </div>
      )}

      {activeTab === "dashboard" && (
        <div role="tabpanel">
          <div className="panel">
            <h3>Economic Figures</h3>
            <div className="economicGrid">
              <EconomicFigure label="Expected Profit" value={formatCurrency(k.expected_profit ?? result.revenue_eur)} />
              <EconomicFigure label="Revenue" value={formatCurrency(k.discharge_revenue)} />
              <EconomicFigure label="Charging Cost" value={formatCurrency(k.charging_cost)} />
              <EconomicFigure label="Cycles" value={formatNumber(k.equivalent_cycles)} />
            </div>
          </div>
          <div className="panel wide">
            <h3>Dispatch Dashboard</h3>
            <table>
              <thead>
                <tr><th>Time</th><th>Price</th><th>Action</th><th>Charge MW</th><th>Discharge MW</th></tr>
              </thead>
              <tbody>
                {schedule.map((row) => (
                  <tr key={row.timestamp}>
                    <td>{row.timestamp.replace("T", " ")}</td>
                    <td>{row.price_eur_mwh}</td>
                    <td><span className={`pill ${row.action}`}>{row.action}</span></td>
                    <td>{row.charge_mw}</td>
                    <td>{row.discharge_mw}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === "bidding" && (
        <ExpectedBidding schedule={schedule} kpis={k} />
      )}

      {activeTab === "investment" && (
        <Investment econ={econ} />
      )}
    </section>
  );
}

const VERDICT_META = {
  worth_it: { label: "Worth it", tone: "verdictGood" },
  marginal: { label: "Marginal", tone: "verdictAmber" },
  burning_money: { label: "Burning money", tone: "verdictBad" }
};

const ECON_FIELDS = [
  ["capex_per_mwh_energy", "CAPEX EUR/MWh", { step: 1000 }],
  ["realization_ratio", "Realization vs perfect-foresight", { step: 0.01, min: 0, max: 1 }],
  ["availability", "Availability", { step: 0.01, min: 0, max: 1 }],
  ["annual_degradation", "Annual capacity fade", { step: 0.005, min: 0, max: 0.2 }],
  ["wacc", "WACC", { step: 0.005, min: 0, max: 0.5 }],
  ["lifetime_years", "Lifetime years", { step: 1, min: 1, max: 30 }],
  ["opex_fixed_pct", "Fixed OPEX (% of CAPEX)", { step: 0.005, min: 0, max: 0.2 }],
  ["opex_var_eur_per_mwh", "Variable OPEX EUR/MWh", { step: 0.5, min: 0 }],
  ["augmentation_pct", "Augmentation (% of energy CAPEX)", { step: 0.001, min: 0, max: 0.1 }],
  ["salvage_pct", "Salvage (% of CAPEX)", { step: 0.01, min: 0, max: 0.5 }],
  ["tax_rate", "Tax rate", { step: 0.01, min: 0, max: 0.6 }],
  ["depreciation_years", "Depreciation years", { step: 1, min: 1, max: 30 }],
  ["grant_eur", "Grant EUR", { step: 100000, min: 0 }],
  ["grid_connection_eur", "Grid connection EUR", { step: 100000, min: 0 }]
];

function Investment({ econ }) {
  const { result, inputs, setInputs, load, busy, error } = econ;

  return (
    <div className="bidStack" role="tabpanel">
      <div className="panel">
        <h3>Investment Verdict</h3>
        <p className="bidNote">
          Annualizes the optimizer's daily revenue with a realistic perfect-foresight realization ratio,
          subtracts OPEX, augmentation, and Greek corporate tax, then discounts at your WACC over the
          asset's lifetime. Defaults reflect mid-range Greek 2026 utility-scale Li-ion BESS values.
        </p>
        <div className="actions">
          <button className="primary" disabled={busy} onClick={() => load()}>
            {result ? (busy ? "Recalculating..." : "Recalculate") : (busy ? "Calculating..." : "Calculate Economics")}
          </button>
        </div>
        {error && <div className="alert">{error}</div>}
      </div>

      {result && (
        <>
          <div className="panel">
            <div className="verdictRow">
              <VerdictBadge verdict={result.verdict} />
              <div className="verdictBlurb">
                <span>NPV (over {inputs.lifetime_years}-year lifetime, discounted at {(inputs.wacc * 100).toFixed(1)}% WACC)</span>
                <strong className={result.npv >= 0 ? "toneGreen" : "toneRose"}>{formatCurrency(result.npv)}</strong>
              </div>
            </div>
            <div className="economicGrid">
              <EconomicFigure label="CAPEX" value={formatCurrency(result.capex)} />
              <EconomicFigure label="Payback (αποσβεση)" value={formatPaybackYears(result.payback_years)} />
              <EconomicFigure label="IRR" value={formatPercent(result.irr)} />
              <EconomicFigure label="Year-1 Revenue" value={formatCurrency(result.annual_revenue_year1)} />
              <EconomicFigure label="Year-1 OPEX" value={formatCurrency(result.annual_opex_year1)} />
              <EconomicFigure label="Daily Revenue" value={formatCurrency(econInputsDailyRevenue(result))} />
            </div>
          </div>

          <CalculationBreakdown result={result} />

          <div className="panel wide">
            <h3>Year-by-Year Cash Flow</h3>
            <div className="tableScroller">
              <table>
                <thead>
                  <tr>
                    <th>Year</th>
                    <th>Revenue</th>
                    <th>OPEX</th>
                    <th>Depreciation</th>
                    <th>Tax</th>
                    <th>Net Cash Flow</th>
                    <th>Cumulative</th>
                  </tr>
                </thead>
                <tbody>
                  {result.cash_flows.map((cf) => (
                    <tr key={cf.year}>
                      <td>{cf.year}</td>
                      <td>{formatCurrency(cf.revenue)}</td>
                      <td>{formatCurrency(cf.opex)}</td>
                      <td>{formatCurrency(cf.depreciation)}</td>
                      <td>{formatCurrency(cf.tax)}</td>
                      <td className={cf.net_cash_flow >= 0 ? "cashPositive" : "cashNegative"}>{formatCurrency(cf.net_cash_flow)}</td>
                      <td className={cf.cumulative_undiscounted >= 0 ? "cashPositive" : "cashNegative"}>{formatCurrency(cf.cumulative_undiscounted)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      <div className="panel">
        <h3>Assumptions</h3>
        <div className="formGrid economicsForm">
          {ECON_FIELDS.map(([key, label, attrs]) => (
            <label key={key}>
              <span>{label}</span>
              <input
                type="number"
                step={attrs?.step ?? 0.01}
                min={attrs?.min}
                max={attrs?.max}
                value={inputs[key]}
                onChange={(e) => setInputs({ ...inputs, [key]: e.target.value === "" ? "" : Number(e.target.value) })}
              />
            </label>
          ))}
        </div>
      </div>
    </div>
  );
}

function CalculationBreakdown({ result }) {
  const i = result.inputs;
  const b = result.breakdown;
  const dailyRevenue = i.daily_revenue_eur;
  const dailyThroughput = i.daily_throughput_mwh;
  const npv = result.npv;
  const wacc = i.wacc;
  const lifetime = i.lifetime_years;

  const paybackRow = (() => {
    if (result.payback_years === null || result.payback_years === undefined) return null;
    const targetYear = Math.ceil(result.payback_years);
    const before = result.cash_flows[targetYear - 2];
    const after = result.cash_flows[targetYear - 1];
    return { targetYear, before, after };
  })();

  return (
    <div className="panel wide">
      <h3>How we got these numbers</h3>
      <p className="bidNote">Every figure above is derived from the values you set in Assumptions plus the optimizer's daily revenue and throughput. Here is the math, with your numbers plugged in.</p>

      <div className="breakdownBlock">
        <div className="breakdownTitle">CAPEX</div>
        <BreakdownLine label={`Energy: ${fmtN(i.energy_capacity_mwh)} MWh × ${fmtMoney0(i.capex_per_mwh_energy)}/MWh`} value={fmtMoney0(b.energy_capex)} />
        <BreakdownLine label={`Power: ${fmtN(i.power_capacity_mw)} MW × ${fmtMoney0(i.capex_per_mw_power)}/MW`} value={fmtMoney0(b.power_capex)} />
        <BreakdownLine label="Grid connection" value={fmtMoney0(b.grid_connection)} />
        <BreakdownLine label="Grant" value={fmtMoney0(-b.grant)} negative />
        <BreakdownTotal label="Total CAPEX" value={fmtMoney0(result.capex)} />
      </div>

      <div className="breakdownBlock">
        <div className="breakdownTitle">Year-1 realized revenue</div>
        <BreakdownLine label="Daily revenue (from optimizer)" value={fmtMoney(dailyRevenue)} />
        <BreakdownLine label={`Operating days per year`} value={fmtN(i.operating_days_per_year)} muted />
        <BreakdownLine label={`Realization ratio (perfect-foresight → real)`} value={fmtN(i.realization_ratio)} muted />
        <BreakdownLine label={`Availability`} value={fmtN(i.availability)} muted />
        <BreakdownTotal label="Year-1 revenue" value={fmtMoney0(result.annual_revenue_year1)} note={`= ${fmtMoney(dailyRevenue)} × ${i.operating_days_per_year} × ${i.realization_ratio} × ${i.availability}`} />
      </div>

      <div className="breakdownBlock">
        <div className="breakdownTitle">Year-1 OPEX</div>
        <BreakdownLine label={`Fixed: CAPEX × ${fmtPct(i.opex_fixed_pct)}`} value={fmtMoney0(b.fixed_opex)} />
        <BreakdownLine label={`Augmentation: energy CAPEX × ${fmtPct(i.augmentation_pct)}`} value={fmtMoney0(b.augmentation)} />
        <BreakdownLine label={`Variable: ${fmtN(dailyThroughput)} MWh/day × ${i.operating_days_per_year} × ${fmtN(i.availability)} × ${fmtMoney(i.opex_var_eur_per_mwh)}/MWh`} value={fmtMoney0(b.var_opex_year1)} />
        <BreakdownTotal label="Year-1 OPEX" value={fmtMoney0(result.annual_opex_year1)} />
        <div className="breakdownNote">Throughput-driven variable OPEX scales down each year by the {fmtPct(i.annual_degradation)} capacity fade; fixed and augmentation stay flat.</div>
      </div>

      <div className="breakdownBlock">
        <div className="breakdownTitle">Each year's net cash flow</div>
        <ul className="breakdownBullets">
          <li>Year-y revenue: year-1 revenue, faded by {fmtPct(i.annual_degradation)} per year.</li>
          <li>Year-y OPEX: fixed + augmentation + variable (variable fades with revenue).</li>
          <li>Tax: {fmtPct(i.tax_rate)} of (revenue − OPEX − depreciation), floored at zero.</li>
          <li>Net cash flow = revenue − OPEX − tax.</li>
        </ul>
        <BreakdownLine label={`Annual depreciation: CAPEX / ${i.depreciation_years} yrs`} value={fmtMoney0(b.annual_depreciation)} />
        <BreakdownLine label={`Year-${lifetime} salvage: CAPEX × ${fmtPct(i.salvage_pct)}`} value={fmtMoney0(b.salvage_value)} />
      </div>

      <div className="breakdownBlock">
        <div className="breakdownTitle">NPV at {fmtPct(wacc)} WACC over {lifetime} years</div>
        <BreakdownLine label="CAPEX (paid up-front)" value={fmtMoney0(-result.capex)} negative />
        <BreakdownLine label={`Sum of discounted cash flows (${lifetime} yrs, incl. salvage)`} value={fmtMoney0(b.discounted_cf_sum)} />
        <BreakdownTotal label="NPV" value={fmtMoney0(npv)} tone={npv >= 0 ? "good" : "bad"} />
      </div>

      <div className="breakdownBlock">
        <div className="breakdownTitle">Payback (αποσβεση)</div>
        {paybackRow ? (
          <>
            <div className="breakdownNote">Cumulative cash flow first crosses CAPEX between year {paybackRow.targetYear - 1} and year {paybackRow.targetYear}.</div>
            {paybackRow.before && (
              <BreakdownLine label={`Cumulative at end of year ${paybackRow.before.year}`} value={fmtMoney0(paybackRow.before.cumulative_undiscounted)} />
            )}
            <BreakdownLine label="CAPEX (target to recover)" value={fmtMoney0(result.capex)} muted />
            <BreakdownLine label={`Cumulative at end of year ${paybackRow.after.year}`} value={fmtMoney0(paybackRow.after.cumulative_undiscounted)} />
            <BreakdownTotal label="Payback" value={`${result.payback_years.toFixed(2)} yrs`} note={`linear interpolation between year ${paybackRow.targetYear - 1} and year ${paybackRow.targetYear}`} />
          </>
        ) : (
          <div className="breakdownNote">Cumulative cash flow never reaches CAPEX over the {lifetime}-year lifetime — payback is "never" with these inputs.</div>
        )}
      </div>

      <div className="breakdownBlock">
        <div className="breakdownTitle">IRR</div>
        <BreakdownLine label="The discount rate that makes NPV zero (solved numerically)" muted />
        <BreakdownTotal label="IRR" value={fmtPct(result.irr)} tone={result.irr !== null && result.irr >= wacc ? "good" : "bad"} note={`vs ${fmtPct(wacc)} WACC — project ${result.irr !== null && result.irr >= wacc ? "clears" : "fails"} the cost of capital.`} />
      </div>
    </div>
  );
}

function BreakdownLine({ label, value, muted, negative, tone }) {
  const cls = ["breakdownLine"];
  if (muted) cls.push("breakdownMuted");
  if (tone === "good") cls.push("toneGreen");
  if (tone === "bad") cls.push("toneRose");
  if (negative) cls.push("toneRose");
  return (
    <div className={cls.join(" ")}>
      <span className="breakdownLabel">{label}</span>
      {value !== undefined && value !== null && value !== "" && (
        <span className="breakdownValue">{value}</span>
      )}
    </div>
  );
}

function BreakdownTotal({ label, value, tone, note }) {
  const cls = ["breakdownLine", "breakdownTotal"];
  if (tone === "good") cls.push("toneGreen");
  if (tone === "bad") cls.push("toneRose");
  return (
    <>
      <div className={cls.join(" ")}>
        <span className="breakdownLabel">{label}</span>
        <span className="breakdownValue">{value}</span>
      </div>
      {note && <div className="breakdownTotalNote">{note}</div>}
    </>
  );
}

function fmtMoney(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return `€${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtMoney0(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return `€${n.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

function fmtN(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return n.toLocaleString("en-US", { maximumFractionDigits: 4 });
}

function fmtPct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return `${(n * 100).toFixed(2)}%`;
}

function VerdictBadge({ verdict }) {
  const meta = VERDICT_META[verdict] || { label: verdict || "Unknown", tone: "verdictAmber" };
  return <div className={`verdictBadge ${meta.tone}`}>{meta.label}</div>;
}

function econInputsDailyRevenue(result) {
  const cf = result.cash_flows?.[0];
  if (!cf) return 0;
  return result.annual_revenue_year1 / 365;
}

function formatPercent(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return `${(number * 100).toFixed(2)}%`;
}

function formatPaybackYears(value) {
  if (value === null || value === undefined) return "never";
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return `${number.toFixed(1)} yrs`;
}

function EconomicFigure({ label, value }) {
  return <div className="economicFigure"><span>{label}</span><strong>{value}</strong></div>;
}

function formatCurrency(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return `€${number.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

function formatNumber(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return number.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function ExpectedBidding({ schedule, kpis }) {
  const bidding = getExpectedBidding(schedule, kpis);

  return (
    <div className="bidStack" role="tabpanel">
      <div className="panel">
        <h3>Expected Bidding Formula</h3>
        <div className="formulaGrid">
          <div><span>Buy / charge quantity</span><strong>Q_buy,t = charge_mw,t × 0.25h</strong></div>
          <div><span>Sell / discharge quantity</span><strong>Q_sell,t = discharge_mw,t × 0.25h</strong></div>
          <div><span>Charge bid cap</span><strong>P_buy,t ≈ η × avg scheduled sell price</strong></div>
          <div><span>Discharge offer floor</span><strong>P_sell,t ≈ max(avg buy price / η, €3/MWh)</strong></div>
        </div>
        <p className="bidNote">
          This is an expected bidding view: it converts the optimized dispatch into buy orders for charging and sell offers for discharging. It is suitable for business interpretation and scenario comparison, while a production exchange submission would still need participant-specific HEnEx order validation, credit, and portfolio rules.
        </p>
      </div>

      <div className="panel">
        <h3>Bidding Summary</h3>
        <div className="economicGrid">
          <EconomicFigure label="Buy Volume" value={formatMwh(bidding.buyMwh)} />
          <EconomicFigure label="Sell Volume" value={formatMwh(bidding.sellMwh)} />
          <EconomicFigure label="Expected Net Cashflow" value={formatCurrency(bidding.expectedNetCashflow)} />
          <EconomicFigure label="Orders" value={formatNumber(bidding.rows.length)} />
        </div>
      </div>

      <div className="panel">
        <h3>Expected Bid / Offer Plan</h3>
        <div className="tableScroller">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Order</th>
                <th>Bid MW</th>
                <th>Energy MWh</th>
                <th>Limit Price</th>
                <th>Forecast DAM</th>
                <th>Expected Cashflow</th>
              </tr>
            </thead>
            <tbody>
              {bidding.rows.map((row) => (
                <tr key={`${row.time}-${row.side}`}>
                  <td>{row.time}</td>
                  <td><span className={`pill ${row.sideClass}`}>{row.side}</span></td>
                  <td>{formatNumber(row.mw)}</td>
                  <td>{formatNumber(row.mwh)}</td>
                  <td>{formatPrice(row.limitPrice)}</td>
                  <td>{formatPrice(row.forecastPrice)}</td>
                  <td className={row.cashflow >= 0 ? "cashPositive" : "cashNegative"}>{formatCurrency(row.cashflow)}</td>
                </tr>
              ))}
              {!bidding.rows.length && (
                <tr><td colSpan="7">No charge or discharge orders were produced for this schedule.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function getExpectedBidding(schedule, kpis) {
  const eta = positiveNumber(kpis.eta, 0.9);
  const minDischargePrice = positiveNumber(kpis.min_discharge_price_eur_mwh, DEFAULT_MIN_DISCHARGE_PRICE);
  const actionStats = getActionStats(schedule);
  const avgBuy = Number.isFinite(actionStats.avgChargePrice)
    ? actionStats.avgChargePrice
    : minPrice(schedule);
  const avgSell = Number.isFinite(actionStats.avgDischargePrice)
    ? actionStats.avgDischargePrice
    : maxPrice(schedule);
  const buyLimit = Number.isFinite(avgSell) ? eta * avgSell : Number.NaN;
  const sellFloor = Math.max(
    Number.isFinite(avgBuy) ? avgBuy / eta : Number.NaN,
    minDischargePrice
  );

  const rows = schedule.flatMap((row) => {
    const forecastPrice = Number(row.price_eur_mwh);
    const chargeMw = Number(row.charge_mw || 0);
    const dischargeMw = Number(row.discharge_mw || 0);
    const time = formatTimeLabel(row.timestamp);

    if (chargeMw > 0) {
      const mwh = chargeMw * INTERVAL_HOURS;
      return [{
        time,
        side: "Buy / Charge",
        sideClass: "charge",
        mw: chargeMw,
        mwh,
        limitPrice: buyLimit,
        forecastPrice,
        cashflow: -mwh * forecastPrice
      }];
    }

    if (dischargeMw > 0) {
      const mwh = dischargeMw * INTERVAL_HOURS;
      return [{
        time,
        side: "Sell / Discharge",
        sideClass: "discharge",
        mw: dischargeMw,
        mwh,
        limitPrice: sellFloor,
        forecastPrice,
        cashflow: mwh * forecastPrice
      }];
    }

    return [];
  });

  return {
    rows,
    buyMwh: rows.filter((row) => row.sideClass === "charge").reduce((sum, row) => sum + row.mwh, 0),
    sellMwh: rows.filter((row) => row.sideClass === "discharge").reduce((sum, row) => sum + row.mwh, 0),
    expectedNetCashflow: rows.reduce((sum, row) => sum + row.cashflow, 0)
  };
}

function positiveNumber(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : fallback;
}

function minPrice(schedule) {
  const prices = schedule.map((row) => Number(row.price_eur_mwh)).filter(Number.isFinite);
  return prices.length ? Math.min(...prices) : Number.NaN;
}

function maxPrice(schedule) {
  const prices = schedule.map((row) => Number(row.price_eur_mwh)).filter(Number.isFinite);
  return prices.length ? Math.max(...prices) : Number.NaN;
}

function SignalExplanation({ rows, schedule }) {
  const priceStats = getPriceStats(rows);
  const actionStats = getActionStats(schedule);

  if (!priceStats) {
    return (
      <div className="panel signalExplanation">
        <h3>Explanation</h3>
        <p>Run an optimization to see how the price signal maps into charge, discharge, and hold decisions.</p>
      </div>
    );
  }

  const spreadText = Number.isFinite(actionStats.spread)
    ? `${formatPrice(actionStats.spread)} average discharge premium`
    : "no completed arbitrage spread";
  const actionText = actionStats.dischargeIntervals > 0
    ? `The optimizer charges ${formatMwh(actionStats.chargeMwh)} around ${actionStats.chargeWindow}, then discharges ${formatMwh(actionStats.dischargeMwh)} around ${actionStats.dischargeWindow}.`
    : `The optimizer charges ${formatMwh(actionStats.chargeMwh)} around ${actionStats.chargeWindow} and avoids discharge when the price spread is not strong enough.`;

  return (
    <div className="panel signalExplanation">
      <h3>Explanation</h3>
      <div className="explanationGrid">
        <div>
          <span>Lowest DAM</span>
          <strong>{formatPrice(priceStats.min.value)}</strong>
          <small>{priceStats.min.time}</small>
        </div>
        <div>
          <span>Highest DAM</span>
          <strong>{formatPrice(priceStats.max.value)}</strong>
          <small>{priceStats.max.time}</small>
        </div>
        <div>
          <span>Charge Intervals</span>
          <strong>{actionStats.chargeIntervals}</strong>
          <small>{formatMwh(actionStats.chargeMwh)}</small>
        </div>
        <div>
          <span>Discharge Intervals</span>
          <strong>{actionStats.dischargeIntervals}</strong>
          <small>{formatMwh(actionStats.dischargeMwh)}</small>
        </div>
      </div>
      <p className="explanationText">
        <strong>The DAM price diagram shows the economic signal:</strong>{" "}
        <span>the battery should absorb energy near the low-price valley and release it near stronger price periods.</span>{" "}
        <strong className="toneViolet">{actionText}</strong>{" "}
        <strong className="toneGreen">The resulting action diagram is a constraint-aware arbitrage schedule with {spreadText}.</strong>{" "}
        <span>Idle intervals indicate moments where the spread, efficiency loss, ramp limits, or minimum discharge threshold do not justify movement.</span>
      </p>
    </div>
  );
}

function getPriceStats(rows) {
  const points = rows
    .map((row) => ({
      time: formatTimeLabel(row.timestamp),
      value: Number(row.price_eur_mwh)
    }))
    .filter((point) => Number.isFinite(point.value));

  if (!points.length) return null;

  return points.reduce((stats, point) => ({
    min: point.value < stats.min.value ? point : stats.min,
    max: point.value > stats.max.value ? point : stats.max
  }), { min: points[0], max: points[0] });
}

function getActionStats(schedule) {
  const chargeRows = schedule.filter((row) => Number(row.charge_mw) > 0);
  const dischargeRows = schedule.filter((row) => Number(row.discharge_mw) > 0);
  const chargeEnergy = chargeRows.reduce((sum, row) => sum + Number(row.charge_mw) * 0.25, 0);
  const dischargeEnergy = dischargeRows.reduce((sum, row) => sum + Number(row.discharge_mw) * 0.25, 0);
  const avgChargePrice = weightedPrice(chargeRows, "charge_mw");
  const avgDischargePrice = weightedPrice(dischargeRows, "discharge_mw");

  return {
    chargeIntervals: chargeRows.length,
    dischargeIntervals: dischargeRows.length,
    chargeMwh: chargeEnergy,
    dischargeMwh: dischargeEnergy,
    chargeWindow: actionWindow(chargeRows),
    dischargeWindow: actionWindow(dischargeRows),
    avgChargePrice,
    avgDischargePrice,
    spread: avgDischargePrice - avgChargePrice
  };
}

function weightedPrice(rows, weightKey) {
  const totalWeight = rows.reduce((sum, row) => sum + Number(row[weightKey] || 0), 0);
  if (totalWeight <= 0) return Number.NaN;
  return rows.reduce((sum, row) => sum + Number(row.price_eur_mwh) * Number(row[weightKey] || 0), 0) / totalWeight;
}

function actionWindow(rows) {
  if (!rows.length) return "no scheduled intervals";
  const first = formatTimeLabel(rows[0].timestamp);
  const last = formatTimeLabel(rows[rows.length - 1].timestamp);
  return first === last ? first : `${first}-${last}`;
}

function formatTimeLabel(value) {
  const text = String(value || "");
  const timePart = text.includes("T") ? text.split("T").pop() : text;
  return timePart.slice(0, 5) || "n/a";
}

function formatPrice(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return `€${number.toLocaleString("en-US", { maximumFractionDigits: 2 })}/MWh`;
}

function formatMwh(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return `${number.toLocaleString("en-US", { maximumFractionDigits: 2 })} MWh`;
}

function Sparkline({ rows }) {
  const data = rows
    .map((row) => ({
      price: Number(row.price_eur_mwh),
      label: formatTimeLabel(row.timestamp)
    }))
    .filter((d) => Number.isFinite(d.price));
  const prices = data.map((d) => d.price);
  const domain = prices.length ? { min: Math.min(...prices), max: Math.max(...prices) } : null;
  const [hoverIdx, setHoverIdx] = useState(null);

  function onMouseMove(e) {
    if (data.length < 2) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const frac = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    setHoverIdx(Math.round(frac * (data.length - 1)));
  }

  const hovered = hoverIdx != null ? data[hoverIdx] : null;
  const span = domain ? Math.max(domain.max - domain.min, 1) : 1;
  const xPct = hoverIdx != null && data.length > 1 ? (hoverIdx / (data.length - 1)) * 100 : 0;
  const yPct = hovered && domain ? 90 - ((hovered.price - domain.min) / span) * 80 : 50;

  return (
    <ChartWithAxis priceDomain={domain}>
      <div
        className="chartLineWrap"
        onMouseMove={onMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        <Line points={prices} color="var(--blue)" />
        {hovered && (
          <Crosshair xPct={xPct} yPct={yPct} time={hovered.label} price={hovered.price} />
        )}
      </div>
    </ChartWithAxis>
  );
}

function Crosshair({ xPct, yPct, time, price }) {
  const tipSide = xPct < 50 ? "tipRight" : "tipLeft";
  return (
    <>
      <span className="crosshairVert" style={{ left: `${xPct}%` }} />
      <span className="crosshairDot" style={{ left: `${xPct}%`, top: `${yPct}%` }} />
      <div className={`crosshairTip ${tipSide}`} style={{ left: `${xPct}%`, top: `${yPct}%` }}>
        <strong>€{price.toFixed(2)}/MWh</strong>
        <span>{time}</span>
      </div>
    </>
  );
}

function ActionBars({ schedule }) {
  const max = Math.max(1, ...schedule.map((row) => Math.max(row.charge_mw, row.discharge_mw)));
  const [hoverIdx, setHoverIdx] = useState(null);

  function onMouseMove(e) {
    if (!schedule.length) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const frac = Math.max(0, Math.min(0.999, (e.clientX - rect.left) / rect.width));
    setHoverIdx(Math.floor(frac * schedule.length));
  }

  const hovered = hoverIdx != null ? schedule[hoverIdx] : null;
  const xPct = hoverIdx != null && schedule.length > 0 ? ((hoverIdx + 0.5) / schedule.length) * 100 : 0;

  return (
    <ChartWithAxis>
      <div
        className="chartLineWrap"
        onMouseMove={onMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        <div className="bars">
          {schedule.map((row, i) => {
            const value = row.action === "charge" ? -row.charge_mw : row.discharge_mw;
            const cls = value < 0 ? "bar charge" : value > 0 ? "bar discharge" : "bar";
            const highlighted = i === hoverIdx ? " barHighlight" : "";
            return (
              <span
                key={row.timestamp}
                className={cls + highlighted}
                style={{ height: `${Math.abs(value) / max * 80 + 3}px` }}
              />
            );
          })}
        </div>
        {hovered && (
          <ActionTooltip
            xPct={xPct}
            time={formatTimeLabel(hovered.timestamp)}
            row={hovered}
          />
        )}
      </div>
    </ChartWithAxis>
  );
}

function ActionTooltip({ xPct, time, row }) {
  const tipSide = xPct < 50 ? "tipRight" : "tipLeft";
  const action = row.action || "hold";
  const mw = action === "charge" ? row.charge_mw : action === "discharge" ? row.discharge_mw : 0;
  const labelByAction = { charge: "Charging", discharge: "Discharging", hold: "Holding" };
  return (
    <>
      <span className="crosshairVert" style={{ left: `${xPct}%` }} />
      <div className={`crosshairTip ${tipSide}`} style={{ left: `${xPct}%`, top: "50%" }}>
        <strong>{labelByAction[action]}{mw > 0 ? ` ${mw.toFixed(2)} MW` : ""}</strong>
        <span>{time} · €{Number(row.price_eur_mwh).toFixed(2)}/MWh</span>
      </div>
    </>
  );
}

function ChartWithAxis({ children, priceDomain }) {
  return (
    <div className="chartViewport">
      <div className="chartTrack">
        <div className="chartArea">
          {priceDomain && <PriceAxis min={priceDomain.min} max={priceDomain.max} />}
          {children}
        </div>
        <TimeAxis />
      </div>
    </div>
  );
}

function PriceAxis({ min, max }) {
  const tickCount = 5;
  const ticks = Array.from({ length: tickCount }, (_, i) => {
    const frac = i / (tickCount - 1);
    return {
      value: max - frac * (max - min),
      topPct: 10 + frac * 80
    };
  });
  return (
    <div className="priceAxis" aria-hidden="true">
      {ticks.map((tick, i) => (
        <div className="priceTick" key={i} style={{ top: `${tick.topPct}%` }}>
          <span className="priceLabel">€{tick.value.toFixed(0)}</span>
          <span className="priceGridline" />
        </div>
      ))}
    </div>
  );
}

function TimeAxis() {
  return (
    <div className="timeAxis" aria-label="Hourly time axis">
      {HOUR_LABELS.map((label) => <span key={label}>{label}</span>)}
    </div>
  );
}

function Line({ points, color }) {
  if (!points.length) return <div className="emptyChart" />;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = max - min || 1;
  const path = points.map((point, index) => {
    const x = points.length === 1 ? 0 : (index / (points.length - 1)) * 100;
    const y = 90 - ((point - min) / span) * 80;
    return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(" ");
  return (
    <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="lineChart">
      <path d={path} fill="none" stroke={color} strokeWidth="2.5" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

createRoot(document.getElementById("root")).render(<App />);
