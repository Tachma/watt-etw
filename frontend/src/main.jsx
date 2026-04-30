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

function App() {
  const [step, setStep] = useState("landing");
  const [batteries, setBatteries] = useState([defaultBattery()]);
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function optimize() {
    setBusy(true);
    setError("");
    try {
      const batteryPayload = batteries.map((battery, index) =>
        normalizeBatteryPayload({ ...defaultBattery(index + 1), ...battery })
      );

      const response = await fetch("/api/optimize-arbitrage", {
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
      setStep("results");
    } catch (err) {
      setError(err.message || "Optimization failed.");
    } finally {
      setBusy(false);
    }
  }

  async function exportSchedule() {
    if (!result) return;
    const response = await fetch("/api/export-schedule", {
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
              <p className="eyebrow">Step 1</p>
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
        <Results result={result} rows={result.schedule || []} exportSchedule={exportSchedule} />
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

function Results({ result, rows, exportSchedule }) {
  const k = result.kpis;
  const schedule = result.schedule || [];
  return (
    <section>
      <div className="sectionHead">
        <div>
          <p className="eyebrow">Step 3</p>
          <h2>Results Dashboard</h2>
        </div>
        <button onClick={exportSchedule}>Export CSV</button>
      </div>
      <div className="kpis">
        <Kpi label="Expected Profit" value={`€${k.expected_profit}`} />
        <Kpi label="Revenue" value={`€${k.discharge_revenue}`} />
        <Kpi label="Charging Cost" value={`€${k.charging_cost}`} />
        <Kpi label="Cycles" value={k.equivalent_cycles} />
        <Kpi label="Final SOC" value={`${k.final_soc_mwh} MWh`} />
      </div>
      <div className="charts">
        <div className="panel"><h3>DAM Price</h3><Sparkline rows={rows} /></div>
        <div className="panel"><h3>Battery Actions</h3><ActionBars schedule={schedule} /></div>
        <div className="panel"><h3>State of Charge</h3><SocLine schedule={schedule} /></div>
      </div>
      <div className="panel wide">
        <h3>Schedule</h3>
        <table>
          <thead>
            <tr><th>Time</th><th>Price</th><th>Action</th><th>Charge MW</th><th>Discharge MW</th><th>SOC</th><th>Reason</th></tr>
          </thead>
          <tbody>
            {schedule.map((row) => (
              <tr key={row.timestamp}>
                <td>{row.timestamp.replace("T", " ")}</td>
                <td>{row.price_eur_mwh}</td>
                <td><span className={`pill ${row.action}`}>{row.action}</span></td>
                <td>{row.charge_mw}</td>
                <td>{row.discharge_mw}</td>
                <td>{row.soc_mwh}</td>
                <td>{row.explanation}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Kpi({ label, value }) {
  return <div className="kpi"><span>{label}</span><strong>{value}</strong></div>;
}

function Sparkline({ rows }) {
  const points = rows.map((row) => Number(row.price_eur_mwh));
  return <Line points={points} color="var(--blue)" />;
}

function SocLine({ schedule }) {
  return <Line points={schedule.map((row) => Number(row.soc_mwh))} color="var(--green)" />;
}

function ActionBars({ schedule }) {
  const max = Math.max(1, ...schedule.map((row) => Math.max(row.charge_mw, row.discharge_mw)));
  return (
    <div className="bars">
      {schedule.map((row) => {
        const value = row.action === "charge" ? -row.charge_mw : row.discharge_mw;
        return <span key={row.timestamp} className={value < 0 ? "bar charge" : value > 0 ? "bar discharge" : "bar"} style={{ height: `${Math.abs(value) / max * 80 + 3}px` }} />;
      })}
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
