import React, { useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { BatteryCharging, CheckCircle2, FileUp, Play, Plus, Trash2 } from "lucide-react";
import "./styles.css";

const defaultBattery = (index = 1) => ({
  name: `Battery ${index}`,
  capacity_mwh: 50,
  power_mw: 25,
  round_trip_efficiency: 0.9,
  initial_soc_pct: 50,
  min_soc_pct: 10,
  max_soc_pct: 95,
  degradation_cost_eur_mwh: 5,
  max_cycles_per_day: ""
});

function App() {
  const [step, setStep] = useState("landing");
  const [batteries, setBatteries] = useState([defaultBattery()]);
  const [validation, setValidation] = useState(null);
  const [selectedDate, setSelectedDate] = useState("");
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const rows = validation?.rows || [];
  const selectedRows = useMemo(() => {
    if (!selectedDate) return rows;
    return rows.filter((row) => row.timestamp.startsWith(selectedDate));
  }, [rows, selectedDate]);

  async function validateFile(file) {
    setBusy(true);
    setError("");
    setValidation(null);
    const formData = new FormData();
    formData.append("file", file);
    try {
      const response = await fetch("/api/market-data/validate", {
        method: "POST",
        body: formData
      });
      const data = await response.json();
      setValidation(data);
      if (data.detected_dates?.length) {
        setSelectedDate(data.detected_dates[0]);
      }
    } catch (err) {
      setError(err.message || "Could not validate file.");
    } finally {
      setBusy(false);
    }
  }

  async function optimize() {
    setBusy(true);
    setError("");
    try {
      const response = await fetch("/api/optimize", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          batteries,
          market_rows: rows,
          selected_date: selectedDate
        })
      });
      const data = await response.json();
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
        <nav>
          <button onClick={() => setStep("fleet")}>Fleet</button>
          <button onClick={() => setStep("import")}>Import</button>
          <button disabled={!result} onClick={() => setStep("results")}>Results</button>
        </nav>
      </header>

      {error && <div className="alert">{error}</div>}

      {step === "landing" && (
        <section className="hero">
          <div>
            <p className="eyebrow">Greek DAM battery arbitrage</p>
            <h2>Turn HEnEx market data into a feasible battery schedule.</h2>
            <p>
              Configure a battery fleet, upload HEnEx-style market data, and calculate profit,
              revenue, operating cost, SOC, and charge/discharge actions.
            </p>
            <button className="primary" onClick={() => setStep("fleet")}>
              <Play size={18} /> Start Optimization
            </button>
          </div>
        </section>
      )}

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
            <button className="primary" onClick={() => setStep("import")}>Continue to Data Import</button>
          </div>
        </section>
      )}

      {step === "import" && (
        <section>
          <p className="eyebrow">Step 2</p>
          <h2>Import CSV/XLSX With DAM Data</h2>
          <label className="dropzone">
            <FileUp size={32} />
            <strong>Upload HEnEx-style DAM data</strong>
            <span>CSV or XLSX with market time and clearing price columns.</span>
            <input type="file" accept=".csv,.xlsx,.xlsm" onChange={(event) => event.target.files?.[0] && validateFile(event.target.files[0])} />
          </label>
          {busy && <p>Processing...</p>}
          {validation && (
            <ValidationPreview
              validation={validation}
              selectedDate={selectedDate}
              setSelectedDate={setSelectedDate}
              selectedRows={selectedRows}
              optimize={optimize}
              busy={busy}
            />
          )}
        </section>
      )}

      {step === "results" && result && (
        <Results result={result} rows={selectedRows} exportSchedule={exportSchedule} />
      )}
    </main>
  );
}

function BatteryForm({ battery, onChange, onRemove, canRemove }) {
  const fields = [
    ["capacity_mwh", "Capacity MWh"],
    ["power_mw", "Power MW"],
    ["round_trip_efficiency", "Efficiency"],
    ["initial_soc_pct", "Initial SOC %"],
    ["min_soc_pct", "Min SOC %"],
    ["max_soc_pct", "Max SOC %"],
    ["degradation_cost_eur_mwh", "Degradation EUR/MWh"],
    ["max_cycles_per_day", "Max Cycles"]
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

function ValidationPreview({ validation, selectedDate, setSelectedDate, selectedRows, optimize, busy }) {
  return (
    <div className="panel wide">
      <div className="statusLine">
        <CheckCircle2 size={18} />
        <strong>{validation.valid ? "Data validated" : "Validation failed"}</strong>
        <span>{validation.row_count} rows</span>
        <span>{validation.interval_minutes || "-"} min intervals</span>
      </div>
      {validation.errors?.map((item) => <p className="alert" key={item}>{item}</p>)}
      {validation.warnings?.map((item) => <p className="warning" key={item}>{item}</p>)}
      <div className="split">
        <label>
          <span>Delivery Day</span>
          <select value={selectedDate} onChange={(e) => setSelectedDate(e.target.value)}>
            {validation.detected_dates?.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <div className="miniStats">
          <span>Min EUR/MWh <strong>{validation.price_summary?.min?.toFixed?.(2)}</strong></span>
          <span>Avg EUR/MWh <strong>{validation.price_summary?.average?.toFixed?.(2)}</strong></span>
          <span>Max EUR/MWh <strong>{validation.price_summary?.max?.toFixed?.(2)}</strong></span>
        </div>
      </div>
      <Sparkline rows={selectedRows} />
      <button className="primary" disabled={!validation.valid || busy} onClick={optimize}>
        <Play size={18} /> Run Optimization
      </button>
    </div>
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
  return <Line points={points} color="#2563eb" />;
}

function SocLine({ schedule }) {
  return <Line points={schedule.map((row) => Number(row.soc_mwh))} color="#059669" />;
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
