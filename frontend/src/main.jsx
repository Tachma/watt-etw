import React, { useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  BatteryCharging,
  CalendarDays,
  CheckCircle2,
  FileUp,
  Gauge,
  Play,
  Plus,
  Trash2,
  UploadCloud
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

const previewBars = [
  34, 30, 28, 32, 39, 55, 72, 69, 58, 46, 42, 48,
  61, 70, 74, 63, 52, 49, 57, 76, 82, 67, 50, 38
];

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
      const data = await parseJsonResponse(response, "Could not validate file");
      if (!response.ok) throw new Error(data.detail || "Could not validate file.");
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
      const batteryPayload = batteries.map((battery, index) =>
        normalizeBatteryPayload({ ...defaultBattery(index + 1), ...battery })
      );

      // Extract prices from the uploaded market rows for the selected date
      const sorted = [...selectedRows].sort((a, b) =>
        a.timestamp < b.timestamp ? -1 : 1
      );
      const priceValues = sorted.map((r) => Number(r.price_eur_mwh));

      // Determine resolution: 96 = 15-min, 24 = hourly, otherwise aggregate
      let pricePayload;
      if (priceValues.length === 96) {
        pricePayload = { prices_15min: priceValues };
      } else if (priceValues.length === 24) {
        pricePayload = { prices_hourly: priceValues };
      } else if (priceValues.length > 0) {
        // Aggregate to 24 hourly by averaging groups
        const perHour = Math.floor(priceValues.length / 24);
        const hourly = Array.from({ length: 24 }, (_, h) => {
          const chunk = priceValues.slice(h * perHour, (h + 1) * perHour);
          return chunk.reduce((s, v) => s + v, 0) / chunk.length;
        });
        pricePayload = { prices_hourly: hourly };
      } else {
        throw new Error("No market data for the selected date.");
      }

      const response = await fetch("/api/optimize-arbitrage", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          batteries: batteryPayload,
          ...pricePayload
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
          <button className={step === "import" ? "active" : ""} onClick={() => setStep("import")}>Import</button>
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

function Landing({ onStart }) {
  return (
    <section className="landing">
      <div className="hero">
        <div className="heroCopy">
          <p className="eyebrow">Greek DAM battery arbitrage</p>
          <h2>From HEnEx DAM prices to a battery dispatch schedule in minutes.</h2>
          <p>
            Configure a battery fleet, upload HEnEx-style market data, and calculate a feasible
            charge/discharge plan with profit, cost, and SOC.
          </p>
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
    <div className="marketPreview" aria-label="Example optimization preview">
      <div className="previewHeader">
        <div>
          <span>Optimization preview</span>
          <strong>DAM price signal</strong>
        </div>
        <span className="liveTag">Ready</span>
      </div>
      <div className="previewStats">
        <span><Gauge size={16} /> Price spread <strong>54.2 EUR/MWh</strong></span>
        <span><BatteryCharging size={16} /> Final SOC <strong>38 MWh</strong></span>
      </div>
      <div className="priceWindow">
        {previewBars.map((value, index) => {
          const action = index < 6 ? "charge" : index > 18 && index < 22 ? "discharge" : "";
          return (
            <span
              className={`priceBar ${action}`}
              key={`${value}-${index}`}
              style={{ "--height": `${value}%` }}
            />
          );
        })}
      </div>
      <div className="previewFooter">
        <span><UploadCloud size={15} /> HEnEx-style upload</span>
        <span><CalendarDays size={15} /> Daily schedule</span>
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
