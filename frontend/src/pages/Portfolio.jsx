import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api.js";

const DEFAULT_ASSETS = [
  { ticker: "IAM", ret: 8, vol: 20 },
  { ticker: "ATW", ret: 10, vol: 30 },
  { ticker: "BCP", ret: 6, vol: 15 },
];

const DEFAULT_HOLDINGS = [
  { ticker: "BDT-10A", issuer: "TRESOR", weight: 25 },
  { ticker: "OBL-ATW", issuer: "ATTIJARIWAFA", weight: 12 },
  { ticker: "OBL-BCP", issuer: "BCP", weight: 8 },
];

function useApiCall() {
  const [error, setError] = useState(null);
  const [paywall, setPaywall] = useState(false);
  const run = async (fn) => {
    setError(null);
    setPaywall(false);
    try {
      return await fn();
    } catch (e) {
      if (e.status === 402) setPaywall(true);
      else setError(e.message);
      return null;
    }
  };
  return { error, paywall, run };
}

export default function Portfolio() {
  const [assets, setAssets] = useState(DEFAULT_ASSETS);
  const [objective, setObjective] = useState("min_variance");
  const [optResult, setOptResult] = useState(null);

  const [stress, setStress] = useState(null);
  const [holdings, setHoldings] = useState(DEFAULT_HOLDINGS);
  const [compliance, setCompliance] = useState(null);

  const { error, paywall, run } = useApiCall();

  const optimize = async () => {
    const tickers = assets.map((a) => a.ticker);
    const mean = assets.map((a) => a.ret / 100);
    // Covariance diagonale (corrélations supposées nulles pour la démo).
    const cov = assets.map((a, i) => assets.map((_, j) => (i === j ? (a.vol / 100) ** 2 : 0)));
    const res = await run(() => api.optimize({ tickers, mean, cov, objective, risk_free: 0.02 }));
    if (res) setOptResult(res);
  };

  const runStress = async () => {
    const res = await run(() =>
      api.stress({ weights: { OT10: 0.6, OT2: 0.4 }, durations: { OT10: 8.5, OT2: 1.9 }, betas: {} })
    );
    if (res) setStress(res);
  };

  const runCompliance = async () => {
    const res = await run(() =>
      api.compliance({
        holdings: holdings.map((h) => ({ ticker: h.ticker, issuer: h.issuer, weight: h.weight / 100 })),
        limits: { max_per_issuer: 0.1, concentration_cap: 0.4 },
      })
    );
    if (res) setCompliance(res);
  };

  const factsheetPayload = () => ({
    fund_name: "Portefeuille Kanyon",
    as_of: new Date().toISOString().slice(0, 10),
    category: "Diversifié",
    holdings: holdings.map((h) => ({
      ticker: h.ticker,
      issuer: h.issuer,
      weight: h.weight / 100,
      asset_class: "obligataire",
    })),
    metrics: optResult
      ? { volatility: optResult.volatility, sharpe: optResult.sharpe, total_return: optResult.expected_return }
      : {},
  });

  const generateFactsheet = async () => {
    const html = await run(() => api.factsheetHtml(factsheetPayload()));
    if (html) window.open(URL.createObjectURL(new Blob([html], { type: "text/html" })), "_blank");
  };

  const downloadPdf = async () => {
    const blob = await run(() => api.factsheetPdf(factsheetPayload()));
    if (blob) {
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = "factsheet-kanyon.pdf";
      a.click();
    }
  };

  const [liveRange, setLiveRange] = useState({ debut: "2021-04-06", fin: "2021-07-01" });
  const [liveResult, setLiveResult] = useState(null);
  const optimizeLive = async () => {
    const res = await run(() => api.optimizeEquities(liveRange.debut, liveRange.fin, "max_sharpe"));
    if (res) setLiveResult(res);
  };

  const updateAsset = (i, field) => (e) => {
    const next = [...assets];
    next[i] = { ...next[i], [field]: field === "ticker" ? e.target.value : parseFloat(e.target.value) };
    setAssets(next);
  };
  const updateHolding = (i, field) => (e) => {
    const next = [...holdings];
    next[i] = { ...next[i], [field]: field === "weight" ? parseFloat(e.target.value) : e.target.value };
    setHoldings(next);
  };

  return (
    <section>
      <h2>Gestion de portefeuille</h2>
      <p className="muted">Optimisation, stress testing et conformité prudentielle (AMMC).</p>
      {paywall && (
        <div className="paywall"><strong>Abonnement requis.</strong> <Link to="/billing">Voir les formules</Link></div>
      )}
      {error && <p className="error">{error}</p>}

      {/* Optimisation sur données réelles MASI */}
      <div className="card">
        <h3>Optimisation sur données réelles (Bourse de Casablanca)</h3>
        <p className="muted">Univers actions MASI en base, estimation rendement/risque sur la période, max-Sharpe.</p>
        <div className="toolbar">
          <input type="date" value={liveRange.debut} onChange={(e) => setLiveRange({ ...liveRange, debut: e.target.value })} />
          <input type="date" value={liveRange.fin} onChange={(e) => setLiveRange({ ...liveRange, fin: e.target.value })} />
          <button className="btn btn-small" onClick={optimizeLive}>Optimiser (données réelles)</button>
        </div>
        {liveResult && (
          <div className="table-wrap">
            <table>
              <thead><tr><th>Valeur</th><th>Poids optimal</th></tr></thead>
              <tbody>
                {Object.entries(liveResult.result.weights)
                  .filter(([, w]) => w > 0.001)
                  .sort((a, b) => b[1] - a[1])
                  .map(([t, w]) => (
                    <tr key={t}><td>{t}</td><td>{(w * 100).toFixed(1)} %</td></tr>
                  ))}
              </tbody>
            </table>
            <p className="muted">
              {liveResult.observations} séances · Rendement {(liveResult.result.expected_return * 100).toFixed(1)} % ·
              Volatilité {(liveResult.result.volatility * 100).toFixed(1)} % · Sharpe {liveResult.result.sharpe.toFixed(2)}
            </p>
          </div>
        )}
      </div>

      {/* Optimisation (saisie manuelle) */}
      <div className="card">
        <h3>Optimisation d'allocation (saisie manuelle)</h3>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Valeur</th><th>Rendement att. (%)</th><th>Volatilité (%)</th><th>Poids optimal</th></tr></thead>
            <tbody>
              {assets.map((a, i) => (
                <tr key={i}>
                  <td><input value={a.ticker} onChange={updateAsset(i, "ticker")} style={{ width: 90 }} /></td>
                  <td><input type="number" step="0.5" value={a.ret} onChange={updateAsset(i, "ret")} style={{ width: 90 }} /></td>
                  <td><input type="number" step="0.5" value={a.vol} onChange={updateAsset(i, "vol")} style={{ width: 90 }} /></td>
                  <td>{optResult ? `${(optResult.weights[a.ticker] * 100).toFixed(1)} %` : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="toolbar">
          <select value={objective} onChange={(e) => setObjective(e.target.value)}>
            <option value="min_variance">Variance minimale</option>
            <option value="max_sharpe">Sharpe maximal</option>
          </select>
          <button className="btn btn-small" onClick={optimize}>Optimiser</button>
        </div>
        {optResult && (
          <p className="muted">
            Rendement {(optResult.expected_return * 100).toFixed(2)} % · Volatilité{" "}
            {(optResult.volatility * 100).toFixed(2)} % · Sharpe {optResult.sharpe.toFixed(2)}
          </p>
        )}
      </div>

      {/* Stress */}
      <div className="card">
        <h3>Stress testing (portefeuille obligataire 60/40 OT10/OT2)</h3>
        <button className="btn btn-small" onClick={runStress}>Lancer les scénarios</button>
        {stress && (
          <div className="table-wrap" style={{ marginTop: ".8rem" }}>
            <table>
              <thead><tr><th>Scénario</th><th>Taux</th><th>Actions</th><th>P&L total</th></tr></thead>
              <tbody>
                {stress.map((s, i) => (
                  <tr key={i}>
                    <td>{s.name}</td>
                    <td>{(s.rate_pnl * 100).toFixed(2)} %</td>
                    <td>{(s.equity_pnl * 100).toFixed(2)} %</td>
                    <td style={{ color: s.total_pnl < 0 ? "#e5484d" : "#38b27b" }}>
                      {(s.total_pnl * 100).toFixed(2)} %
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Conformité */}
      <div className="card">
        <h3>Conformité prudentielle (AMMC)</h3>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Ligne</th><th>Émetteur</th><th>Poids (%)</th></tr></thead>
            <tbody>
              {holdings.map((h, i) => (
                <tr key={i}>
                  <td><input value={h.ticker} onChange={updateHolding(i, "ticker")} style={{ width: 110 }} /></td>
                  <td><input value={h.issuer} onChange={updateHolding(i, "issuer")} style={{ width: 140 }} /></td>
                  <td><input type="number" step="1" value={h.weight} onChange={updateHolding(i, "weight")} style={{ width: 80 }} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="toolbar">
          <button className="btn btn-small" onClick={runCompliance}>Contrôler</button>
          <button className="btn btn-small btn-outline" onClick={generateFactsheet}>Fiche HTML</button>
          <button className="btn btn-small btn-outline" onClick={downloadPdf}>Télécharger le PDF</button>
        </div>
        {compliance && (
          <div>
            <p><strong style={{ color: compliance.compliant ? "#38b27b" : "#e5484d" }}>
              {compliance.compliant ? "Conforme ✓" : "Non conforme ✗"}
            </strong></p>
            <ul className="kv">
              {compliance.checks.map((c, i) => (
                <li key={i}>
                  <span>{c.name} {c.detail ? `(${c.detail})` : ""}</span>
                  <strong style={{ color: c.status === "ok" ? "#38b27b" : "#e5484d" }}>
                    {(c.value * 100).toFixed(1)} % / {(c.limit * 100).toFixed(0)} % · {c.status === "ok" ? "OK" : "DÉPASSÉ"}
                  </strong>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </section>
  );
}
