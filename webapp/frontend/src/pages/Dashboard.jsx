import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api.js";
import { useAuth } from "../context/AuthContext.jsx";

const RESOURCES = [
  { key: "indices", label: "Indices MASI", params: ["debut", "fin"] },
  { key: "volumes", label: "Volumes par valeur", params: ["debut", "fin"] },
  { key: "composition", label: "Composition MASI", params: ["debut", "fin"] },
  { key: "monetaire", label: "Marché monétaire", params: ["debut", "fin"] },
  { key: "tenors", label: "Courbe des tenors", params: ["debut", "fin"] },
  { key: "courbe", label: "Courbe des taux", params: ["date_marche"] },
];

export default function Dashboard() {
  const { user, subscription } = useAuth();
  const [resource, setResource] = useState(RESOURCES[0]);
  const [form, setForm] = useState({ debut: "2021-01-01", fin: "2021-12-31", date_marche: "2021-07-01" });
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);
  const [paywall, setPaywall] = useState(false);
  const [busy, setBusy] = useState(false);

  const load = async () => {
    setBusy(true);
    setError(null);
    setPaywall(false);
    try {
      const params = Object.fromEntries(resource.params.map((p) => [p, form[p]]));
      const data = await api.market(resource.key, params);
      setRows(data);
    } catch (e) {
      if (e.status === 402) setPaywall(true);
      else setError(e.message);
      setRows(null);
    } finally {
      setBusy(false);
    }
  };

  const columns = rows && rows.length ? Object.keys(rows[0]) : [];

  return (
    <section>
      <div className="dash-head">
        <div>
          <h2>Tableau de bord</h2>
          <p className="muted">Bonjour {user.full_name || user.email}.</p>
        </div>
        {subscription && (
          <div className="sub-pill">
            {subscription.plan.name} · {subscription.status}
            <span className="muted"> jusqu'au {new Date(subscription.current_period_end).toLocaleDateString("fr-MA")}</span>
          </div>
        )}
      </div>

      {paywall && (
        <div className="paywall">
          <strong>Abonnement requis.</strong> Votre accès a expiré ou n'est pas actif.{" "}
          <Link to="/billing">Voir les formules</Link>
        </div>
      )}

      <div className="toolbar">
        <select
          value={resource.key}
          onChange={(e) => setResource(RESOURCES.find((r) => r.key === e.target.value))}
        >
          {RESOURCES.map((r) => <option key={r.key} value={r.key}>{r.label}</option>)}
        </select>
        {resource.params.includes("debut") && (
          <>
            <input type="date" value={form.debut} onChange={(e) => setForm({ ...form, debut: e.target.value })} />
            <input type="date" value={form.fin} onChange={(e) => setForm({ ...form, fin: e.target.value })} />
          </>
        )}
        {resource.params.includes("date_marche") && (
          <input type="date" value={form.date_marche} onChange={(e) => setForm({ ...form, date_marche: e.target.value })} />
        )}
        <button className="btn btn-small" onClick={load} disabled={busy}>{busy ? "…" : "Charger"}</button>
      </div>

      {error && <p className="error">{error}</p>}

      {rows && rows.length === 0 && <p className="muted">Aucune donnée sur cette période.</p>}

      {rows && rows.length > 0 && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
            </thead>
            <tbody>
              {rows.slice(0, 200).map((row, i) => (
                <tr key={i}>{columns.map((c) => <td key={c}>{String(row[c])}</td>)}</tr>
              ))}
            </tbody>
          </table>
          {rows.length > 200 && <p className="muted">{rows.length} lignes — 200 affichées.</p>}
        </div>
      )}
    </section>
  );
}
