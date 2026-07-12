import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api.js";

const DEFAULT_BOND = {
  date_valeur: "2021-06-15",
  date_emission: "2015-06-15",
  date_jouissance: "2015-06-15",
  date_echeance: "2025-06-15",
  taux_facial: 0.03,
  taux_courbe: 0.03,
  nominal: 100,
};

export default function Pricer() {
  const [bond, setBond] = useState(DEFAULT_BOND);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [paywall, setPaywall] = useState(false);
  const [busy, setBusy] = useState(false);

  const update = (field) => (e) => {
    const value = e.target.type === "number" ? parseFloat(e.target.value) : e.target.value;
    setBond({ ...bond, [field]: value });
  };

  const submit = async (e) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    setPaywall(false);
    try {
      setResult(await api.priceBond(bond));
    } catch (err) {
      if (err.status === 402) setPaywall(true);
      else setError(err.message);
      setResult(null);
    } finally {
      setBusy(false);
    }
  };

  return (
    <section>
      <h2>Pricer obligataire</h2>
      <p className="muted">
        Valorisation d'une obligation à taux fixe (marché marocain) : prix pied de
        coupon, coupon couru, prix plein.
      </p>

      {paywall && (
        <div className="paywall">
          <strong>Abonnement requis.</strong> <Link to="/billing">Voir les formules</Link>
        </div>
      )}
      {error && <p className="error">{error}</p>}

      <div className="pricer-grid">
        <form className="pricer-form" onSubmit={submit}>
          <label>Date de valeur<input type="date" value={bond.date_valeur} onChange={update("date_valeur")} /></label>
          <label>Date d'émission<input type="date" value={bond.date_emission} onChange={update("date_emission")} /></label>
          <label>Date de jouissance<input type="date" value={bond.date_jouissance} onChange={update("date_jouissance")} /></label>
          <label>Date d'échéance<input type="date" value={bond.date_echeance} onChange={update("date_echeance")} /></label>
          <label>Taux facial (décimal)<input type="number" step="0.001" value={bond.taux_facial} onChange={update("taux_facial")} /></label>
          <label>Taux de courbe (décimal)<input type="number" step="0.001" value={bond.taux_courbe} onChange={update("taux_courbe")} /></label>
          <label>Nominal<input type="number" step="1" value={bond.nominal} onChange={update("nominal")} /></label>
          <button className="btn" type="submit" disabled={busy}>{busy ? "…" : "Valoriser"}</button>
        </form>

        <div className="pricer-result">
          {result ? (
            <>
              <div className="result-price">{result.price.toLocaleString("fr-MA", { minimumFractionDigits: 3 })}</div>
              <p className="muted">Prix pied de coupon</p>
              <ul className="kv">
                <li><span>Prix plein (dirty)</span><strong>{result.dirty_price}</strong></li>
                <li><span>Coupon couru</span><strong>{result.coupon_couru}</strong></li>
                <li><span>Taux de courbe</span><strong>{result.taux_courbe}</strong></li>
                <li><span>Type</span><strong>{result.type}</strong></li>
                <li><span>Maturité résiduelle</span><strong>{result.maturite_residuelle} j</strong></li>
                <li><span>Maturité initiale</span><strong>{result.maturite_initiale} j</strong></li>
              </ul>
            </>
          ) : (
            <p className="muted">Renseignez les caractéristiques du titre puis lancez la valorisation.</p>
          )}
        </div>
      </div>
    </section>
  );
}
