import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api.js";
import { useAuth } from "../context/AuthContext.jsx";
import { postToPaymentGateway, formatPrice } from "../checkout.js";

export default function Home() {
  const [plans, setPlans] = useState([]);
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(null);
  const { user, refresh } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    api.plans().then(setPlans).catch((e) => setError(e.message));
  }, []);

  const choose = async (planCode) => {
    if (!user) {
      navigate("/register");
      return;
    }
    setBusy(planCode);
    setError(null);
    try {
      const checkout = await api.subscribe(planCode);
      if (checkout && checkout.payment_url) {
        postToPaymentGateway(checkout); // redirection CMI
      } else {
        await refresh(); // plan gratuit activé
        navigate("/dashboard");
      }
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(null);
    }
  };

  return (
    <section>
      <div className="hero">
        <h1>Les marchés financiers marocains, en données exploitables.</h1>
        <p className="lead">
          Indices MASI, volumes, courbe des taux BKAM et pricing obligataire —
          via une API et un tableau de bord, sur abonnement mensuel.
        </p>
      </div>

      {error && <p className="error">{error}</p>}

      <h2 className="section-title">Nos formules</h2>
      <div className="plans">
        {plans.map((plan) => (
          <div className={`plan-card ${plan.code === "premium" ? "highlight" : ""}`} key={plan.id}>
            {plan.code === "premium" && <span className="badge">Populaire</span>}
            <h3>{plan.name}</h3>
            <p className="price">{formatPrice(plan.price_cents, plan.currency)}</p>
            <p className="muted">{plan.description}</p>
            <ul className="features">
              <li>{plan.daily_quota ? `${plan.daily_quota} requêtes/jour` : "Requêtes illimitées"}</li>
              <li>Accès API + tableau de bord</li>
              {plan.code !== "free" && <li>Courbe des taux & pricing</li>}
            </ul>
            <button className="btn" disabled={busy === plan.code} onClick={() => choose(plan.code)}>
              {busy === plan.code ? "…" : plan.price_cents ? "S'abonner" : "Commencer"}
            </button>
          </div>
        ))}
      </div>
    </section>
  );
}
