import { useEffect, useState } from "react";
import { api } from "../api.js";
import { useAuth } from "../context/AuthContext.jsx";
import { postToPaymentGateway, formatPrice } from "../checkout.js";

export default function Billing() {
  const { subscription, refresh } = useAuth();
  const [plans, setPlans] = useState([]);
  const [busy, setBusy] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    api.plans().then(setPlans).catch((e) => setError(e.message));
  }, []);

  const subscribe = async (planCode) => {
    setBusy(planCode);
    setError(null);
    try {
      const checkout = await api.subscribe(planCode);
      if (checkout && checkout.payment_url) postToPaymentGateway(checkout);
      else await refresh();
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(null);
    }
  };

  const cancel = async () => {
    setBusy("cancel");
    try {
      await api.cancel();
      await refresh();
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(null);
    }
  };

  return (
    <section>
      <h2>Mon abonnement</h2>
      {error && <p className="error">{error}</p>}

      {subscription ? (
        <div className="sub-summary">
          <p>
            Formule actuelle : <strong>{subscription.plan.name}</strong> — statut{" "}
            <strong>{subscription.status}</strong>, valable jusqu'au{" "}
            {new Date(subscription.current_period_end).toLocaleDateString("fr-MA")}.
          </p>
          {subscription.cancel_at_period_end ? (
            <p className="muted">Le renouvellement est annulé ; l'accès reste actif jusqu'à échéance.</p>
          ) : (
            <button className="btn btn-outline btn-small" onClick={cancel} disabled={busy === "cancel"}>
              {busy === "cancel" ? "…" : "Annuler le renouvellement"}
            </button>
          )}
        </div>
      ) : (
        <p className="muted">Aucun abonnement actif.</p>
      )}

      <h3 className="section-title">Changer de formule</h3>
      <div className="plans">
        {plans.map((plan) => {
          const current = subscription && subscription.plan.code === plan.code;
          return (
            <div className={`plan-card ${current ? "highlight" : ""}`} key={plan.id}>
              <h3>{plan.name}</h3>
              <p className="price">{formatPrice(plan.price_cents, plan.currency)}</p>
              <p className="muted">{plan.description}</p>
              <button
                className="btn"
                disabled={busy === plan.code || current}
                onClick={() => subscribe(plan.code)}
              >
                {current ? "Formule actuelle" : busy === plan.code ? "…" : plan.price_cents ? "Choisir" : "Passer au gratuit"}
              </button>
            </div>
          );
        })}
      </div>
    </section>
  );
}
