import { useEffect } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "../context/AuthContext.jsx";

// Page de retour après paiement (URLs okUrl / failUrl du prestataire).
export default function BillingResult({ ok }) {
  const { refresh } = useAuth();

  useEffect(() => {
    if (ok) refresh(); // récupère l'abonnement mis à jour
  }, [ok, refresh]);

  return (
    <div className="auth-card center">
      {ok ? (
        <>
          <h2>Paiement confirmé ✅</h2>
          <p className="muted">Votre abonnement est actif. Merci !</p>
          <Link className="btn" to="/dashboard">Accéder au tableau de bord</Link>
        </>
      ) : (
        <>
          <h2>Paiement non abouti ❌</h2>
          <p className="muted">La transaction n'a pas pu être finalisée.</p>
          <Link className="btn btn-outline" to="/billing">Réessayer</Link>
        </>
      )}
    </div>
  );
}
