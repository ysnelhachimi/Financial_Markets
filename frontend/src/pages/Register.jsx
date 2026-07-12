import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext.jsx";

export default function Register() {
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(false);
  const { register } = useAuth();
  const navigate = useNavigate();

  const submit = async (e) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await register(email, password, fullName);
      navigate("/dashboard");
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="auth-card">
      <h2>Créer un compte</h2>
      <p className="muted">Essai gratuit inclus, sans carte bancaire.</p>
      {error && <p className="error">{error}</p>}
      <form onSubmit={submit}>
        <label>Nom complet
          <input value={fullName} onChange={(e) => setFullName(e.target.value)} />
        </label>
        <label>Email
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
        </label>
        <label>Mot de passe (8 caractères min.)
          <input type="password" minLength={8} value={password} onChange={(e) => setPassword(e.target.value)} required />
        </label>
        <button className="btn" type="submit" disabled={busy}>{busy ? "…" : "Créer mon compte"}</button>
      </form>
      <p className="muted">Déjà inscrit ? <Link to="/login">Se connecter</Link></p>
    </div>
  );
}
