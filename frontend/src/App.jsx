import { Link, Outlet, useNavigate } from "react-router-dom";
import { useAuth } from "./context/AuthContext.jsx";

export default function App() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate("/");
  };

  return (
    <div className="app">
      <header className="nav">
        <Link to="/" className="brand">Kanyon<span>Markets</span></Link>
        <nav>
          <Link to="/">Tarifs</Link>
          {user ? (
            <>
              <Link to="/dashboard">Tableau de bord</Link>
              <Link to="/pricer">Pricer</Link>
              <Link to="/portfolio">Portefeuille</Link>
              <Link to="/billing">Abonnement</Link>
              <button className="link" onClick={handleLogout}>Déconnexion</button>
            </>
          ) : (
            <>
              <Link to="/login">Connexion</Link>
              <Link to="/register" className="btn btn-small">Créer un compte</Link>
            </>
          )}
        </nav>
      </header>

      <main className="container">
        <Outlet />
      </main>

      <footer className="footer">
        <span>© {new Date().getFullYear()} Kanyon Markets — données des marchés financiers marocains.</span>
      </footer>
    </div>
  );
}
