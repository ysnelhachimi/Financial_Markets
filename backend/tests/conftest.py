"""Fixtures de test du backend.

Configure l'environnement (provider factice, bases SQLite temporaires, essai
gratuit désactivé) *avant* l'import de l'application, puis fournit un client de
test FastAPI.
"""
import os
import tempfile

import pytest

# --- Configuration d'environnement (avant tout import de app.*) ---
_TMP = tempfile.mkdtemp(prefix="kweb-tests-")
os.environ.update(
    {
        "KWEB_PAYMENT_PROVIDER": "fake",
        "KWEB_SECRET_KEY": "test-secret",
        "KWEB_TRIAL_DAYS": "0",  # pas d'essai => teste le mur payant
        "KWEB_DATABASE_URL": f"sqlite:///{_TMP}/web.db",
        "KWEB_PUBLIC_BASE_URL": "http://testserver",
        "KWEB_FRONTEND_BASE_URL": "http://frontend",
        "KANYON_DATABASE_URL": f"sqlite:///{_TMP}/data.db",
    }
)


@pytest.fixture(scope="session", autouse=True)
def _prepare_kanyon_schema():
    """Crée les tables de données kanyon (vides) pour les endpoints market."""
    from kanyon.db.base import Base, get_engine
    from kanyon.db import models  # noqa: F401

    Base.metadata.create_all(bind=get_engine())
    yield


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient

    from app import models  # noqa: F401  (enregistre les modèles)
    from app.database import Base, engine, init_db
    from app.main import app

    # Isolation : repart d'une base web vierge à chaque test.
    Base.metadata.drop_all(bind=engine)
    init_db()  # recrée les tables et amorce les plans
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def auth_client(client):
    """Client authentifié (utilisateur fraîchement inscrit, sans abonnement actif)."""
    email = "user@example.com"
    client.post("/api/auth/register", json={"email": email, "password": "motdepasse123"})
    resp = client.post("/api/auth/login", data={"username": email, "password": "motdepasse123"})
    token = resp.json()["access_token"]
    client.headers.update({"Authorization": f"Bearer {token}"})
    return client
