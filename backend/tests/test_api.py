"""Tests de bout en bout de l'API (auth, plans, mur payant, paiement, pricer)."""
import pytest


def test_health(client):
    assert client.get("/api/health").json()["status"] == "ok"


def test_register_login_me(client):
    r = client.post("/api/auth/register", json={"email": "a@b.com", "password": "password123"})
    assert r.status_code == 201

    # Doublon rejeté.
    r2 = client.post("/api/auth/register", json={"email": "a@b.com", "password": "password123"})
    assert r2.status_code == 409

    login = client.post("/api/auth/login", data={"username": "a@b.com", "password": "password123"})
    assert login.status_code == 200
    token = login.json()["access_token"]

    me = client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.json()["email"] == "a@b.com"


def test_login_mauvais_mdp(client):
    client.post("/api/auth/register", json={"email": "c@d.com", "password": "password123"})
    r = client.post("/api/auth/login", data={"username": "c@d.com", "password": "faux"})
    assert r.status_code == 401


def test_plans_amorces(client):
    codes = {p["code"] for p in client.get("/api/plans").json()}
    assert {"free", "premium", "pro"} <= codes


def test_mur_payant_bloque_sans_abonnement(auth_client):
    # Essai désactivé (trial=0) => pas d'accès.
    r = auth_client.get("/api/market/indices", params={"debut": "2021-01-01", "fin": "2021-12-31"})
    assert r.status_code == 402


def test_plan_gratuit_ouvre_acces(auth_client):
    sub = auth_client.post("/api/billing/subscribe", json={"plan_code": "free"})
    assert sub.status_code == 200  # activation immédiate, pas de paiement

    r = auth_client.get("/api/market/indices", params={"debut": "2021-01-01", "fin": "2021-12-31"})
    assert r.status_code == 200
    assert r.json() == []  # base de données de marché vide


def test_paiement_premium_factice(auth_client):
    # Initie l'abonnement premium => renvoie un formulaire de paiement.
    checkout = auth_client.post("/api/billing/subscribe", json={"plan_code": "premium"})
    assert checkout.status_code == 200
    data = checkout.json()
    oid = data["provider_ref"]

    # Toujours pas d'accès tant que le paiement n'est pas confirmé.
    assert auth_client.get(
        "/api/market/courbe", params={"date_marche": "2021-07-01"}
    ).status_code == 402

    # Simule le callback de paiement réussi (sans suivre la redirection frontend).
    cb = auth_client.post(
        "/api/billing/fake/callback", data={"oid": oid, "status": "success"}, follow_redirects=False
    )
    assert cb.status_code in (200, 303)

    # Accès désormais ouvert.
    assert auth_client.get(
        "/api/market/courbe", params={"date_marche": "2021-07-01"}
    ).status_code == 200


def test_pricer_price_obligation_au_pair(auth_client):
    # Le pricer exige un abonnement : on active d'abord le plan gratuit.
    auth_client.post("/api/billing/subscribe", json={"plan_code": "free"})
    resp = auth_client.post(
        "/api/pricer/price",
        json={
            "date_valeur": "2021-06-15",
            "date_emission": "2015-06-15",
            "date_jouissance": "2015-06-15",
            "date_echeance": "2025-06-15",
            "taux_facial": 0.03,
            "taux_courbe": 0.03,
            "nominal": 100,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["price"] == pytest.approx(100.0, abs=0.5)
    assert body["type"] == "obligation_ordinaire"


def test_pricer_exige_abonnement(auth_client):
    # Sans abonnement actif (essai désactivé en test) => 402.
    resp = auth_client.post(
        "/api/pricer/price",
        json={
            "date_valeur": "2021-06-15",
            "date_emission": "2015-06-15",
            "date_jouissance": "2015-06-15",
            "date_echeance": "2025-06-15",
            "taux_facial": 0.03,
            "taux_courbe": 0.03,
            "nominal": 100,
        },
    )
    assert resp.status_code == 402


def test_callback_signature_invalide_refuse(auth_client):
    checkout = auth_client.post("/api/billing/subscribe", json={"plan_code": "premium"}).json()
    oid = checkout["provider_ref"]
    # status != success => refus, pas d'activation.
    auth_client.post(
        "/api/billing/fake/callback", data={"oid": oid, "status": "echec"}, follow_redirects=False
    )
    assert auth_client.get(
        "/api/market/courbe", params={"date_marche": "2021-07-01"}
    ).status_code == 402
