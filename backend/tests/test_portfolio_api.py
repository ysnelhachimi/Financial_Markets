"""Tests des endpoints du moteur de portefeuille (avec abonnement)."""


def _subscribe_free(client):
    client.post("/api/billing/subscribe", json={"plan_code": "free"})


def test_portfolio_exige_abonnement(auth_client):
    r = auth_client.post(
        "/api/portfolio/optimize",
        json={"tickers": ["A", "B"], "mean": [0.08, 0.1], "cov": [[0.04, 0.0], [0.0, 0.09]]},
    )
    assert r.status_code == 402


def test_optimize(auth_client):
    _subscribe_free(auth_client)
    r = auth_client.post(
        "/api/portfolio/optimize",
        json={
            "tickers": ["A", "B", "C"],
            "mean": [0.08, 0.10, 0.06],
            "cov": [[0.04, 0.01, 0.0], [0.01, 0.09, 0.01], [0.0, 0.01, 0.02]],
            "objective": "min_variance",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert abs(sum(body["weights"].values()) - 1.0) < 1e-3


def test_stress_defaut(auth_client):
    _subscribe_free(auth_client)
    r = auth_client.post(
        "/api/portfolio/stress",
        json={"weights": {"OT10": 1.0}, "durations": {"OT10": 8.0}, "betas": {}},
    )
    assert r.status_code == 200
    names = [s["name"] for s in r.json()]
    assert any("100pb" in n for n in names)


def test_compliance_breach(auth_client):
    _subscribe_free(auth_client)
    r = auth_client.post(
        "/api/portfolio/compliance",
        json={
            "holdings": [{"ticker": "A", "issuer": "EM1", "weight": 0.25}],
            "limits": {"max_per_issuer": 0.10},
        },
    )
    assert r.status_code == 200
    assert r.json()["compliant"] is False


def test_equities_optimize_sans_donnees(auth_client):
    _subscribe_free(auth_client)
    # Base de marché vide en test -> 404.
    r = auth_client.get("/api/portfolio/equities/optimize", params={"debut": "2021-01-01", "fin": "2021-12-31"})
    assert r.status_code == 404


def test_strategies_liste(auth_client):
    _subscribe_free(auth_client)
    r = auth_client.get("/api/portfolio/strategies")
    assert r.status_code == 200
    assert {s["code"] for s in r.json()} >= {"actions", "omlt", "oct", "monetaire", "diversifie"}
