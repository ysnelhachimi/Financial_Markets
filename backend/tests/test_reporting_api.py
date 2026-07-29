"""Tests de l'endpoint de reporting (factsheet)."""


def test_factsheet_exige_abonnement(auth_client):
    r = auth_client.post(
        "/api/reporting/factsheet",
        json={"fund_name": "F", "as_of": "2021-07-01", "holdings": []},
    )
    assert r.status_code == 402


def test_factsheet_html(auth_client):
    auth_client.post("/api/billing/subscribe", json={"plan_code": "free"})
    r = auth_client.post(
        "/api/reporting/factsheet",
        json={
            "fund_name": "OPCVM Oblig Maroc",
            "as_of": "2021-07-01",
            "category": "OMLT",
            "holdings": [
                {"ticker": "BDT10", "issuer": "TRESOR", "weight": 0.6, "asset_class": "obligataire", "sensitivity": 8.0},
                {"ticker": "OBLATW", "issuer": "ATW", "weight": 0.25, "asset_class": "obligataire", "sensitivity": 3.0},
                {"ticker": "CASH", "issuer": "CASH", "weight": 0.15, "asset_class": "monetaire"},
            ],
            "metrics": {"total_return": 0.08, "volatility": 0.05, "sharpe": 1.2},
        },
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    assert "OPCVM Oblig Maroc" in body
    assert "stress testing" in body.lower()
    assert "Conformité" in body


def test_factsheet_pdf(auth_client):
    auth_client.post("/api/billing/subscribe", json={"plan_code": "free"})
    r = auth_client.post(
        "/api/reporting/factsheet.pdf",
        json={
            "fund_name": "OPCVM Test",
            "as_of": "2021-07-01",
            "holdings": [{"ticker": "BDT10", "issuer": "TRESOR", "weight": 1.0, "sensitivity": 8.0}],
        },
    )
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert r.content[:5] == b"%PDF-"
