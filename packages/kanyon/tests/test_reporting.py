"""Tests du reporting (factsheet HTML)."""
from kanyon.reporting import FactsheetContext, render_factsheet_html


def _ctx():
    return FactsheetContext(
        fund_name="OPCVM Actions Maroc",
        as_of="2021-07-01",
        category="Actions",
        allocation=[{"label": "Actions", "weight": 0.85}, {"label": "Monétaire", "weight": 0.15}],
        metrics={"total_return": 0.123, "volatility": 0.18, "sharpe": 0.9, "max_drawdown": -0.12},
        stress=[{"name": "Krach actions -20%", "total_pnl": -0.17}],
        compliance={
            "compliant": False,
            "checks": [{"name": "Plafond par émetteur", "status": "breach", "value": 0.25, "limit": 0.10}],
        },
        notes="Portefeuille de démonstration.",
    )


def test_render_contient_les_sections():
    html = render_factsheet_html(_ctx())
    assert "<!doctype html>" in html.lower()
    assert "OPCVM Actions Maroc" in html
    assert "Allocation" in html and "Actions" in html
    assert "stress testing" in html.lower()
    assert "Conformité" in html
    # Statut de conformité rendu.
    assert "Non conforme" in html
    # Pourcentages formatés (espace insécable avant %).
    assert "12.30" in html and "%" in html  # performance 0.123


def test_render_sans_donnees_optionnelles():
    ctx = FactsheetContext(fund_name="Vide", as_of="2021-01-01")
    html = render_factsheet_html(ctx)
    assert "Vide" in html
    assert "Non disponible" in html  # sections vides gérées
