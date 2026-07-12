"""Tests du moteur de portefeuille (fonctions pures)."""
import numpy as np
import pandas as pd
import pytest

from kanyon.portfolio import (
    optimize,
    target_between_curves,
    get_strategy,
    group_bounds_from_strategy,
    backtest,
    run_scenarios,
    StressScenario,
    check_compliance,
    Holding,
    ComplianceLimits,
)


# --- M3 construction ---
class TestConstruction:
    mean = [0.08, 0.10, 0.06]
    cov = [[0.04, 0.01, 0.00], [0.01, 0.09, 0.01], [0.00, 0.01, 0.02]]
    tickers = ["A", "B", "C"]

    def test_min_variance_somme_1(self):
        res = optimize(self.mean, self.cov, self.tickers, objective="min_variance")
        assert sum(res.weights.values()) == pytest.approx(1.0, abs=1e-4)
        assert all(-1e-6 <= w <= 1 + 1e-6 for w in res.weights.values())

    def test_min_variance_est_le_moins_volatil(self):
        mv = optimize(self.mean, self.cov, self.tickers, objective="min_variance")
        eq = optimize(self.mean, self.cov, self.tickers, objective="target_return", target_return=0.08)
        assert mv.volatility <= eq.volatility + 1e-6

    def test_max_sharpe(self):
        res = optimize(self.mean, self.cov, self.tickers, objective="max_sharpe", risk_free=0.02)
        assert res.sharpe > 0
        assert sum(res.weights.values()) == pytest.approx(1.0, abs=1e-4)

    def test_bornes_de_groupe(self):
        groups = {"g": (["A", "B"], 0.0, 0.5)}
        res = optimize(self.mean, self.cov, self.tickers, objective="min_variance", group_bounds=groups)
        assert res.weights["A"] + res.weights["B"] <= 0.5 + 1e-4

    def test_target_between_curves_atteint_sensibilite(self):
        res = target_between_curves(
            tickers=["OT2", "OT5", "OT10"],
            sensitivities=[1.9, 4.6, 8.5],
            rate_start={"OT2": 0.023, "OT5": 0.027, "OT10": 0.030},
            rate_end={"OT2": 0.025, "OT5": 0.030, "OT10": 0.035},
            target_sensitivity=5.0,
        )
        assert res.volatility == pytest.approx(5.0, abs=1e-2)  # sensibilité cible atteinte
        assert sum(res.weights.values()) == pytest.approx(1.0, abs=1e-4)


# --- M4 stratégies ---
class TestStrategies:
    def test_categories_connues(self):
        for code in ("actions", "diversifie", "omlt", "oct", "monetaire"):
            assert get_strategy(code).code == code

    def test_categorie_inconnue(self):
        with pytest.raises(KeyError):
            get_strategy("inexistant")

    def test_group_bounds(self):
        strat = get_strategy("monetaire")
        asset_class = {"A1": "monetaire", "A2": "obligataire", "A3": "actions"}
        groups = group_bounds_from_strategy(strat, asset_class)
        assert "monetaire" in groups and groups["monetaire"][1] == 0.70


# --- M5 backtest ---
class TestBacktest:
    def test_backtest_metriques(self):
        idx = pd.date_range("2021-01-01", periods=60, freq="D")
        rng = np.random.default_rng(0)
        prices = pd.DataFrame(
            {"A": 100 * (1 + rng.normal(0.001, 0.01, 60)).cumprod(),
             "B": 100 * (1 + rng.normal(0.0008, 0.008, 60)).cumprod()},
            index=idx,
        )
        res = backtest(prices, {"A": 0.5, "B": 0.5})
        assert res.nav.iloc[0] > 0
        assert -1.0 <= res.max_drawdown <= 0.0
        assert "total_return" in res.summary()

    def test_backtest_actif_inconnu(self):
        prices = pd.DataFrame({"A": [100, 101]})
        with pytest.raises(ValueError):
            backtest(prices, {"Z": 1.0})


# --- M6 stress ---
class TestStress:
    def test_choc_taux_fait_perdre_les_obligations(self):
        r = run_scenarios(
            weights={"OT10": 1.0},
            durations={"OT10": 8.0},
            betas={},
            scenarios=[StressScenario("+100pb", rate_shock_bps=100)],
        )
        # +100pb, duration 8 -> ~ -0.08
        assert r[0]["total_pnl"] == pytest.approx(-0.08, abs=1e-6)

    def test_choc_actions(self):
        r = run_scenarios(
            weights={"IAM": 1.0},
            durations={},
            betas={"IAM": 1.2},
            scenarios=[StressScenario("krach", equity_shock=-0.20)],
        )
        assert r[0]["total_pnl"] == pytest.approx(-0.24, abs=1e-6)


# --- M8 conformité ---
class TestCompliance:
    def test_portefeuille_conforme(self):
        holdings = [
            Holding("A", "EM1", 0.08, emprise=0.02),
            Holding("B", "EM2", 0.08, emprise=0.03),
            Holding("C", "EM3", 0.08, emprise=0.01),
        ]
        rep = check_compliance(holdings, ComplianceLimits())
        assert rep.compliant is True

    def test_depassement_plafond_emetteur(self):
        holdings = [Holding("A", "EM1", 0.25, emprise=0.02)]
        rep = check_compliance(holdings, ComplianceLimits(max_per_issuer=0.10))
        assert rep.compliant is False
        assert any(c.name == "Plafond par émetteur" and c.status == "breach" for c in rep.checks)

    def test_sensibilite_hors_bornes(self):
        holdings = [Holding("A", "EM1", 1.0, sensitivity=8.0)]
        rep = check_compliance(holdings, ComplianceLimits(sensitivity_bounds=(0.0, 0.5)))
        assert rep.compliant is False
