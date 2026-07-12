"""Tests des indicateurs de gestion de portefeuille."""
import numpy as np
import pandas as pd
import pytest

from kanyon.analytics import ratios


def test_sharpe():
    assert ratios.sharpe(mean_perf=0.10, std_perf=0.05, risk_free=0.02) == pytest.approx(1.6)


def test_treynor():
    assert ratios.treynor(mean_perf=0.10, beta_value=0.5, risk_free=0.02) == pytest.approx(0.16)


def test_beta_avec_benchmark_identique():
    perf = pd.Series([0.01, -0.02, 0.03, 0.00, 0.015])
    # Un fonds répliquant exactement son benchmark a un bêta de 1.
    var_bench = float(np.cov(perf, perf)[0, 1])
    assert ratios.beta(perf, perf, var_bench) == pytest.approx(1.0)


def test_parametric_var_positive():
    assert ratios.parametric_var(std_perf=0.05, alpha=0.05) > 0


def test_historic_var_quantile():
    perf = pd.Series(range(100)) / 100.0  # 0.00 .. 0.99
    assert ratios.historic_var(perf, alpha=0.05) == pytest.approx(np.percentile(perf, 5))


def test_montecarlo_var_reproductible():
    a = ratios.montecarlo_var(0.0, 0.05, alpha=0.05, n_sims=1000, rng=np.random.default_rng(42))
    b = ratios.montecarlo_var(0.0, 0.05, alpha=0.05, n_sims=1000, rng=np.random.default_rng(42))
    assert a == pytest.approx(b)


def test_expected_shortfall_moyenne_queue():
    perf = pd.Series([-0.10, -0.08, -0.05, 0.0, 0.05, 0.10])
    # Moyenne des rendements <= -0.05.
    assert ratios.expected_shortfall(perf, -0.05) == pytest.approx((-0.10 - 0.08 - 0.05) / 3)


def test_compute_indicators_structure():
    rng = np.random.default_rng(0)
    frame = pd.DataFrame(
        {
            "perf": rng.normal(0.01, 0.05, 200),
            "perf_bench": rng.normal(0.008, 0.04, 200),
        }
    )
    registre = {"FND1": frame}
    result = ratios.compute_indicators(
        registre=registre,
        variances_bench={"FND1": frame["perf_bench"].var()},
        means={"FND1": frame["perf"].mean()},
        stds={"FND1": frame["perf"].std()},
        risk_free=0.002,
        seed=0,
    )
    assert list(result.index) == ["FND1"]
    for col in ("beta", "sharpe", "treynor", "pvar_99", "hvar_95", "simcvar_90"):
        assert col in result.columns
