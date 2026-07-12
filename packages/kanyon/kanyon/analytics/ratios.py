"""Indicateurs de gestion de portefeuille.

Ce module remplace l'ancien fragment ``Ratios of portfolio management`` (non
exécutable : il référençait des variables globales jamais définies). Il fournit
des fonctions autonomes et testables pour :

* les ratios de performance ajustée au risque : bêta, Sharpe, Treynor ;
* la *Value at Risk* (VaR) paramétrique (loi normale), historique et Monte-Carlo ;
* l'*Expected Shortfall* (CVaR).

Convention : les rendements (``perf``) sont exprimés en variations (par ex. en
pourcentage). Les seuils de confiance usuels sont fournis dans
:data:`CONFIDENCE_LEVELS` (1 %, 5 %, 10 %).
"""
from __future__ import annotations

from typing import Dict, Mapping

import numpy as np
import pandas as pd
from scipy import stats

# Seuils (alpha) des niveaux de confiance 99 %, 95 %, 90 %.
CONFIDENCE_LEVELS = {"99": 0.01, "95": 0.05, "90": 0.10}

# Nombre de tirages par défaut pour la VaR de Monte-Carlo.
DEFAULT_N_SIMS = 500


def beta(perf: pd.Series, perf_bench: pd.Series, var_bench: float) -> float:
    """Bêta d'un fonds relativement à son benchmark.

    Args:
        perf: Série des rendements du fonds.
        perf_bench: Série des rendements du benchmark.
        var_bench: Variance des rendements du benchmark.

    Returns:
        Le bêta (covariance fonds/benchmark divisée par la variance du benchmark).
    """
    cov = np.cov(perf.dropna(), perf_bench.dropna())[0, 1]
    return cov / var_bench


def sharpe(mean_perf: float, std_perf: float, risk_free: float) -> float:
    """Ratio de Sharpe.

    Args:
        mean_perf: Rendement moyen du fonds.
        std_perf: Écart-type des rendements du fonds.
        risk_free: Taux sans risque (par ex. TMP moyen).

    Returns:
        L'excès de rendement par unité de risque total.
    """
    return (mean_perf - risk_free) / std_perf


def treynor(mean_perf: float, beta_value: float, risk_free: float) -> float:
    """Ratio de Treynor.

    Args:
        mean_perf: Rendement moyen du fonds.
        beta_value: Bêta du fonds.
        risk_free: Taux sans risque.

    Returns:
        L'excès de rendement par unité de risque systématique (bêta).
    """
    return (mean_perf - risk_free) / beta_value


def parametric_var(std_perf: float, alpha: float) -> float:
    """VaR paramétrique sous hypothèse de normalité.

    Args:
        std_perf: Écart-type des rendements.
        alpha: Seuil (ex. ``0.01`` pour 99 %).

    Returns:
        La VaR paramétrique (grandeur positive de perte potentielle).
    """
    return std_perf * stats.norm.ppf(1 - alpha)


def historic_var(perf: pd.Series, alpha: float) -> float:
    """VaR historique (quantile empirique des rendements).

    Args:
        perf: Série des rendements.
        alpha: Seuil (ex. ``0.01`` pour 99 %).

    Returns:
        Le quantile empirique au niveau ``alpha`` (rendement, généralement négatif).
    """
    return float(np.percentile(perf.dropna(), alpha * 100))


def montecarlo_var(
    mean_perf: float,
    std_perf: float,
    alpha: float,
    n_sims: int = DEFAULT_N_SIMS,
    rng: np.random.Generator | None = None,
) -> float:
    """VaR simulée par Monte-Carlo (rendements normaux).

    Args:
        mean_perf: Rendement moyen simulé.
        std_perf: Écart-type simulé.
        alpha: Seuil (ex. ``0.01`` pour 99 %).
        n_sims: Nombre de tirages.
        rng: Générateur aléatoire optionnel (pour la reproductibilité).

    Returns:
        Le quantile ``alpha`` de la distribution simulée.
    """
    rng = rng or np.random.default_rng()
    draws = rng.normal(mean_perf, std_perf, n_sims)
    return float(np.percentile(draws, alpha * 100))


def expected_shortfall(perf: pd.Series, var_threshold: float) -> float:
    """Expected Shortfall (CVaR) : perte moyenne au-delà de la VaR.

    Il s'agit de la moyenne des rendements inférieurs ou égaux au seuil de VaR
    (définition standard de la perte extrême conditionnelle).

    Args:
        perf: Série des rendements.
        var_threshold: Seuil de VaR (rendement).

    Returns:
        La moyenne des rendements de la queue au-delà de la VaR
        (``nan`` si aucune observation ne dépasse le seuil).
    """
    perf = perf.dropna()
    tail = perf[perf <= var_threshold]
    return float(tail.mean()) if not tail.empty else float("nan")


def compute_indicators(
    registre: Mapping[str, pd.DataFrame],
    variances_bench: Mapping[str, float],
    means: Mapping[str, float],
    stds: Mapping[str, float],
    risk_free: float,
    n_sims: int = DEFAULT_N_SIMS,
    seed: int | None = None,
) -> pd.DataFrame:
    """Calcule l'ensemble des indicateurs pour un registre de fonds.

    Args:
        registre: Dict ``code_fonds -> DataFrame`` comportant au moins les
            colonnes ``perf`` (rendements du fonds) et ``perf_bench``
            (rendements du benchmark).
        variances_bench: Variance du benchmark par fonds.
        means: Rendement moyen par fonds.
        stds: Écart-type des rendements par fonds.
        risk_free: Taux sans risque commun.
        n_sims: Nombre de tirages Monte-Carlo.
        seed: Graine aléatoire optionnelle (reproductibilité de la VaR MC).

    Returns:
        Un ``DataFrame`` indexé par code de fonds regroupant bêta, Sharpe,
        Treynor, VaR (paramétrique/historique/Monte-Carlo) et Expected
        Shortfall associés, pour chaque niveau de confiance.
    """
    rng = np.random.default_rng(seed)
    rows: Dict[str, dict] = {}

    for code, frame in registre.items():
        perf = frame["perf"]
        perf_bench = frame["perf_bench"]
        var_bench = variances_bench[code]
        beta_value = beta(perf, perf_bench, var_bench)

        indicators = {
            "beta": beta_value,
            "sharpe": sharpe(means[code], stds[code], risk_free),
            "treynor": treynor(means[code], beta_value, risk_free),
        }

        for name, alpha in CONFIDENCE_LEVELS.items():
            pvar = parametric_var(stds[code], alpha)
            hvar = historic_var(perf, alpha)
            svar = montecarlo_var(means[code], stds[code], alpha, n_sims, rng)
            indicators[f"pvar_{name}"] = pvar
            indicators[f"hvar_{name}"] = hvar
            indicators[f"simvar_{name}"] = svar
            indicators[f"pcvar_{name}"] = expected_shortfall(perf, pvar)
            indicators[f"hcvar_{name}"] = expected_shortfall(perf, hvar)
            indicators[f"simcvar_{name}"] = expected_shortfall(perf, svar)

        rows[code] = indicators

    return pd.DataFrame.from_dict(rows, orient="index")
