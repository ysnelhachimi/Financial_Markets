"""Construction et optimisation de portefeuille (M3).

Fonctions **pures** (données injectées) : elles reçoivent des rendements
attendus et une matrice de covariance, et renvoient des poids optimaux sous
contraintes. Aucune dépendance à la base de données.

Objectifs supportés :

* ``min_variance``   — portefeuille de variance minimale ;
* ``max_sharpe``     — portefeuille tangent (Sharpe maximal) ;
* ``target_return``  — variance minimale pour un rendement cible donné ;
* ``risk_parity``    — contribution au risque égalisée entre actifs.

Contraintes : budget (somme des poids = 1), bornes par actif (long-only par
défaut), et bornes de groupe (secteurs, classes d'actifs).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
from scipy.optimize import minimize


@dataclass
class OptimizationResult:
    """Résultat d'une optimisation de portefeuille."""

    weights: Dict[str, float]
    expected_return: float
    volatility: float
    sharpe: float

    def as_dict(self) -> dict:
        return {
            "weights": {k: round(v, 6) for k, v in self.weights.items()},
            "expected_return": round(self.expected_return, 6),
            "volatility": round(self.volatility, 6),
            "sharpe": round(self.sharpe, 6),
        }


def portfolio_stats(
    weights: np.ndarray, mean: np.ndarray, cov: np.ndarray, risk_free: float = 0.0
) -> Tuple[float, float, float]:
    """Rendement, volatilité et Sharpe d'un portefeuille pondéré."""
    exp_return = float(weights @ mean)
    variance = float(weights @ cov @ weights)
    vol = float(np.sqrt(max(variance, 0.0)))
    sharpe = (exp_return - risk_free) / vol if vol > 0 else 0.0
    return exp_return, vol, sharpe


def optimize(
    mean: Sequence[float],
    cov: Sequence[Sequence[float]],
    tickers: Sequence[str],
    objective: str = "min_variance",
    *,
    target_return: Optional[float] = None,
    risk_free: float = 0.0,
    bounds: Tuple[float, float] = (0.0, 1.0),
    group_bounds: Optional[Mapping[str, Tuple[Sequence[str], float, float]]] = None,
) -> OptimizationResult:
    """Optimise les poids d'un portefeuille sous contraintes.

    Args:
        mean: Rendements attendus par actif.
        cov: Matrice de covariance (n x n).
        tickers: Identifiants des actifs (longueur n).
        objective: ``min_variance`` | ``max_sharpe`` | ``target_return`` | ``risk_parity``.
        target_return: Rendement visé (requis pour ``target_return``).
        risk_free: Taux sans risque (pour le Sharpe).
        bounds: Bornes (min, max) appliquées à chaque poids.
        group_bounds: Contraintes de groupe ``nom -> (tickers, min, max)`` sur la
            somme des poids du groupe (secteurs, classes d'actifs).

    Returns:
        Un :class:`OptimizationResult`.

    Raises:
        ValueError: dimensions incohérentes ou objectif inconnu.
    """
    mean_v = np.asarray(mean, dtype=float)
    cov_m = np.asarray(cov, dtype=float)
    n = len(tickers)
    if mean_v.shape[0] != n or cov_m.shape != (n, n):
        raise ValueError("Dimensions incohérentes entre mean, cov et tickers.")

    if objective == "risk_parity":
        weights = _risk_parity(cov_m, bounds)
    else:
        weights = _constrained_optimize(
            mean_v, cov_m, tickers, objective, target_return, risk_free, bounds, group_bounds
        )

    exp_return, vol, sharpe = portfolio_stats(weights, mean_v, cov_m, risk_free)
    return OptimizationResult(
        weights={t: float(w) for t, w in zip(tickers, weights)},
        expected_return=exp_return,
        volatility=vol,
        sharpe=sharpe,
    )


def _constrained_optimize(
    mean_v, cov_m, tickers, objective, target_return, risk_free, bounds, group_bounds
) -> np.ndarray:
    n = len(tickers)
    x0 = np.full(n, 1.0 / n)
    bnds = [bounds] * n

    constraints: List[dict] = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]

    if group_bounds:
        index = {t: i for i, t in enumerate(tickers)}
        for _, (members, gmin, gmax) in group_bounds.items():
            idx = [index[m] for m in members if m in index]
            if not idx:
                continue
            constraints.append({"type": "ineq", "fun": (lambda w, idx=idx, gmin=gmin: np.sum(w[idx]) - gmin)})
            constraints.append({"type": "ineq", "fun": (lambda w, idx=idx, gmax=gmax: gmax - np.sum(w[idx]))})

    if objective == "min_variance":
        fun = lambda w: w @ cov_m @ w
    elif objective == "max_sharpe":
        def fun(w):
            ret = w @ mean_v
            vol = np.sqrt(max(w @ cov_m @ w, 1e-12))
            return -(ret - risk_free) / vol
    elif objective == "target_return":
        if target_return is None:
            raise ValueError("target_return est requis pour l'objectif 'target_return'.")
        fun = lambda w: w @ cov_m @ w
        constraints.append({"type": "eq", "fun": lambda w: w @ mean_v - target_return})
    else:
        raise ValueError(f"Objectif inconnu : {objective!r}")

    res = minimize(fun, x0, method="SLSQP", bounds=bnds, constraints=constraints,
                   options={"maxiter": 500, "ftol": 1e-10})
    weights = np.clip(res.x, bounds[0], bounds[1])
    total = weights.sum()
    return weights / total if total > 0 else x0


def _risk_parity(cov_m: np.ndarray, bounds: Tuple[float, float]) -> np.ndarray:
    """Poids à contribution au risque égale (long-only)."""
    n = cov_m.shape[0]
    x0 = np.full(n, 1.0 / n)

    def objective(w):
        port_vol = np.sqrt(max(w @ cov_m @ w, 1e-12))
        marginal = cov_m @ w
        contrib = w * marginal / port_vol
        target = port_vol / n
        return np.sum((contrib - target) ** 2)

    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
    res = minimize(objective, x0, method="SLSQP", bounds=[(max(bounds[0], 1e-6), bounds[1])] * n,
                   constraints=constraints, options={"maxiter": 1000, "ftol": 1e-12})
    weights = np.clip(res.x, 1e-6, bounds[1])
    return weights / weights.sum()


def target_between_curves(
    tickers: Sequence[str],
    sensitivities: Sequence[float],
    rate_start: Mapping[str, float],
    rate_end: Mapping[str, float],
    target_sensitivity: float,
    bounds: Tuple[float, float] = (0.0, 1.0),
) -> OptimizationResult:
    """Portefeuille obligataire cible entre deux courbes de taux (scénario).

    Construit une allocation dont la sensibilité globale atteint la cible tout en
    maximisant le gain de portage/valorisation attendu au passage de la courbe de
    départ à la courbe d'arrivée.

    Args:
        tickers: Identifiants des lignes obligataires.
        sensitivities: Sensibilité (duration modifiée) de chaque ligne.
        rate_start: Taux de départ par ligne.
        rate_end: Taux d'arrivée (scénario) par ligne.
        target_sensitivity: Sensibilité globale visée du portefeuille.
        bounds: Bornes par ligne.

    Returns:
        Un :class:`OptimizationResult` (``expected_return`` = gain de scénario estimé).
    """
    n = len(tickers)
    sens = np.asarray(sensitivities, dtype=float)
    # Gain de prix approché par ligne : -sensibilité * variation de taux.
    delta_rate = np.array([rate_end[t] - rate_start[t] for t in tickers])
    expected_gain = -sens * delta_rate  # rendement de scénario par ligne

    x0 = np.full(n, 1.0 / n)
    constraints = [
        {"type": "eq", "fun": lambda w: np.sum(w) - 1.0},
        {"type": "eq", "fun": lambda w: w @ sens - target_sensitivity},
    ]
    # Maximise le gain de scénario (minimise son opposé).
    res = minimize(lambda w: -(w @ expected_gain), x0, method="SLSQP",
                   bounds=[bounds] * n, constraints=constraints, options={"maxiter": 500})
    weights = np.clip(res.x, bounds[0], bounds[1])
    weights = weights / weights.sum() if weights.sum() > 0 else x0

    portfolio_sens = float(weights @ sens)
    scenario_gain = float(weights @ expected_gain)
    return OptimizationResult(
        weights={t: float(w) for t, w in zip(tickers, weights)},
        expected_return=scenario_gain,
        volatility=portfolio_sens,  # ici : sensibilité globale du portefeuille
        sharpe=0.0,
    )
