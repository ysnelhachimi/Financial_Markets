"""Stress testing de portefeuille (M6).

Chocs appliqués aux positions :

* **taux** : variation de rendement (translation parallèle) répercutée sur les
  lignes obligataires via leur sensibilité (duration modifiée) ;
* **actions** : choc de marché répercuté via le bêta de chaque ligne ;
* **VaR stressée** : VaR paramétrique recalculée après décalage défavorable.

Fonctions pures : positions et sensibilités sont fournies par l'appelant.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Mapping, Sequence

from kanyon.analytics.ratios import parametric_var


@dataclass
class StressScenario:
    """Définition d'un scénario de stress."""

    name: str
    rate_shock_bps: float = 0.0     # choc de taux en points de base (ex. +100)
    equity_shock: float = 0.0       # choc de marché actions en fraction (ex. -0.20)


def rate_shock_return(
    weights: Mapping[str, float],
    durations: Mapping[str, float],
    shock_bps: float,
) -> float:
    """Rendement de portefeuille sous un choc de taux parallèle.

    P&L d'une ligne ≈ -poids × sensibilité × Δtaux. ``shock_bps`` en points de base.
    """
    delta = shock_bps / 10_000.0
    return float(sum(weights.get(t, 0.0) * -durations.get(t, 0.0) * delta for t in durations))


def equity_shock_return(
    weights: Mapping[str, float],
    betas: Mapping[str, float],
    market_shock: float,
) -> float:
    """Rendement de portefeuille sous un choc de marché actions (via bêta)."""
    return float(sum(weights.get(t, 0.0) * betas.get(t, 0.0) * market_shock for t in betas))


def run_scenarios(
    weights: Mapping[str, float],
    durations: Mapping[str, float],
    betas: Mapping[str, float],
    scenarios: Sequence[StressScenario],
) -> List[dict]:
    """Évalue une liste de scénarios et retourne le P&L de chacun.

    Returns:
        Liste de ``{name, rate_pnl, equity_pnl, total_pnl}`` (en fraction).
    """
    results = []
    for sc in scenarios:
        rate_pnl = rate_shock_return(weights, durations, sc.rate_shock_bps) if sc.rate_shock_bps else 0.0
        equity_pnl = equity_shock_return(weights, betas, sc.equity_shock) if sc.equity_shock else 0.0
        results.append(
            {
                "name": sc.name,
                "rate_pnl": round(rate_pnl, 6),
                "equity_pnl": round(equity_pnl, 6),
                "total_pnl": round(rate_pnl + equity_pnl, 6),
            }
        )
    return results


def stressed_var(volatility: float, alpha: float, adverse_shift: float = 0.0) -> float:
    """VaR paramétrique stressée (volatilité + décalage défavorable de la moyenne).

    Args:
        volatility: Écart-type des rendements du portefeuille.
        alpha: Seuil (ex. 0.01 pour 99 %).
        adverse_shift: Perte moyenne additionnelle imposée par le stress (>= 0).

    Returns:
        La VaR stressée (grandeur positive de perte potentielle).
    """
    return parametric_var(volatility, alpha) + max(adverse_shift, 0.0)


# Scénarios standard proposés par défaut (personnalisables côté produit).
DEFAULT_SCENARIOS = [
    StressScenario("Hausse des taux +100pb", rate_shock_bps=100),
    StressScenario("Hausse des taux +200pb", rate_shock_bps=200),
    StressScenario("Baisse des taux -100pb", rate_shock_bps=-100),
    StressScenario("Krach actions -20%", equity_shock=-0.20),
    StressScenario("Stagflation (taux +150pb, actions -15%)", rate_shock_bps=150, equity_shock=-0.15),
]
