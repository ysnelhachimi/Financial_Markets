"""Backtesting d'une allocation de portefeuille (M5).

Rejeu historique d'une allocation cible (stratégie *constant-mix* : rééquilibrage
vers les poids cibles à chaque période) sur une série de prix, avec calcul des
indicateurs de performance et de risque.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Mapping

import numpy as np
import pandas as pd


@dataclass
class BacktestResult:
    """Résultat d'un backtest."""

    nav: pd.Series           # valeur liquidative simulée
    returns: pd.Series       # rendements périodiques du portefeuille
    total_return: float
    cagr: float
    volatility: float        # annualisée
    sharpe: float
    max_drawdown: float

    def summary(self) -> dict:
        """Résumé sérialisable (sans les séries)."""
        return {
            "total_return": round(self.total_return, 6),
            "cagr": round(self.cagr, 6),
            "volatility": round(self.volatility, 6),
            "sharpe": round(self.sharpe, 6),
            "max_drawdown": round(self.max_drawdown, 6),
            "start": str(self.nav.index[0]),
            "end": str(self.nav.index[-1]),
            "observations": int(len(self.nav)),
        }


def max_drawdown(nav: pd.Series) -> float:
    """Drawdown maximal (perte maximale depuis un plus-haut), en fraction négative."""
    running_max = nav.cummax()
    drawdowns = nav / running_max - 1.0
    return float(drawdowns.min())


def backtest(
    prices: pd.DataFrame,
    weights: Mapping[str, float],
    *,
    initial: float = 100.0,
    periods_per_year: int = 252,
    risk_free: float = 0.0,
) -> BacktestResult:
    """Backteste une allocation *constant-mix* sur une série de prix.

    Args:
        prices: Prix (index = dates triées, colonnes = actifs).
        weights: Poids cibles par actif (repondérés pour sommer à 1).
        initial: Valeur liquidative initiale.
        periods_per_year: Périodes par an (252 quotidien, 52 hebdo, 12 mensuel).
        risk_free: Taux sans risque annualisé (pour le Sharpe).

    Returns:
        Un :class:`BacktestResult`.

    Raises:
        ValueError: si aucun actif commun entre ``prices`` et ``weights``.
    """
    cols = [c for c in prices.columns if c in weights]
    if not cols:
        raise ValueError("Aucun actif commun entre les prix et les poids.")

    w = np.array([weights[c] for c in cols], dtype=float)
    w = w / w.sum()

    asset_returns = prices[cols].sort_index().pct_change().dropna(how="all").fillna(0.0)
    port_returns = asset_returns.to_numpy() @ w
    port_returns = pd.Series(port_returns, index=asset_returns.index, name="return")

    nav = initial * (1.0 + port_returns).cumprod()
    nav.name = "nav"

    total_return = float(nav.iloc[-1] / initial - 1.0)
    years = max(len(port_returns) / periods_per_year, 1e-9)
    cagr = float((nav.iloc[-1] / initial) ** (1.0 / years) - 1.0)
    vol = float(port_returns.std(ddof=1) * np.sqrt(periods_per_year))
    excess = port_returns.mean() * periods_per_year - risk_free
    sharpe = float(excess / vol) if vol > 0 else 0.0
    mdd = max_drawdown(nav)

    return BacktestResult(
        nav=nav, returns=port_returns, total_return=total_return, cagr=cagr,
        volatility=vol, sharpe=sharpe, max_drawdown=mdd,
    )
