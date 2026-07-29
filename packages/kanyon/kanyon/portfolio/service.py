"""Service portefeuille sur données réelles (DB) — orchestration.

Relie l'univers investissable (:mod:`kanyon.portfolio.universe`) aux moteurs de
construction et de backtest, à partir des données de marché en base.
"""
from __future__ import annotations

from typing import Optional

from kanyon.portfolio.backtest import backtest
from kanyon.portfolio.construction import optimize
from kanyon.portfolio.universe import equity_prices, estimate_inputs


def optimize_equities(
    date_debut,
    date_fin,
    objective: str = "max_sharpe",
    risk_free: float = 0.02,
    session=None,
) -> dict:
    """Optimise un portefeuille actions sur l'univers MASI réel de la période.

    Returns:
        ``{tickers, result, observations}`` où ``result`` est le dict d'optimisation.

    Raises:
        ValueError: si l'univers est insuffisant sur la période.
    """
    prices = equity_prices(date_debut, date_fin, session)
    if prices.empty:
        raise ValueError("Aucune donnée d'actions en base sur cette période.")
    tickers, mean, cov = estimate_inputs(prices)
    result = optimize(mean, cov, tickers, objective=objective, risk_free=risk_free)
    return {
        "tickers": tickers,
        "observations": int(len(prices)),
        "result": result.as_dict(),
    }


def backtest_equities(date_debut, date_fin, weights: Optional[dict] = None, session=None) -> dict:
    """Backteste une allocation actions sur l'univers MASI réel.

    Si ``weights`` est absent, une allocation équipondérée est utilisée.
    """
    prices = equity_prices(date_debut, date_fin, session)
    if prices.empty:
        raise ValueError("Aucune donnée d'actions en base sur cette période.")
    if not weights:
        cols = list(prices.columns)
        weights = {c: 1.0 / len(cols) for c in cols}
    return backtest(prices, weights).summary()
