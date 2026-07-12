"""Moteur de gestion de portefeuille (construction, stratégies, backtest,
stress, conformité) — modules M3–M8.

Cœur pur et testable (données injectées) ; la couche API du produit l'appelle
en fournissant les données issues de :mod:`kanyon.db` et :mod:`kanyon.pricer`.
"""
from kanyon.portfolio.construction import (
    OptimizationResult,
    optimize,
    portfolio_stats,
    target_between_curves,
)
from kanyon.portfolio.strategies import (
    STRATEGIES,
    CategoryStrategy,
    get_strategy,
    group_bounds_from_strategy,
)
from kanyon.portfolio.backtest import BacktestResult, backtest, max_drawdown
from kanyon.portfolio.stress import (
    StressScenario,
    DEFAULT_SCENARIOS,
    run_scenarios,
    rate_shock_return,
    equity_shock_return,
    stressed_var,
)
from kanyon.portfolio.compliance import (
    Holding,
    ComplianceLimits,
    ComplianceReport,
    check_compliance,
)

__all__ = [
    "OptimizationResult", "optimize", "portfolio_stats", "target_between_curves",
    "STRATEGIES", "CategoryStrategy", "get_strategy", "group_bounds_from_strategy",
    "BacktestResult", "backtest", "max_drawdown",
    "StressScenario", "DEFAULT_SCENARIOS", "run_scenarios", "rate_shock_return",
    "equity_shock_return", "stressed_var",
    "Holding", "ComplianceLimits", "ComplianceReport", "check_compliance",
]
