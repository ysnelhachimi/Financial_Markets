"""API du moteur de portefeuille (M3–M8), protégée par le mur d'abonnement.

Endpoints :
    GET  /api/portfolio/strategies   catégories OPCVM et leurs paramètres
    POST /api/portfolio/optimize     optimisation d'allocation
    POST /api/portfolio/backtest     backtest d'une allocation
    POST /api/portfolio/stress       stress testing (chocs taux/actions)
    POST /api/portfolio/compliance   contrôle des ratios prudentiels AMMC
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.deps import active_subscription

router = APIRouter(
    prefix="/api/portfolio", tags=["portfolio"], dependencies=[Depends(active_subscription)]
)


# --------------------------- Schémas ---------------------------
class OptimizeInput(BaseModel):
    tickers: List[str]
    mean: List[float]
    cov: List[List[float]]
    objective: str = "min_variance"
    target_return: Optional[float] = None
    risk_free: float = 0.0
    bounds: Tuple[float, float] = (0.0, 1.0)


class BacktestInput(BaseModel):
    prices: Dict[str, List[float]] = Field(..., description="Série de prix par actif")
    weights: Dict[str, float]
    periods_per_year: int = 252
    risk_free: float = 0.0


class StressScenarioInput(BaseModel):
    name: str
    rate_shock_bps: float = 0.0
    equity_shock: float = 0.0


class StressInput(BaseModel):
    weights: Dict[str, float]
    durations: Dict[str, float] = {}
    betas: Dict[str, float] = {}
    scenarios: Optional[List[StressScenarioInput]] = None


class HoldingInput(BaseModel):
    ticker: str
    issuer: str
    weight: float
    asset_class: str = "obligataire"
    sensitivity: float = 0.0
    emprise: float = 0.0
    liquid: bool = True
    issuer_group: Optional[str] = None


class LimitsInput(BaseModel):
    max_per_issuer: float = 0.10
    concentration_threshold: float = 0.05
    concentration_cap: float = 0.40
    max_emprise: float = 0.10
    max_illiquid: float = 0.10
    sensitivity_bounds: Optional[Tuple[float, float]] = None


class ComplianceInput(BaseModel):
    holdings: List[HoldingInput]
    limits: LimitsInput = LimitsInput()


# --------------------------- Endpoints ---------------------------
@router.get("/strategies")
def strategies() -> list[dict]:
    """Catégories OPCVM et leurs paramètres de stratégie."""
    from kanyon.portfolio import STRATEGIES

    return [
        {
            "code": s.code,
            "label": s.label,
            "objective": s.objective,
            "asset_bounds": s.asset_bounds,
            "sensitivity_bounds": s.sensitivity_bounds,
            "description": s.description,
        }
        for s in STRATEGIES.values()
    ]


@router.post("/optimize")
def optimize_endpoint(payload: OptimizeInput) -> dict:
    """Optimise une allocation sous contraintes."""
    from kanyon.portfolio import optimize

    try:
        result = optimize(
            payload.mean, payload.cov, payload.tickers,
            objective=payload.objective, target_return=payload.target_return,
            risk_free=payload.risk_free, bounds=payload.bounds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return result.as_dict()


@router.post("/backtest")
def backtest_endpoint(payload: BacktestInput) -> dict:
    """Backteste une allocation constant-mix sur une série de prix."""
    import pandas as pd

    from kanyon.portfolio import backtest

    frame = pd.DataFrame(payload.prices)
    try:
        result = backtest(
            frame, payload.weights,
            periods_per_year=payload.periods_per_year, risk_free=payload.risk_free,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return result.summary()


@router.post("/stress")
def stress_endpoint(payload: StressInput) -> list[dict]:
    """Évalue des scénarios de stress (chocs de taux et d'actions)."""
    from kanyon.portfolio import DEFAULT_SCENARIOS, StressScenario, run_scenarios

    if payload.scenarios:
        scenarios = [
            StressScenario(s.name, rate_shock_bps=s.rate_shock_bps, equity_shock=s.equity_shock)
            for s in payload.scenarios
        ]
    else:
        scenarios = DEFAULT_SCENARIOS
    return run_scenarios(payload.weights, payload.durations, payload.betas, scenarios)


@router.get("/equities/optimize")
def optimize_equities_endpoint(
    debut: str = Query(..., description="Début de période (YYYY-MM-DD)"),
    fin: str = Query(..., description="Fin de période (YYYY-MM-DD)"),
    objective: str = Query("max_sharpe"),
) -> dict:
    """Optimise un portefeuille sur l'univers actions MASI **réel** de la période."""
    from kanyon.portfolio.service import optimize_equities

    try:
        return optimize_equities(debut, fin, objective=objective)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/equities/backtest")
def backtest_equities_endpoint(
    debut: str = Query(...),
    fin: str = Query(...),
) -> dict:
    """Backteste une allocation équipondérée sur l'univers actions MASI **réel**."""
    from kanyon.portfolio.service import backtest_equities

    try:
        return backtest_equities(debut, fin)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/compliance")
def compliance_endpoint(payload: ComplianceInput) -> dict:
    """Contrôle les ratios prudentiels AMMC d'un portefeuille."""
    from kanyon.portfolio import ComplianceLimits, Holding, check_compliance

    holdings = [
        Holding(
            ticker=h.ticker, issuer=h.issuer, weight=h.weight, asset_class=h.asset_class,
            sensitivity=h.sensitivity, emprise=h.emprise, liquid=h.liquid, issuer_group=h.issuer_group,
        )
        for h in payload.holdings
    ]
    limits = ComplianceLimits(
        max_per_issuer=payload.limits.max_per_issuer,
        concentration_threshold=payload.limits.concentration_threshold,
        concentration_cap=payload.limits.concentration_cap,
        max_emprise=payload.limits.max_emprise,
        max_illiquid=payload.limits.max_illiquid,
        sensitivity_bounds=payload.limits.sensitivity_bounds,
    )
    return check_compliance(holdings, limits).as_dict()
