"""API de reporting : génération de fiches de portefeuille (factsheet) — M9.

Assemble l'allocation, le stress testing et la conformité à partir des positions
fournies, puis renvoie une fiche HTML imprimable (impression PDF côté client).
Protégé par le mur d'abonnement.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.deps import active_subscription

router = APIRouter(prefix="/api/reporting", tags=["reporting"], dependencies=[Depends(active_subscription)])


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


class FactsheetInput(BaseModel):
    fund_name: str
    as_of: str
    category: str = ""
    currency: str = "MAD"
    holdings: List[HoldingInput]
    durations: Dict[str, float] = {}
    betas: Dict[str, float] = {}
    limits: LimitsInput = LimitsInput()
    metrics: Dict[str, float] = {}
    notes: str = ""


@router.post("/factsheet", response_class=HTMLResponse)
def factsheet(payload: FactsheetInput) -> HTMLResponse:
    """Génère la fiche HTML imprimable d'un portefeuille."""
    from kanyon.portfolio import (
        ComplianceLimits,
        DEFAULT_SCENARIOS,
        Holding,
        check_compliance,
        run_scenarios,
    )
    from kanyon.reporting import FactsheetContext, render_factsheet_html

    weights = {h.ticker: h.weight for h in payload.holdings}

    # Allocation agrégée par classe d'actifs.
    by_class: Dict[str, float] = defaultdict(float)
    for h in payload.holdings:
        by_class[h.asset_class] += h.weight
    allocation = [{"label": cls.capitalize(), "weight": w} for cls, w in sorted(by_class.items())]

    # Durations : fournies, sinon déduites de la sensibilité des lignes.
    durations = payload.durations or {h.ticker: h.sensitivity for h in payload.holdings if h.sensitivity}
    stress = run_scenarios(weights, durations, payload.betas, DEFAULT_SCENARIOS)

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
    compliance = check_compliance(holdings, limits).as_dict()

    ctx = FactsheetContext(
        fund_name=payload.fund_name,
        as_of=payload.as_of,
        category=payload.category,
        currency=payload.currency,
        allocation=allocation,
        metrics=payload.metrics,
        stress=stress,
        compliance=compliance,
        notes=payload.notes,
    )
    return HTMLResponse(content=render_factsheet_html(ctx))
