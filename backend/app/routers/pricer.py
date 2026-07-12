"""API du pricer obligataire, protégée par le mur d'abonnement.

* ``/api/pricer/price`` valorise directement un jeu de caractéristiques de titre
  (fonction pure, ne nécessite pas de données en base) — idéal pour la démo.
* ``/api/pricer/tenors`` et ``/api/pricer/price-isin`` s'appuient sur les données
  de courbe/titres présentes en base (``KANYON_DATABASE_URL``).
"""
from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.deps import active_subscription

router = APIRouter(prefix="/api/pricer", tags=["pricer"], dependencies=[Depends(active_subscription)])


class BondInput(BaseModel):
    """Caractéristiques d'un titre à taux fixe pour une valorisation directe."""

    date_valeur: dt.date
    date_emission: dt.date
    date_jouissance: dt.date
    date_echeance: dt.date
    taux_facial: float = Field(gt=0, description="Taux facial en décimal, ex. 0.035")
    taux_courbe: float = Field(gt=0, description="Taux de courbe en décimal, ex. 0.031")
    nominal: float = Field(gt=0, default=100)


@router.post("/price")
def price_bond(payload: BondInput) -> dict:
    """Valorise une obligation à taux fixe (calcul pur, sans base de données)."""
    from kanyon.pricer import price_fixed_bond

    return price_fixed_bond(
        date_valeur=payload.date_valeur,
        date_emission=payload.date_emission,
        date_jouissance=payload.date_jouissance,
        date_echeance=payload.date_echeance,
        taux_facial=payload.taux_facial,
        taux_courbe=payload.taux_courbe,
        nominal=payload.nominal,
    )


@router.get("/tenors")
def tenors(date_marche: str = Query(..., description="Date de la courbe (YYYY-MM-DD)")) -> dict:
    """Taux interpolés aux tenors standard, à partir des données de courbe en base."""
    from kanyon.pricer.service import interpolate_tenors

    try:
        return interpolate_tenors(date_marche)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/price-isin")
def price_isin_endpoint(
    isin: str = Query(...),
    date_courbe: str = Query(...),
    date_valeur: str = Query(...),
) -> dict:
    """Valorise un titre par ISIN sur une courbe donnée (données en base)."""
    from kanyon.pricer.service import price_isin

    try:
        return price_isin(isin, date_courbe=date_courbe, date_valeur=date_valeur)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
