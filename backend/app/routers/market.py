"""API de données de marché, protégée par le mur d'abonnement.

Les endpoints enveloppent les requêtes du package :mod:`kanyon` et renvoient du
JSON. L'accès exige un abonnement en cours (dépendance :func:`active_subscription`).
Les données proviennent de la base pointée par ``KANYON_DATABASE_URL``.
"""
from __future__ import annotations

from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, Query

from app.deps import active_subscription

router = APIRouter(prefix="/api/market", tags=["market"], dependencies=[Depends(active_subscription)])


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convertit un DataFrame en liste d'enregistrements JSON-sérialisables."""
    if df is None or df.empty:
        return []
    # ``to_json``/``read_json`` normalise dates et types pour la sérialisation.
    return df.to_dict(orient="records")


@router.get("/indices")
def indices(debut: str = Query(...), fin: str = Query(...)) -> list[dict]:
    """Séries d'indices MASI (et sectoriels) entre deux dates."""
    from kanyon.db.queries import query_masi_indice

    return _records(query_masi_indice(debut, fin))


@router.get("/composition")
def composition(debut: str = Query(...), fin: str = Query(...)) -> list[dict]:
    """Composition de l'indice MASI entre deux dates."""
    from kanyon.db.queries import query_masi_composition

    return _records(query_masi_composition(debut, fin))


@router.get("/volumes")
def volumes(debut: str = Query(...), fin: str = Query(...)) -> list[dict]:
    """Volumes échangés par valeur entre deux dates."""
    from kanyon.db.queries import query_masi_volume

    return _records(query_masi_volume(debut, fin))


@router.get("/courbe")
def courbe(date_marche: str = Query(...)) -> list[dict]:
    """Courbe des taux secondaires pour une date de marché."""
    from kanyon.db.queries import query_courbe

    return _records(query_courbe(date_marche))


@router.get("/tenors")
def tenors(debut: str = Query(...), fin: str = Query(...)) -> list[dict]:
    """Courbe des tenors entre deux dates."""
    from kanyon.db.queries import query_tenors_range

    return _records(query_tenors_range(debut, fin))


@router.get("/monetaire")
def marche_monetaire(debut: str = Query(...), fin: str = Query(...)) -> list[dict]:
    """Vue consolidée du marché monétaire (MONIA + TMP + taux directeur)."""
    from kanyon.db.queries import query_marche_monetaire

    return _records(query_marche_monetaire(debut, fin))
