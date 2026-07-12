"""Orchestration du pricer : lecture des données puis calculs.

Seule couche du pricer à toucher la base de données. Elle assemble la courbe à
partir de :func:`kanyon.db.queries.query_courbe`, interpole les tenors standard
et valorise un titre identifié par son ISIN.
"""
from __future__ import annotations

import datetime as dt
from typing import Dict, List, Tuple

import pandas as pd

from kanyon.db.queries import query_courbe, query_titre_all
from kanyon.helpers import add_years, to_date
from kanyon.pricer.bonds import price_fixed_bond
from kanyon.pricer.curve import interpolate_curve
from kanyon.pricer.dates import tenor_map

Point = Tuple[float, float]


def _as_date(value) -> dt.date:
    return to_date(str(value)) if not isinstance(value, dt.date) else value


def build_curve(date_marche, session=None) -> Tuple[List[Point], float]:
    """Construit les points de courbe ``(maturité_jours, taux)`` pour une date.

    Returns:
        Un tuple ``(points, base_annuelle)``.

    Raises:
        ValueError: si aucune donnée de courbe n'existe pour cette date.
    """
    df = query_courbe(date_marche, session)
    if df is None or df.empty:
        raise ValueError(f"Aucune courbe en base pour le {date_marche}.")

    echeance = pd.to_datetime(df["date_echeance"])
    valeur = pd.to_datetime(df["date_valeur"])
    maturities = (echeance - valeur).dt.days.astype(float)
    rates = df["taux"].astype(float)
    points = [(m, r) for m, r in zip(maturities, rates) if m > 0]

    ref = _as_date(date_marche)
    year_basis = (add_years(ref, 1) - ref).days
    return points, year_basis


def interpolate_tenors(date_marche, session=None) -> Dict[str, float]:
    """Taux interpolés aux tenors standard pour une date de marché."""
    points, year_basis = build_curve(date_marche, session)
    ref = _as_date(date_marche)
    return {
        key: round(interpolate_curve(points, days, year_basis), 6)
        for key, days in tenor_map(ref).items()
    }


def rate_for_maturity(date_marche, maturity_days: int, session=None) -> float:
    """Taux interpolé pour une maturité résiduelle (en jours) à une date donnée."""
    points, year_basis = build_curve(date_marche, session)
    return round(interpolate_curve(points, maturity_days, year_basis), 6)


def _find_title(isin: str, session=None) -> dict:
    """Récupère les caractéristiques d'un titre par ISIN (référentiel Maroclear)."""
    df = query_titre_all(session)
    if df is None or df.empty:
        raise ValueError("Référentiel des titres vide.")
    key = isin.strip().upper()
    match = df[df["code_isin"].astype(str).str.upper().str.contains(key, na=False)]
    if match.empty:
        raise ValueError(f"Titre {isin} introuvable dans le référentiel.")
    return match.iloc[0].to_dict()


def price_isin(isin: str, date_courbe, date_valeur, session=None) -> dict:
    """Valorise un titre par son ISIN à une date de valeur, sur une courbe donnée.

    Args:
        isin: Identifiant (tout ou partie du ``code_isin``).
        date_courbe: Date de la courbe des taux utilisée.
        date_valeur: Date de valorisation du titre.
        session: Session SQLAlchemy optionnelle.

    Returns:
        Le dictionnaire de valorisation enrichi de l'ISIN et des dates.
    """
    title = _find_title(isin, session)
    date_valeur = _as_date(date_valeur)

    date_emission = _as_date(title["date_emission"])
    date_jouissance = _as_date(title["date_jouissance"])
    date_echeance = _as_date(title["date_echeance"])
    taux_facial = float(title["taux_facial"]) / 100.0
    nominal = float(title["nominal"])

    mat_res = (date_echeance - date_valeur).days
    taux_courbe = rate_for_maturity(date_courbe, mat_res, session)

    result = price_fixed_bond(
        date_valeur=date_valeur,
        date_emission=date_emission,
        date_jouissance=date_jouissance,
        date_echeance=date_echeance,
        taux_facial=taux_facial,
        taux_courbe=taux_courbe,
        nominal=nominal,
    )
    result.update(
        {
            "isin": str(title["code_isin"]),
            "date_valeur": date_valeur.isoformat(),
            "date_courbe": _as_date(date_courbe).isoformat(),
            "taux_facial": taux_facial,
            "nominal": nominal,
        }
    )
    return result
