"""Tenors normalisés et calculs de périodes.

Les tenors standard de la courbe marocaine vont de 13 semaines à 30 ans. Chaque
tenor est exprimé en nombre de jours *à partir d'une date de valeur donnée*
(les années civiles n'ont pas toutes la même longueur).
"""
from __future__ import annotations

import datetime as dt
from typing import List

from dateutil.relativedelta import relativedelta

# Tenors standard sous forme "<n>:<unité>" (A = années, S = semaines).
TENOR_PERIODS = [
    "13:S", "26:S", "52:S",
    "1:A", "2:A", "5:A", "10:A", "15:A", "20:A", "25:A", "30:A",
]

# Libellés courts correspondants (clés de sortie).
TENOR_KEYS = [
    "13s", "26s", "52s",
    "1an", "2ans", "5ans", "10ans", "15ans", "20ans", "25ans", "30ans",
]

_UNIT_TO_KWARG = {"A": "years", "S": "weeks", "M": "months", "D": "days"}


def period_days(reference: dt.date, period: str) -> int:
    """Nombre de jours d'une période ``"<n>:<unité>"`` à partir d'une date.

    Args:
        reference: Date de valeur (point de départ).
        period: Période au format ``"13:S"`` (13 semaines), ``"5:A"`` (5 ans)...

    Returns:
        Le nombre de jours calendaires de la période.
    """
    n, unit = period.upper().split(":")
    kwarg = _UNIT_TO_KWARG[unit]
    end = reference + relativedelta(**{kwarg: int(n)})
    return (end - reference).days


def tenor_days(reference: dt.date) -> List[int]:
    """Liste des tenors standard exprimés en jours, pour une date de valeur."""
    return [period_days(reference, p) for p in TENOR_PERIODS]


def tenor_map(reference: dt.date) -> dict[str, int]:
    """Association libellé de tenor -> nombre de jours, pour une date de valeur."""
    return dict(zip(TENOR_KEYS, tenor_days(reference)))
