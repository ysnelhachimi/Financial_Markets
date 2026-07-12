"""Interpolation de la courbe des taux BKAM (fonctions pures).

La courbe est fournie sous forme de points ``(maturité_en_jours, taux)`` triés
par maturité croissante ; aucune dépendance à la base de données ici.

Conventions marocaines :

* en deçà d'un an, les taux sont *monétaires* (base 360, linéaire) ;
* au-delà d'un an, ils sont *actuariels* (base année civile, capitalisation).

Le passage d'une convention à l'autre est géré au franchissement de la maturité
d'un an lors de l'interpolation.
"""
from __future__ import annotations

from typing import List, Sequence, Tuple

Point = Tuple[float, float]  # (maturité en jours, taux)


def money_market_rate(rate: float, maturity: float, year_basis: float) -> float:
    """Convertit un taux actuariel en taux monétaire (base 360).

    Args:
        rate: Taux actuariel.
        maturity: Maturité résiduelle en jours.
        year_basis: Nombre de jours de l'année civile de référence (365/366).
    """
    return ((1 + rate) ** (maturity / year_basis) - 1) * (360 / maturity)


def actuarial_rate(rate: float, maturity: float, year_basis: float) -> float:
    """Convertit un taux monétaire en taux actuariel.

    Args:
        rate: Taux monétaire.
        maturity: Maturité résiduelle en jours.
        year_basis: Nombre de jours de l'année civile de référence (365/366).
    """
    return (1 + rate * maturity / 360) ** (year_basis / maturity) - 1


def _linear(m0: float, r0: float, m1: float, r1: float, mat: float) -> float:
    """Interpolation/extrapolation linéaire simple."""
    if m1 == m0:
        return r0
    return r0 + (r1 - r0) * (mat - m0) / (m1 - m0)


def interpolate_curve(points: Sequence[Point], mat_res: float, year_basis: float) -> float:
    """Retourne le taux interpolé pour une maturité résiduelle donnée.

    Args:
        points: Points ``(maturité_jours, taux)`` triés par maturité croissante.
        mat_res: Maturité résiduelle recherchée, en jours.
        year_basis: Jours de l'année civile de référence (ex. 365 ou 366).

    Returns:
        Le taux interpolé.

    Raises:
        ValueError: si la courbe est vide.
    """
    pts: List[Point] = sorted(points, key=lambda p: p[0])
    if not pts:
        raise ValueError("Courbe vide : interpolation impossible.")
    if len(pts) == 1:
        return pts[0][1]

    # Extrapolation sous le premier / au-delà du dernier point.
    if mat_res <= pts[0][0]:
        return _linear(pts[0][0], pts[0][1], pts[1][0], pts[1][1], mat_res)
    if mat_res >= pts[-1][0]:
        return _linear(pts[-2][0], pts[-2][1], pts[-1][0], pts[-1][1], mat_res)

    # Segment encadrant mat_res.
    for i in range(len(pts) - 1):
        m0, r0 = pts[i]
        m1, r1 = pts[i + 1]
        if m0 <= mat_res <= m1:
            crosses_one_year = m0 < 365 <= m1
            if not crosses_one_year:
                return _linear(m0, r0, m1, r1, mat_res)
            # Segment franchissant la maturité d'un an : conversion de convention.
            if mat_res >= 365:
                r0_act = actuarial_rate(r0, m0, year_basis)
                return _linear(m0, r0_act, m1, r1, mat_res)
            r1_mon = money_market_rate(r1, m1, year_basis)
            return _linear(m0, r0, m1, r1_mon, mat_res)

    # Ne devrait pas arriver (mat_res est encadré).
    return pts[-1][1]
