"""Dates de coupon et coupon couru des obligations à taux fixe.

Convention : coupons annuels aux anniversaires de la date de jouissance ; le
premier coupon tombe un an après la jouissance. Pour les titres émis avant leur
jouissance (émission ≠ jouissance), la première période court depuis l'émission.
"""
from __future__ import annotations

import datetime as dt

from kanyon.helpers import add_years


def first_coupon(date_jouissance: dt.date, date_echeance: dt.date) -> dt.date:
    """Date du premier coupon (un an après la jouissance), bornée à l'échéance."""
    return min(add_years(date_jouissance, 1), date_echeance)


def next_coupon(date_valeur: dt.date, date_jouissance: dt.date, date_echeance: dt.date) -> dt.date:
    """Date du prochain coupon strictement après ``date_valeur``, bornée à l'échéance."""
    coupon = add_years(date_jouissance, 1)
    while coupon <= date_valeur:
        coupon = add_years(coupon, 1)
    return min(coupon, date_echeance)


def previous_coupon(
    date_valeur: dt.date,
    date_emission: dt.date,
    date_jouissance: dt.date,
    date_echeance: dt.date,
) -> dt.date:
    """Date du coupon précédent (dernier coupon <= ``date_valeur``).

    Avant le premier coupon, la période court depuis la jouissance (ou depuis
    l'émission si le titre a été émis avant sa jouissance).
    """
    if date_valeur >= date_echeance:
        return date_echeance

    previous = date_jouissance
    coupon = add_years(date_jouissance, 1)
    while coupon <= date_valeur:
        previous = coupon
        coupon = add_years(coupon, 1)

    # Titre émis avant jouissance : première période depuis l'émission.
    if date_emission != date_jouissance and date_valeur < add_years(date_jouissance, 1):
        previous = date_emission
    return previous


def accrued_coupon(
    date_valeur: dt.date,
    date_emission: dt.date,
    date_jouissance: dt.date,
    date_echeance: dt.date,
    taux_facial: float,
    nominal: float,
) -> float:
    """Coupon couru à ``date_valeur`` (intérêts depuis le coupon précédent).

    Args:
        date_valeur: Date de valorisation.
        date_emission: Date d'émission du titre.
        date_jouissance: Date de jouissance.
        date_echeance: Date d'échéance.
        taux_facial: Taux facial (ex. 0.035 pour 3,5 %).
        nominal: Nominal du titre.

    Returns:
        Le montant du coupon couru.
    """
    prev = previous_coupon(date_valeur, date_emission, date_jouissance, date_echeance)
    year_basis = (add_years(prev, 1) - prev).days
    if date_valeur < date_echeance:
        return taux_facial * nominal * (date_valeur - prev).days / year_basis
    return taux_facial * nominal * (date_valeur - prev).days / 365
