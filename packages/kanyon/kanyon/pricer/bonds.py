"""Typage et pricing des obligations à taux fixe (marché marocain).

``price_fixed_bond`` reproduit la logique de valorisation d'origine
(obligations ordinaires et titres atypiques à un ou plusieurs flux), nettoyée et
dotée d'une interface explicite : l'appelant fournit les caractéristiques du
titre, la date de valorisation et le taux de courbe correspondant ; les dates de
coupon sont déduites en interne.
"""
from __future__ import annotations

import datetime as dt
from typing import Dict

from kanyon.helpers import add_years
from kanyon.pricer.coupons import (
    accrued_coupon,
    first_coupon,
    next_coupon,
    previous_coupon,
)

# Typologies de titres reconnues.
ORDINARY = "obligation_ordinaire"
ATYPICAL_ONE_FLOW = "atypique_un_flux"
ATYPICAL_N_FLOWS = "atypique_n_flux"


def bond_type(
    date_valeur: dt.date,
    date_emission: dt.date,
    date_jouissance: dt.date,
    date_echeance: dt.date,
) -> str:
    """Classe un titre selon ses dates caractéristiques."""
    if date_emission.day != date_jouissance.day or date_emission.month != date_jouissance.month:
        return ATYPICAL_N_FLOWS
    if (
        date_emission == date_jouissance
        and date_emission.day == date_echeance.day
        and date_emission.month == date_echeance.month
    ):
        return ORDINARY
    if first_coupon(date_jouissance, date_echeance) == date_echeance:
        return ATYPICAL_ONE_FLOW
    return ORDINARY


def price_fixed_bond(
    *,
    date_valeur: dt.date,
    date_emission: dt.date,
    date_jouissance: dt.date,
    date_echeance: dt.date,
    taux_facial: float,
    taux_courbe: float,
    nominal: float,
) -> Dict[str, float]:
    """Valorise une obligation à taux fixe à une date donnée.

    Args:
        date_valeur: Date de valorisation.
        date_emission: Date d'émission.
        date_jouissance: Date de jouissance.
        date_echeance: Date d'échéance.
        taux_facial: Taux facial en décimal (ex. 0.035).
        taux_courbe: Taux de la courbe pour la maturité résiduelle (décimal).
        nominal: Nominal du titre.

    Returns:
        Un dictionnaire ``{price, dirty_price, coupon_couru, taux_courbe,
        type, maturite_initiale, maturite_residuelle}``. ``price`` est le prix
        pied de coupon, ``dirty_price`` inclut le coupon couru.
    """
    mat_init = (date_echeance - date_emission).days
    mat_res = (date_echeance - date_valeur).days
    ttype = bond_type(date_valeur, date_emission, date_jouissance, date_echeance)
    premier = first_coupon(date_jouissance, date_echeance)
    cp = previous_coupon(date_valeur, date_emission, date_jouissance, date_echeance)
    cs = next_coupon(date_valeur, date_jouissance, date_echeance)
    year_basis = (add_years(cp, 1) - cp).days
    tc = taux_courbe

    price = _clean_price(
        ttype=ttype,
        date_valeur=date_valeur,
        date_emission=date_emission,
        date_jouissance=date_jouissance,
        premier_coupon=premier,
        cs=cs,
        year_basis=year_basis,
        taux_facial=taux_facial,
        taux_courbe=tc,
        nominal=nominal,
        mat_init=mat_init,
        mat_res=mat_res,
    )
    cc = accrued_coupon(date_valeur, date_emission, date_jouissance, date_echeance, taux_facial, nominal)
    return {
        "price": round(price, 5),
        "dirty_price": round(price + cc, 5),
        "coupon_couru": round(cc, 5),
        "taux_courbe": round(tc, 6),
        "type": ttype,
        "maturite_initiale": mat_init,
        "maturite_residuelle": mat_res,
    }


def _clean_price(
    *,
    ttype: str,
    date_valeur: dt.date,
    date_emission: dt.date,
    date_jouissance: dt.date,
    premier_coupon: dt.date,
    cs: dt.date,
    year_basis: float,
    taux_facial: float,
    taux_courbe: float,
    nominal: float,
    mat_init: int,
    mat_res: int,
) -> float:
    """Prix pied de coupon selon la typologie et les maturités."""
    tc = taux_courbe
    tf = taux_facial
    n = nominal
    a = year_basis

    # Titre de maturité initiale < 1 an : actualisation monétaire simple.
    if mat_init < 366:
        return n * (1 + tf * mat_init / 360) / (1 + tc * mat_res / 360)

    # Maturité résiduelle < 1 an : dernier flux, actualisation monétaire.
    if mat_res < 366:
        if (ttype == ATYPICAL_N_FLOWS and date_valeur <= premier_coupon) or ttype == ATYPICAL_ONE_FLOW:
            return n * (1 + tf * mat_init / a) / (1 + tc * mat_res / 360)
        return n * (1 + tf) / (1 + tc * mat_res / 360)

    # Maturité résiduelle >= 1 an : actualisation actuarielle des flux.
    nb_coupons = int(mat_res / 365.25) + 1
    nb_jours = (cs - date_valeur).days

    if ttype == ORDINARY or (ttype == ATYPICAL_N_FLOWS and date_valeur >= premier_coupon):
        s = sum(tf / (1 + tc) ** (j - 1) for j in range(1, nb_coupons + 1))
        return n / (1 + tc) ** (nb_jours / a) * (s + 1 / (1 + tc) ** (nb_coupons - 1))

    if ttype == ATYPICAL_N_FLOWS and date_valeur < premier_coupon:
        if date_valeur <= date_jouissance:
            nb_coupons -= 1
        s = sum(tf / (1 + tc) ** (j - 1) for j in range(2, nb_coupons + 1))
        premier_frac = tf * (premier_coupon - date_emission).days / a
        return n / (1 + tc) ** (nb_jours / a) * (premier_frac + s + 1 / (1 + tc) ** (nb_coupons - 1))

    if ttype == ATYPICAL_ONE_FLOW:
        return n * (1 + tf * mat_init / a) / (1 + tc) ** (nb_jours / a)

    # Repli : actualisation actuarielle ordinaire.
    s = sum(tf / (1 + tc) ** (j - 1) for j in range(1, nb_coupons + 1))
    return n / (1 + tc) ** (nb_jours / a) * (s + 1 / (1 + tc) ** (nb_coupons - 1))
