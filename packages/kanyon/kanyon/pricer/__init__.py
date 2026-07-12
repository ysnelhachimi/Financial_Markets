"""Pricer obligataire marocain (Yield Harbor).

Ce package porte, nettoie et rend testable le moteur de pricing d'origine :

* :mod:`kanyon.pricer.dates`   — tenors normalisés et calcul de périodes.
* :mod:`kanyon.pricer.curve`   — interpolation de la courbe des taux BKAM
  (conventions monétaire / actuarielle), fonctions **pures** (données injectées).
* :mod:`kanyon.pricer.coupons` — dates de coupon (premier, suivant, précédent)
  et coupon couru.
* :mod:`kanyon.pricer.bonds`   — typage des titres et pricing (``price_oblig_fixe``).
* :mod:`kanyon.pricer.service` — orchestration : lecture des données via
  :mod:`kanyon.db.queries` puis appel des fonctions pures.

Le cœur mathématique ne dépend pas de la base de données (il reçoit la courbe
sous forme de points), ce qui le rend unitairement testable ; seule la couche
``service`` touche la base.
"""
from __future__ import annotations

from kanyon.pricer.bonds import bond_type, price_fixed_bond
from kanyon.pricer.coupons import (
    accrued_coupon,
    next_coupon,
    previous_coupon,
    first_coupon,
)
from kanyon.pricer.curve import (
    interpolate_curve,
    money_market_rate,
    actuarial_rate,
)
from kanyon.pricer.dates import TENOR_KEYS, period_days, tenor_days

__all__ = [
    "bond_type",
    "price_fixed_bond",
    "accrued_coupon",
    "next_coupon",
    "previous_coupon",
    "first_coupon",
    "interpolate_curve",
    "money_market_rate",
    "actuarial_rate",
    "TENOR_KEYS",
    "period_days",
    "tenor_days",
]
