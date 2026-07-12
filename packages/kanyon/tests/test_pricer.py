"""Tests du pricer obligataire (fonctions pures, sans base de données)."""
import datetime as dt

import pytest

from kanyon.pricer import (
    bond_type,
    price_fixed_bond,
    interpolate_curve,
    money_market_rate,
    actuarial_rate,
    first_coupon,
    next_coupon,
    previous_coupon,
    accrued_coupon,
    period_days,
    tenor_days,
    TENOR_KEYS,
)
from kanyon.pricer.dates import tenor_map


class TestDates:
    def test_semaines(self):
        assert period_days(dt.date(2021, 1, 1), "13:S") == 91

    def test_un_an(self):
        assert period_days(dt.date(2021, 1, 1), "1:A") == 365

    def test_un_an_bissextile(self):
        assert period_days(dt.date(2020, 1, 1), "1:A") == 366

    def test_tenors_complet(self):
        assert len(tenor_days(dt.date(2021, 7, 1))) == len(TENOR_KEYS) == 11
        assert set(tenor_map(dt.date(2021, 7, 1))) == set(TENOR_KEYS)


class TestCurve:
    def test_conversions_inverses(self):
        # actuariel -> monétaire -> actuariel redonne le taux de départ.
        r = 0.032
        mono = money_market_rate(r, 200, 365)
        back = actuarial_rate(mono, 200, 365)
        assert back == pytest.approx(r, rel=1e-9)

    def test_interpolation_au_noeud(self):
        points = [(91, 0.02), (365, 0.03), (730, 0.035)]
        assert interpolate_curve(points, 365, 365) == pytest.approx(0.03)

    def test_interpolation_lineaire(self):
        points = [(365, 0.03), (730, 0.035), (1095, 0.04)]
        # milieu du segment 730-1095
        expected = 0.035 + (0.04 - 0.035) * (912 - 730) / (1095 - 730)
        assert interpolate_curve(points, 912, 365) == pytest.approx(expected)

    def test_extrapolation_haute(self):
        points = [(365, 0.03), (730, 0.035)]
        assert interpolate_curve(points, 900, 365) > 0.035

    def test_courbe_vide(self):
        with pytest.raises(ValueError):
            interpolate_curve([], 365, 365)


class TestCoupons:
    J = dt.date(2015, 6, 15)   # jouissance
    E = dt.date(2025, 6, 15)   # échéance

    def test_premier_coupon(self):
        assert first_coupon(self.J, self.E) == dt.date(2016, 6, 15)

    def test_coupon_suivant(self):
        assert next_coupon(dt.date(2021, 3, 1), self.J, self.E) == dt.date(2021, 6, 15)

    def test_coupon_precedent(self):
        assert previous_coupon(dt.date(2021, 8, 1), self.J, self.J, self.E) == dt.date(2021, 6, 15)

    def test_coupon_couru_positif(self):
        cc = accrued_coupon(dt.date(2021, 12, 15), self.J, self.J, self.E, 0.03, 100)
        assert cc > 0


class TestPricing:
    def test_type_ordinaire(self):
        d = dt.date
        assert bond_type(d(2021, 1, 1), d(2015, 6, 15), d(2015, 6, 15), d(2025, 6, 15)) == "obligation_ordinaire"

    def test_obligation_au_pair(self):
        # Taux de courbe == taux facial, à une date de coupon => cote au pair.
        res = price_fixed_bond(
            date_valeur=dt.date(2021, 6, 15),
            date_emission=dt.date(2015, 6, 15),
            date_jouissance=dt.date(2015, 6, 15),
            date_echeance=dt.date(2025, 6, 15),
            taux_facial=0.03,
            taux_courbe=0.03,
            nominal=100,
        )
        assert res["price"] == pytest.approx(100.0, abs=0.5)
        # À la date de coupon, le coupon couru est nul.
        assert res["coupon_couru"] == pytest.approx(0.0, abs=1e-6)
        assert res["dirty_price"] == pytest.approx(res["price"] + res["coupon_couru"])

    def test_prix_baisse_si_taux_monte(self):
        base = dict(
            date_valeur=dt.date(2021, 6, 15),
            date_emission=dt.date(2015, 6, 15),
            date_jouissance=dt.date(2015, 6, 15),
            date_echeance=dt.date(2025, 6, 15),
            taux_facial=0.03,
            nominal=100,
        )
        p_bas = price_fixed_bond(taux_courbe=0.03, **base)["price"]
        p_haut = price_fixed_bond(taux_courbe=0.05, **base)["price"]
        assert p_haut < p_bas  # relation prix/taux inverse
