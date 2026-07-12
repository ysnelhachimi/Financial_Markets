"""Tests des utilitaires de parsing."""
import datetime as dt

import pytest

from kanyon.helpers import sanitize_float, to_date, upper_string


class TestToDate:
    def test_format_jj_mm_aaaa(self):
        assert to_date("12/06/2021") == dt.date(2021, 6, 12)

    def test_format_iso(self):
        assert to_date("2021-06-12") == dt.date(2021, 6, 12)

    def test_datetime_passthrough(self):
        assert to_date(dt.datetime(2021, 6, 12, 9, 30)) == dt.date(2021, 6, 12)

    def test_date_passthrough(self):
        d = dt.date(2021, 6, 12)
        assert to_date(d) == d

    def test_format_inconnu(self):
        with pytest.raises(ValueError):
            to_date("pas une date")


class TestSanitizeFloat:
    def test_virgule_decimale(self):
        assert sanitize_float("1234,56") == pytest.approx(1234.56)

    def test_separateur_milliers(self):
        assert sanitize_float("1 234,56") == pytest.approx(1234.56)

    def test_entier(self):
        assert sanitize_float("42") == pytest.approx(42.0)


class TestUpperString:
    def test_trim_et_majuscules(self):
        assert upper_string("  attijariwafa  ") == "ATTIJARIWAFA"

    def test_none(self):
        assert upper_string(None) is None
