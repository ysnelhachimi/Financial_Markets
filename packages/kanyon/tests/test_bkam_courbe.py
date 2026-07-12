"""Test du parsing de la courbe BKAM (fonction pure, sans réseau)."""
import datetime as dt

import pytest

from kanyon.imports.bkam_courbe import parse_courbe_csv

# Deux lignes d'en-tête (ignorées), puis les données et une ligne "Total".
SAMPLE_CSV = (
    "Courbe des taux BKAM;;;\n"
    "Marche secondaire;;;\n"
    "15/07/2025; 1 000 000,00; 2,05%; 01/07/2021\n"
    "15/07/2030; 500 000,00; 2,70%; 01/07/2021\n"
    "Total; 1 500 000,00; ; \n"
)


def test_parse_courbe_csv():
    df = parse_courbe_csv(SAMPLE_CSV, dt.date(2021, 7, 1))

    # La ligne "Total" est retirée ; 2 lignes de données restent.
    assert len(df) == 2
    assert list(df.columns) == [
        "date_marche", "date_echeance", "transactions", "taux", "date_valeur", "date_transaction",
    ]
    # Taux converti en décimal.
    assert df.iloc[0]["taux"] == pytest.approx(0.0205)
    assert df.iloc[1]["taux"] == pytest.approx(0.0270)
    # Dates typées.
    assert df.iloc[0]["date_echeance"] == dt.date(2025, 7, 15)
    assert df.iloc[0]["date_valeur"] == dt.date(2021, 7, 1)
    assert df.iloc[0]["date_marche"] == dt.date(2021, 7, 1)
    # Transactions numériques.
    assert df.iloc[0]["transactions"] == pytest.approx(1_000_000.0)
