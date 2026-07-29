"""Amorce des données de marché de démonstration dans la base kanyon.

Insère une courbe des taux BKAM, quelques titres (référentiel Maroclear), des
tenors et des indices MASI, afin de rendre le pricer et le tableau de bord
immédiatement démontrables.

Usage :
    KANYON_DATABASE_URL=sqlite:///./kanyon_data.db python scripts/seed_demo.py
"""
from __future__ import annotations

import datetime as dt

import numpy as np

from kanyon.db.base import get_engine, get_session, Base
from kanyon.db import models
from kanyon.db.models import BkamCourbe, LexActions, LexIndiceAct, MasiIndices, MasiVolume, Mcl

DATE_MARCHE = dt.date(2021, 7, 1)

# Courbe des taux : (maturité en jours, taux décimal).
CURVE = [
    (91, 0.0165), (182, 0.0180), (364, 0.0205),
    (730, 0.0235), (1825, 0.0270), (3650, 0.0305),
    (5475, 0.0330), (7300, 0.0345), (10950, 0.0360),
]

# Titres du référentiel Maroclear (obligations du Trésor fictives mais cohérentes).
TITRES = [
    {
        "code_isin": "MA0000091234",
        "nom_emetteur": "TRESOR",
        "famille_instrument": "BDT",
        "categorie_instrument": "OT",
        "taux_facial": "3.50",
        "date_emission": dt.date(2015, 6, 15),
        "date_jouissance": dt.date(2015, 6, 15),
        "date_echeance": dt.date(2025, 6, 15),
        "nominal": 100000.0,
        "nominal_actuel": 100000.0,
    },
    {
        "code_isin": "MA0000095678",
        "nom_emetteur": "TRESOR",
        "famille_instrument": "BDT",
        "categorie_instrument": "OT",
        "taux_facial": "2.75",
        "date_emission": dt.date(2019, 3, 20),
        "date_jouissance": dt.date(2019, 3, 20),
        "date_echeance": dt.date(2029, 3, 20),
        "nominal": 100000.0,
        "nominal_actuel": 100000.0,
    },
]

INDICES = [
    (dt.date(2021, 6, 28), "MASI", 12050.3, "0.30"),
    (dt.date(2021, 6, 29), "MASI", 12088.1, "0.31"),
    (dt.date(2021, 6, 30), "MASI", 12110.7, "0.19"),
    (dt.date(2021, 7, 1), "MASI", 12145.2, "0.28"),
]

# Séries de cours quotidiens (démo) pour l'optimisation/backtest sur données réelles.
# (nom, cours de départ, dérive quotidienne, volatilité quotidienne)
VALEURS_DEMO = [
    ("ITISSALAT AL-MAGHRIB", 135.0, 0.0004, 0.011),
    ("ATTIJARIWAFA BANK", 480.0, 0.0006, 0.015),
    ("BCP", 265.0, 0.0003, 0.013),
    ("COSUMAR", 235.0, 0.0002, 0.009),
    ("LAFARGEHOLCIM MAR", 2100.0, 0.0005, 0.017),
]
VOLUME_START = dt.date(2021, 4, 6)
VOLUME_DAYS = 60


def _generate_volume_rows():
    """Génère des séries de cours quotidiens réalistes pour les valeurs de démo."""
    rng = np.random.default_rng(42)
    # Jours ouvrés (lun-ven).
    dates = []
    d = VOLUME_START
    while len(dates) < VOLUME_DAYS:
        if d.weekday() < 5:
            dates.append(d)
        d += dt.timedelta(days=1)

    rows = []
    for name, start, drift, vol in VALEURS_DEMO:
        price = start
        for seance in dates:
            price *= 1 + rng.normal(drift, vol)
            rows.append(
                MasiVolume(
                    seance=seance, name=name, cours_cloture=round(price, 2),
                    cours_ajuste=round(price, 2), evolution=0.0,
                    quantite_echange=float(rng.integers(1000, 50000)), volume=round(price * 1000, 2),
                )
            )
    return rows


def seed() -> None:
    Base.metadata.create_all(bind=get_engine())
    session = get_session()
    try:
        if session.query(BkamCourbe).count() == 0:
            for maturity_days, rate in CURVE:
                session.add(
                    BkamCourbe(
                        date_marche=DATE_MARCHE,
                        date_valeur=DATE_MARCHE,
                        date_echeance=DATE_MARCHE + dt.timedelta(days=maturity_days),
                        transactions=1_000_000.0,
                        taux=rate,
                        date_transaction=DATE_MARCHE,
                    )
                )

        if session.query(Mcl).count() == 0:
            for t in TITRES:
                session.add(Mcl(**t))

        if session.query(MasiIndices).count() == 0:
            session.add(LexIndiceAct(code="MASI", libelle="MASI"))
            for seance, name, instrument, variation in INDICES:
                session.add(
                    MasiIndices(seance=seance, name=name, instrument=instrument, variation=variation)
                )

        if session.query(LexActions).count() == 0:
            session.add(
                LexActions(
                    code_isin="MA0000012345",
                    ticker="IAM",
                    description="ITISSALAT AL-MAGHRIB",
                    secteur="TELECOMMUNICATIONS",
                    profil="Défensif",
                    cyclicite="Faible",
                )
            )

        if session.query(MasiVolume).count() == 0:
            for row in _generate_volume_rows():
                session.add(row)

        session.commit()
        print("Données de démonstration insérées.")
        print(f"  Courbe : {session.query(BkamCourbe).count()} points au {DATE_MARCHE}")
        print(f"  Titres : {session.query(Mcl).count()}")
        print(f"  Indices: {session.query(MasiIndices).count()}")
        print(f"  Volumes actions: {session.query(MasiVolume).count()} lignes")
    finally:
        session.close()


if __name__ == "__main__":
    seed()
