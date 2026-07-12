"""Amorce des données de marché de démonstration dans la base kanyon.

Insère une courbe des taux BKAM, quelques titres (référentiel Maroclear), des
tenors et des indices MASI, afin de rendre le pricer et le tableau de bord
immédiatement démontrables.

Usage :
    KANYON_DATABASE_URL=sqlite:///./kanyon_data.db python scripts/seed_demo.py
"""
from __future__ import annotations

import datetime as dt

from kanyon.db.base import get_engine, get_session, Base
from kanyon.db import models
from kanyon.db.models import BkamCourbe, LexActions, LexIndiceAct, MasiIndices, Mcl

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

        session.commit()
        print("Données de démonstration insérées.")
        print(f"  Courbe : {session.query(BkamCourbe).count()} points au {DATE_MARCHE}")
        print(f"  Titres : {session.query(Mcl).count()}")
        print(f"  Indices: {session.query(MasiIndices).count()}")
    finally:
        session.close()


if __name__ == "__main__":
    seed()
