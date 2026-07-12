"""Import de la courbe des taux secondaire de Bank Al-Maghrib (BKAM).

BKAM publie la courbe secondaire sous forme d'un export CSV daté. Ce module
télécharge cet export, le normalise et l'insère (de façon incrémentale) dans la
table ``bkam_courbe``, qui alimente le pricer obligataire.

Le parsing est isolé dans :func:`parse_courbe_csv` (fonction pure, testable sans
réseau) ; seul :func:`import_courbe_bam` touche le réseau et la base.
"""
from __future__ import annotations

import datetime as dt
import io
import logging

import pandas as pd

from kanyon.db.base import get_engine, get_session
from kanyon.db.models import BkamCourbe
from kanyon.helpers import to_date

logger = logging.getLogger(__name__)

# Point d'export CSV de la courbe secondaire BKAM (paramétré par la date).
BKAM_CURVE_URL = (
    "https://www.bkam.ma/export/blockcsv/2340/"
    "c3367fcefc5f524397748201aee5dab8/"
    "e1d6b9bbf87f86f8ba53e8518e882982"
    "?date={date}&block=e1d6b9bbf87f86f8ba53e8518e882982"
)

CURVE_COLUMNS = ["date_echeance", "transactions", "taux", "date_valeur"]

REQUEST_TIMEOUT = 30


def parse_courbe_csv(text: str, date_marche: dt.date) -> pd.DataFrame:
    """Transforme le CSV BKAM en DataFrame prêt pour ``bkam_courbe``.

    Args:
        text: Contenu CSV brut (séparateur ``;``, 2 lignes d'en-tête).
        date_marche: Date de marché associée à la courbe.

    Returns:
        Un DataFrame aux colonnes ``date_marche, date_echeance, transactions,
        taux, date_valeur, date_transaction`` (taux en décimal).
    """
    frame = pd.read_csv(
        io.StringIO(text),
        delimiter=";",
        skipinitialspace=True,
        engine="python",
        skiprows=2,
        header=None,
    )
    frame = frame.iloc[:, : len(CURVE_COLUMNS)]
    frame.columns = CURVE_COLUMNS

    # Nettoyage des cellules et suppression de la ligne "Total".
    frame = frame.apply(lambda col: col.astype(str).str.strip())
    frame = frame.drop(frame[frame["date_echeance"].str.lower() == "total"].index)
    frame = frame[frame["date_echeance"].str.len() > 0]

    frame["date_echeance"] = frame["date_echeance"].apply(to_date)
    frame["date_valeur"] = frame["date_valeur"].apply(to_date)

    taux = (
        frame["taux"].str.replace(" ", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    frame["taux"] = pd.to_numeric(taux) / 100.0
    frame["transactions"] = pd.to_numeric(
        frame["transactions"].str.replace(" ", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )

    frame.insert(0, "date_marche", date_marche)
    frame["date_transaction"] = frame["date_valeur"]
    return frame[["date_marche", "date_echeance", "transactions", "taux", "date_valeur", "date_transaction"]]


def fetch_courbe_csv(date_marche: dt.date) -> str:
    """Télécharge l'export CSV de la courbe BKAM pour une date."""
    import requests

    url = BKAM_CURVE_URL.format(date=date_marche.isoformat())
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.content.decode("latin-1", "ignore")


def _existing_curve_dates(session) -> set:
    query = session.query(BkamCourbe.date_marche).distinct()
    return {row[0] for row in query.all()}


def import_courbe_bam(date_marche, session=None) -> int:
    """Importe la courbe BKAM d'une date si elle est absente de la base.

    Args:
        date_marche: Date de marché (``date`` ou chaîne ``YYYY-MM-DD``).
        session: Session SQLAlchemy optionnelle.

    Returns:
        Le nombre de lignes insérées (0 si la date était déjà présente).
    """
    day = to_date(date_marche)
    own = session is None
    session = session or get_session()
    try:
        if day in _existing_curve_dates(session):
            logger.info("Courbe BKAM déjà en base pour le %s.", day)
            return 0
    finally:
        if own:
            session.close()

    frame = parse_courbe_csv(fetch_courbe_csv(day), day)
    if frame.empty:
        logger.info("Courbe BKAM vide pour le %s.", day)
        return 0
    frame.to_sql("bkam_courbe", con=get_engine(), if_exists="append", index=False)
    logger.info("Courbe BKAM importée pour le %s (%d lignes).", day, len(frame))
    return len(frame)
