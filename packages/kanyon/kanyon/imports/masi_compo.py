"""Extraction de la composition de l'indice MASI (pondérations).

Récupère les pondérations publiées par la Bourse de Casablanca (fichier de
téléchargement HTTP), les normalise, puis les insère dans ``masi_composition``.
"""
from __future__ import annotations

import logging

import bs4
import pandas as pd
import requests

from kanyon.db.base import get_engine
from kanyon.db.models import MasiComposition
from kanyon.helpers import sanitize_float, to_date
from kanyon.imports._common import run_incremental_import
from kanyon.imports.masi_indice import get_days_imports_base

logger = logging.getLogger(__name__)

URL_PONDERATION = (
    "http://www.casablanca-bourse.com/bourseweb/Telechargement/"
    "Telechargement-Ponderation.aspx"
)

PONDERATION_COLS = [
    "seance",
    "code_isin",
    "libelle",
    "nombre_de_titre",
    "cours",
    "facteur_flottant",
    "facteur_de_plafonnement",
    "capi_flottante",
    "poids",
]

# Délai (secondes) pour les requêtes HTTP sortantes.
REQUEST_TIMEOUT = 30


def extract_table_ponderation(soup: bs4.BeautifulSoup, cols: list) -> list:
    """Extrait les lignes du tableau HTML des pondérations."""
    rows = []
    for row in soup.find_all("tr")[1:]:
        values = [span.text for span in row.find_all("span")]
        rows.append(dict(zip(cols, values)))
    return rows


def sanitize_table_ponderation(records: list) -> list:
    """Normalise dates, ISIN, libellés et flottants de la composition."""
    for el in records:
        el["seance"] = to_date(el["seance"])
        el["code_isin"] = el["code_isin"].strip().upper()
        el["libelle"] = el["libelle"].strip().upper()
        el["nombre_de_titre"] = int(sanitize_float(el["nombre_de_titre"]))
        el["cours"] = sanitize_float(el["cours"])
        el["facteur_flottant"] = sanitize_float(el["facteur_flottant"])
        el["facteur_de_plafonnement"] = sanitize_float(el["facteur_de_plafonnement"])
        el["capi_flottante"] = sanitize_float(el["capi_flottante"])
        el["poids"] = sanitize_float(el["poids"])
    return records


def extract_masi_compo(debut, fin) -> list:
    """Télécharge et parse la composition MASI entre deux dates.

    Args:
        debut: Date initiale (chaîne ``dd/mm/YYYY`` ou date).
        fin: Date finale (chaîne ``dd/mm/YYYY`` ou date).

    Returns:
        Liste de dictionnaires normalisés (une ligne par valeur/séance).
    """
    deb, fin = to_date(debut), to_date(fin)
    params = {
        "Historique": "Ponderation",
        "dateDeb": f"{deb:%d/%m/%Y}00:00:00",
        "dateFin": f"{fin:%d/%m/%Y}00:00:00",
        "var": "MASI",
    }
    response = requests.get(URL_PONDERATION, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    # html5lib requis pour ce contenu ; installer via `pip install html5lib`.
    soup = bs4.BeautifulSoup(response.text, "html5lib")
    return sanitize_table_ponderation(extract_table_ponderation(soup, PONDERATION_COLS))


def _import_compo_range(debut, fin) -> None:
    """Extrait et insère la composition MASI sur une plage de dates."""
    try:
        frame = pd.DataFrame(extract_masi_compo(str(debut), str(fin)))
        if frame.empty:
            logger.info("Composition MASI : aucune donnée du %s au %s.", debut, fin)
            return
        frame.to_sql("masi_composition", con=get_engine(), if_exists="append", index=False)
        logger.info("Composition MASI importée du %s au %s.", debut, fin)
    except Exception:
        logger.exception("Échec de l'import de la composition MASI.")


def extract_implement_compo(d_i: str, d_f: str) -> None:
    """Importe (de façon incrémentale) la composition MASI entre deux dates.

    Args:
        d_i: Date initiale au format ``dd/mm/YYYY``.
        d_f: Date finale au format ``dd/mm/YYYY``.

    Example:
        >>> extract_implement_compo("12/06/2021", "08/07/2021")
    """
    run_incremental_import(
        model=MasiComposition,
        dedup_columns=["seance", "code_isin", "libelle", "cours", "capi_flottante", "poids"],
        d_i=d_i,
        d_f=d_f,
        import_range=_import_compo_range,
        list_dates_provider=get_days_imports_base,
        label="masi_composition",
    )
