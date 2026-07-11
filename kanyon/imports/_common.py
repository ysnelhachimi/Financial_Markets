"""Utilitaires partagés par les modules d'extraction.

Regroupe :

* la factorisation de la logique d'import incrémental (identique pour indices,
  volumes et composition dans le code d'origine) ;
* la fabrique d'un pilote Chrome/Selenium 4 configuré (headless, répertoire de
  téléchargement), remplaçant l'API Selenium 3 obsolète.

Selenium et ``webdriver_manager`` sont importés paresseusement afin que les
modules d'extraction restent importables même sans pile navigateur installée.
"""
from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from kanyon.config import HEADLESS, ensure_download_dir
from kanyon.db.base import get_session
from kanyon.helpers import to_date

logger = logging.getLogger(__name__)


def build_chrome_driver():
    """Construit un pilote Chrome headless (API Selenium 4).

    Le répertoire de téléchargement est celui défini dans :mod:`kanyon.config`.

    Returns:
        Une instance ``selenium.webdriver.Chrome`` prête à l'emploi.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    download_dir = str(ensure_download_dir())
    options = webdriver.ChromeOptions()
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "safebrowsing.enabled": True,
            "directory_upgrade": True,
        },
    )
    if HEADLESS:
        options.add_argument("--headless=new")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def existing_dates(model, dedup_columns: Iterable[str], session=None):
    """Retourne les dates de séance déjà présentes en base pour un modèle.

    Args:
        model: Classe ORM comportant une colonne ``seance``.
        dedup_columns: Colonnes servant à dédupliquer les lignes.
        session: Session SQLAlchemy optionnelle.

    Returns:
        Un tuple ``(premiere_date, derniere_date, liste_dates)``. Les dates sont
        des ``datetime.date`` triées ; le tuple vaut ``(None, None, [])`` si la
        table est vide.
    """
    own = session is None
    session = session or get_session()
    try:
        query = session.query(model)
        frame = pd.read_sql(query.statement, query.session.bind)
    finally:
        if own:
            session.close()

    if frame.empty:
        return None, None, []

    frame = (
        frame.drop(columns=[c for c in ("id",) if c in frame.columns])
        .sort_values("seance")
        .drop_duplicates(list(dedup_columns))
        .sort_values("seance")
    )
    dates = list(frame["seance"])
    return dates[0], dates[-1], dates


def missing_dates(all_dates, last_date_indb):
    """Filtre les dates strictement postérieures à la dernière date en base."""
    return [d for d in all_dates if d > last_date_indb]


def run_incremental_import(
    *,
    model,
    dedup_columns,
    d_i,
    d_f,
    import_range,
    list_dates_provider,
    label,
    session=None,
):
    """Orchestration commune d'un import incrémental.

    Détermine, à partir de l'état de la base, la plage de dates réellement à
    importer, puis délègue l'import effectif à ``import_range``.

    Args:
        model: Modèle ORM cible (doit exposer ``seance``).
        dedup_columns: Colonnes de déduplication pour l'état en base.
        d_i: Date initiale demandée (chaîne ``dd/mm/YYYY``).
        d_f: Date finale demandée (chaîne ``dd/mm/YYYY``).
        import_range: Callable ``(date_debut, date_fin) -> None`` réalisant
            l'extraction et l'insertion sur la plage donnée.
        list_dates_provider: Callable ``(d_i, d_f) -> list[date]`` fournissant
            les dates de séance ouvrées de la plage (via la source).
        label: Libellé lisible pour la journalisation.
        session: Session SQLAlchemy optionnelle.
    """
    first_indb, last_indb, dates_indb = existing_dates(model, dedup_columns, session)

    di_date, df_date = to_date(d_i), to_date(d_f)

    if first_indb is not None and di_date in dates_indb and df_date in dates_indb:
        logger.info(
            "%s : déjà présent du %s au %s (base : %s -> %s)",
            label, d_i, d_f, first_indb, last_indb,
        )
        return

    if (
        first_indb is not None
        and di_date in dates_indb
        and df_date not in dates_indb
        and df_date > last_indb
    ):
        dates_a_importer = missing_dates(list_dates_provider(d_i, d_f), last_indb)
        if not dates_a_importer:
            logger.info("%s : aucune nouvelle date à importer.", label)
            return
        debut, fin = dates_a_importer[0], dates_a_importer[-1]
        logger.info("%s : import du %s au %s.", label, debut, fin)
        import_range(debut, fin)
        return

    logger.info("%s : import du %s au %s.", label, d_i, d_f)
    import_range(d_i, d_f)
