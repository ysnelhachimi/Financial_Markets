"""Extraction des indices de la Bourse de Casablanca (MASI et sectoriels).

Récupère l'historique des indices via le formulaire public de la Bourse
(téléchargement piloté par Selenium), normalise les données puis les insère
dans la table ``masi_indices``.
"""
from __future__ import annotations

import logging

import bs4
import pandas as pd

from kanyon.config import ensure_download_dir
from kanyon.db.base import get_engine
from kanyon.db.models import MasiIndices
from kanyon.helpers import sanitize_float, to_date
from kanyon.imports._common import build_chrome_driver, run_incremental_import

logger = logging.getLogger(__name__)

# Correspondance code -> libellé des indices de la Bourse de Casablanca.
DICO_INDICE = {
    "AGRO": "AGROALIMENTAIRE / PRODUCTION",
    "ASSUR": "ASSURANCES",
    "BANK": "BANQUES",
    "B&MC": "BATIMENT & MATERIAUX DE CONSTRUCTION",
    "BOISS": "BOISSONS",
    "ESGI": "Casablanca ESG 10",
    "CHIM": "CHIMIE",
    "DISTR": "DISTRIBUTEURS",
    "ELEC": "ELECTRICITE",
    "EEE": "EQUIPEMENTS ELECTRONIQUES & ELECTRIQUES",
    "ESGIO": "ESGI OUVERTURE",
    "PHARM": "INDUSTRIE PHARMACEUTIQUE",
    "I&BEI": "INGENIERIES & BIENS D'EQUIPEMENT INDUSTRIELS",
    "L&H": "LOISIRS ET HOTELS",
    "MADX": "MADEX",
    "MADXR": "MADEX RENTABILITE BRUT",
    "MADRN": "MADEX RENTABILITE NET",
    "MASI": "MASI",
    "MASIE": "MASI (EUR)",
    "MASID": "MASI (USD)",
    "MASIO": "MASI OUVERTURE",
    "MASIR": "MASI RENTABILITE BRUT",
    "MASRN": "MASI RENTABILITE NET",
    "L&SI": "MATERIELS,LOGICIELS & SERVICES INFORMATIQUES",
    "MINES": "MINES",
    "MSI20": "Morocco Stock Index 20",
    "IMMOB": "PARTICIPATION ET PROMOTION IMMOBILIERES",
    "P&G": "PETROLE & GAZ",
    "SAC": "SERVICES AUX COLLECTIVITES",
    "SDT": "SERVICES DE TRANSPORT",
    "SF&AF": "SOCIETE DE FINANCEMENT & AUTRES ACTIVITES FINANCIERES",
    "SPI": "SOCIETES DE PLACEMENT IMMOBILIER",
    "SP&H": "SOCIETES DE PORTEFEUILLES - HOLDINGS",
    "S&P": "SYLVICULTURE & PAPIER",
    "TCOM": "TELECOMMUNICATIONS",
    "TRANS": "TRANSPORT",
}

INDICE_COLS = ["seance", "instrument", "variation"]

URL_INDICE = "http://www.casablanca-bourse.com/bourseweb/indice-historique.aspx?Cat=22&IdLink=299"

XML_FILE = "Telechargement-indice.aspx"


def extract_table_indice(soup: bs4.BeautifulSoup, cols: list) -> list:
    """Extrait les lignes du tableau HTML de l'historique des indices."""
    rows = []
    for row in soup.find_all("tr")[1:]:
        values = [span.text for span in row.find_all("span")]
        rows.append(dict(zip(cols, values)))
    return rows


def sanitize_table_indice(records: list) -> list:
    """Normalise dates et flottants des enregistrements d'indice."""
    for el in records:
        el["seance"] = to_date(el["seance"])
        el["instrument"] = sanitize_float(el["instrument"])
        el["variation"] = sanitize_float(el["variation"])
    return records


def extract_indice(indice: str, d_i: str, d_f: str) -> list:
    """Télécharge et parse l'historique d'un indice entre deux dates.

    Args:
        indice: Code de l'indice (clé de :data:`DICO_INDICE`).
        d_i: Date initiale (``dd/mm/YYYY``).
        d_f: Date finale (``dd/mm/YYYY``).

    Returns:
        Liste de dictionnaires (une ligne par séance), normalisée.
    """
    from selenium.webdriver.common.by import By

    download_dir = ensure_download_dir()
    driver = build_chrome_driver()
    try:
        driver.get(URL_INDICE)

        elem = driver.find_element(By.ID, "IndiceHistorique1_RBSearchDate")
        date_ini = driver.find_element(By.ID, "IndiceHistorique1_DateTimeControl1_TBCalendar")
        date_fini = driver.find_element(By.ID, "IndiceHistorique1_DateTimeControl2_TBCalendar")
        download = driver.find_element(By.ID, "IndiceHistorique1_LinkButton1")
        listbox = driver.find_element(By.ID, "IndiceHistorique1_DDLIndice")

        driver.execute_script("arguments[0].click()", elem)
        driver.execute_script(f"arguments[0].value = '{indice}'", listbox)
        driver.execute_script(f"arguments[0].value='{d_i}'", date_ini)
        driver.execute_script(f"arguments[0].value='{d_f}'", date_fini)
        driver.execute_script("arguments[0].click()", download)

        _wait_for_download(driver)
    finally:
        driver.quit()

    xml_path = download_dir / XML_FILE
    data = xml_path.read_text(encoding="utf-8", errors="ignore")
    soup = bs4.BeautifulSoup(data, "html.parser")
    records = sanitize_table_indice(extract_table_indice(soup, INDICE_COLS))
    xml_path.unlink(missing_ok=True)
    return records


def _wait_for_download(driver, timeout: int = 30) -> None:
    """Attend l'apparition du fichier téléchargé (poll léger)."""
    import time

    download_dir = ensure_download_dir()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (download_dir / XML_FILE).exists():
            return
        time.sleep(0.5)
    logger.warning("Fichier %s introuvable après %ss.", XML_FILE, timeout)


def get_days_imports_base(d_i: str, d_f: str) -> list:
    """Retourne les dates de séance ouvrées de la plage via l'indice MASI."""
    frame = pd.DataFrame(extract_indice("MASI", d_i, d_f))
    return list(frame["seance"]) if not frame.empty else []


def _import_indices_range(debut, fin) -> None:
    """Extrait et insère tous les indices de :data:`DICO_INDICE` sur une plage."""
    for code in DICO_INDICE:
        try:
            frame = pd.DataFrame(extract_indice(code, str(debut), str(fin)))
            if frame.empty:
                logger.info("Indice %s : aucune donnée du %s au %s.", code, debut, fin)
                continue
            frame.insert(1, "name", code)
            frame.to_sql("masi_indices", con=get_engine(), if_exists="append", index=False)
            logger.info("Indice %s importé du %s au %s.", code, debut, fin)
        except Exception:
            logger.exception("Échec de l'import de l'indice %s.", code)


def extract_implement_indice(d_i: str, d_f: str) -> None:
    """Importe (de façon incrémentale) les indices MASI entre deux dates.

    Args:
        d_i: Date initiale au format ``dd/mm/YYYY``.
        d_f: Date finale au format ``dd/mm/YYYY``.

    Example:
        >>> extract_implement_indice("12/06/2021", "08/07/2021")
    """
    run_incremental_import(
        model=MasiIndices,
        dedup_columns=["seance", "name", "instrument", "variation"],
        d_i=d_i,
        d_f=d_f,
        import_range=_import_indices_range,
        list_dates_provider=get_days_imports_base,
        label="masi_indices",
    )
