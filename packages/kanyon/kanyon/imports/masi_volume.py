"""Extraction des volumes de négociation par valeur (Bourse de Casablanca).

Récupère l'historique des cours et volumes valeur par valeur via le formulaire
public (téléchargement piloté par Selenium), normalise les données puis les
insère dans ``masi_volume``.
"""
from __future__ import annotations

import logging

import pandas as pd
from bs4 import BeautifulSoup

from kanyon.config import ensure_download_dir
from kanyon.db.base import get_engine
from kanyon.db.models import MasiVolume
from kanyon.helpers import sanitize_float, to_date
from kanyon.imports._common import build_chrome_driver, run_incremental_import
from kanyon.imports.masi_indice import get_days_imports_base

logger = logging.getLogger(__name__)

URL_VOLUMES = "http://www.casablanca-bourse.com/bourseweb/Negociation-Historique.aspx?Cat=24&IdLink=302"

XML_FILE = "Telechargement-Histo-Valeur.aspx"

VOLUMES_COLS = [
    "seance",
    "cours_cloture",
    "cours_ajuste",
    "evolution",
    "quantite_echange",
    "volume",
]

# Correspondance code interne -> libellé de la valeur cotée.
DICO_VALEURS = {
    "9000  ": "DOUJA PROM ADDOHA",
    "11200 ": "ALLIANCES",
    "11700 ": "AFRIC INDUSTRIES SA",
    "12200 ": "AFMA",
    "6700  ": "AGMA",
    "6600  ": "ALUMINIUM DU MAROC",
    "27    ": "ARADEI CAPITAL",
    "3200  ": "AUTO HALL",
    "10300 ": "ATLANTASANAD",
    "8200  ": "ATTIJARIWAFA BANK",
    "3300  ": "BALIMA",
    "5100  ": "BMCI",
    "8000  ": "BCP",
    "1100  ": "BANK OF AFRICA",
    "3900  ": "CENTRALE DANONE",
    "3600  ": "CDM",
    "3100  ": "CIH",
    "4000  ": "CIMENTS DU MAROC",
    "11000 ": "MINIERE TOUISSIT",
    "9200  ": "COLORADO",
    "8900  ": "CARTIER SAADA",
    "4100  ": "COSUMAR",
    "2200  ": "CTM",
    "10900 ": "DELTA HOLDING",
    "4200  ": "DIAC SALAF",
    "10800 ": "DELATTRE LEVIVIER MAROC",
    "8500  ": "DARI COUSPATE",
    "9700  ": "DISWAY",
    "2300  ": "EQDOM",
    "9300  ": "FENIE BROSSETTE",
    "7100  ": "AFRIQUIA GAZ",
    "9600  ": "HPS",
    "8001  ": "ITISSALAT AL-MAGHRIB",
    "7600  ": "IB MAROC.COM",
    "12    ": "IMMORENTE INVEST",
    "9500  ": "INVOLYS",
    "11600 ": "JET CONTRACTORS",
    "11100 ": "LABEL VIE",
    "4800  ": "LESIEUR CRISTAL",
    "3800  ": "LAFARGEHOLCIM MAR",
    "8600  ": "LYDEC",
    "10000 ": "M2M Group",
    "1600  ": "MAGHREBAIL",
    "6500  ": "MED PAPER",
    "10600 ": "MICRODATA",
    "2500  ": "MAROC LEASING",
    "7300  ": "MANAGEM",
    "7200  ": "MAGHREB OXYGENE",
    "12300 ": "SODEP-Marsa Maroc",
    "21    ": "MUTANDIS SCA",
    "7000  ": "AUTO NEJMA",
    "7400  ": "NEXANS MAROC",
    "11300 ": "ENNAKL",
    "5200  ": "OULMES",
    "9900  ": "PROMOPHARM S.A.",
    "12000 ": "RES DAR SAADA",
    "5300  ": "REBAB COMPANY",
    "8700  ": "RISMA",
    "11800 ": "S.M MONETIQUE",
    "11400 ": "SAHAM ASSURANCE",
    "6800  ": "SAMIR",
    "2000  ": "SOCIETE DES BOISSONS DU MAROC",
    "1300  ": "SONASID",
    "10700 ": "SALAFIN",
    "1500  ": "SMI",
    "10400 ": "STOKVIS NORD AFRIQUE",
    "10500 ": "SNEP",
    "9800  ": "SOTHEMA",
    "9400  ": "REALISATIONS MECANIQUES",
    "11500 ": "STROC INDUSTRIE",
    "29    ": "TGCC S.A",
    "10100 ": "TIMAR",
    "12100 ": "TOTAL MAROC",
    "11900 ": "TAQA MOROCCO",
    "7500  ": "UNIMER",
    "6400  ": "WAFA ASSURANCE",
    "5800  ": "ZELLIDJA S.A",
}


def extract_volume_content(act_mad: str) -> pd.DataFrame:
    """Lit et normalise le fichier téléchargé pour une valeur donnée.

    Args:
        act_mad: Code interne de la valeur (clé de :data:`DICO_VALEURS`).

    Returns:
        Un ``DataFrame`` normalisé (dates, flottants, colonne ``name``).
    """
    download_dir = ensure_download_dir()
    xml_path = download_dir / XML_FILE
    data = xml_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(data, "html.parser")

    rows = [
        [cell.text.strip() for cell in row.find_all("td")]
        for row in soup.find_all("tr")[1:]
    ]

    frame = pd.DataFrame(rows, columns=VOLUMES_COLS)
    frame.insert(1, "name", DICO_VALEURS[act_mad])
    frame["seance"] = frame["seance"].map(to_date)
    for col in ("cours_cloture", "cours_ajuste", "evolution", "volume"):
        frame[col] = frame[col].map(sanitize_float)
    frame["quantite_echange"] = frame["quantite_echange"].map(sanitize_float)

    xml_path.unlink(missing_ok=True)
    return frame


def extract_volume(act_mad: str, d_i: str, d_f: str) -> pd.DataFrame:
    """Télécharge et parse l'historique de cours/volume d'une valeur.

    Args:
        act_mad: Code interne de la valeur (clé de :data:`DICO_VALEURS`).
        d_i: Date initiale (``dd/mm/YYYY``).
        d_f: Date finale (``dd/mm/YYYY``).

    Returns:
        Un ``DataFrame`` normalisé.
    """
    from selenium.webdriver.common.by import By

    driver = build_chrome_driver()
    try:
        driver.get(URL_VOLUMES)

        prefix = "HistoriqueNegociation1_HistValeur1_"
        elem = driver.find_element(By.ID, prefix + "RBSearchDate")
        date_ini = driver.find_element(By.ID, prefix + "DateTimeControl1_TBCalendar")
        date_fini = driver.find_element(By.ID, prefix + "DateTimeControl2_TBCalendar")
        download = driver.find_element(By.ID, prefix + "LinkButton1")
        listbox = driver.find_element(By.ID, prefix + "DDValeur")
        valid = driver.find_element(By.ID, prefix + "ImageButton1")

        driver.execute_script("arguments[0].click()", elem)
        driver.execute_script(f"arguments[0].value ='{act_mad}'", listbox)
        driver.execute_script(f"arguments[0].value='{d_i}'", date_ini)
        driver.execute_script(f"arguments[0].value='{d_f}'", date_fini)
        driver.execute_script("arguments[0].click()", valid)
        driver.execute_script("arguments[0].click()", download)

        _wait_for_download()
        return extract_volume_content(act_mad)
    finally:
        driver.quit()


def _wait_for_download(timeout: int = 30) -> None:
    """Attend l'apparition du fichier téléchargé (poll léger)."""
    import time

    download_dir = ensure_download_dir()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (download_dir / XML_FILE).exists():
            return
        time.sleep(0.5)
    logger.warning("Fichier %s introuvable après %ss.", XML_FILE, timeout)


def _import_volume_range(debut, fin) -> None:
    """Extrait et insère les volumes de toutes les valeurs sur une plage."""
    for code in DICO_VALEURS:
        try:
            frame = extract_volume(code, str(debut), str(fin))
            if frame.empty:
                logger.info("Valeur %s : aucune donnée du %s au %s.", code, debut, fin)
                continue
            frame.to_sql("masi_volume", con=get_engine(), if_exists="append", index=False)
            logger.info("Valeur %s importée du %s au %s.", code.strip(), debut, fin)
        except Exception:
            logger.exception("Échec de l'import de la valeur %s.", code.strip())


def extract_implement_volume(d_i: str, d_f: str) -> None:
    """Importe (de façon incrémentale) les volumes MASI entre deux dates.

    Args:
        d_i: Date initiale au format ``dd/mm/YYYY``.
        d_f: Date finale au format ``dd/mm/YYYY``.

    Example:
        >>> extract_implement_volume("12/06/2021", "08/07/2021")
    """
    run_incremental_import(
        model=MasiVolume,
        dedup_columns=[
            "seance",
            "name",
            "cours_cloture",
            "cours_ajuste",
            "evolution",
            "quantite_echange",
            "volume",
        ],
        d_i=d_i,
        d_f=d_f,
        import_range=_import_volume_range,
        list_dates_provider=get_days_imports_base,
        label="masi_volume",
    )
