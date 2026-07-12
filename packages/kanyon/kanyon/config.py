"""Configuration centralisée du projet.

Toutes les valeurs sensibles ou dépendantes de l'environnement (URL de base de
données, répertoires de téléchargement) sont lues depuis des variables
d'environnement, avec des valeurs par défaut raisonnables. Aucun secret n'est
codé en dur dans le dépôt.

Variables reconnues :
    KANYON_DATABASE_URL  URL SQLAlchemy de la base (défaut : PostgreSQL local).
    KANYON_DOWNLOAD_DIR  Répertoire de téléchargement des fichiers scrappés.
    KANYON_HEADLESS      "1"/"0" pour (dé)activer le mode headless de Chrome.
"""
from __future__ import annotations

import os
from pathlib import Path

# Racine du projet (kanyon/ -> parent).
BASE_DIR = Path(__file__).resolve().parent.parent

# URL de connexion à la base. Par défaut PostgreSQL local ; surchargeable via
# la variable d'environnement pour cibler une autre instance (ou SQLite).
#
# Exemples :
#   postgresql+psycopg2://user:password@127.0.0.1:5432/db_kanyon
#   sqlite:///./data/kanyon.db
DATABASE_URL = os.environ.get(
    "KANYON_DATABASE_URL",
    "postgresql+psycopg2://localhost:5432/db_kanyon",
)

# Répertoire où les extractions déposent leurs fichiers temporaires.
DOWNLOAD_DIR = Path(
    os.environ.get("KANYON_DOWNLOAD_DIR", str(BASE_DIR / "data" / "masi_files"))
)

# Mode headless pour Selenium/Chrome (True par défaut).
HEADLESS = os.environ.get("KANYON_HEADLESS", "1") not in {"0", "false", "False"}


def ensure_download_dir() -> Path:
    """Crée le répertoire de téléchargement s'il n'existe pas et le retourne."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    return DOWNLOAD_DIR
