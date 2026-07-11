"""Fonctions de parsing et de normalisation des données scrappées.

Ce module remplace l'ancien ``p_factory`` (absent du dépôt) référencé par les
scripts d'origine. Il regroupe les conversions récurrentes : chaînes -> dates,
chaînes localisées (virgule décimale) -> flottants, normalisation de texte.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

# Formats de date rencontrés sur les sources marocaines (Bourse de Casablanca,
# BKAM...). Essayés dans l'ordre.
_DATE_FORMATS = (
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d",
    "%d-%m-%Y",
)


def to_date(value) -> dt.date:
    """Convertit une valeur en ``datetime.date``.

    Accepte une ``date``/``datetime`` (retournée telle quelle) ou une chaîne
    dans l'un des formats connus.

    Args:
        value: date, datetime ou chaîne de caractères.

    Returns:
        L'objet ``datetime.date`` correspondant.

    Raises:
        ValueError: si la chaîne ne correspond à aucun format connu.
    """
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value

    text = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Format de date non reconnu : {value!r}")


def sanitize_float(value: str) -> float:
    """Convertit une chaîne numérique localisée en ``float``.

    Gère la virgule comme séparateur décimal et les espaces (y compris
    insécables) utilisés comme séparateurs de milliers.

    Args:
        value: chaîne représentant un nombre (ex. ``"1 234,56"``).

    Returns:
        La valeur flottante correspondante.
    """
    text = str(value).replace(" ", "").replace(" ", "").replace(",", ".")
    return float(text)


def upper_string(value: Optional[str]) -> Optional[str]:
    """Normalise une chaîne : espaces rognés et majuscules.

    Args:
        value: chaîne à normaliser (ou ``None``).

    Returns:
        La chaîne normalisée, ou ``None`` si l'entrée est ``None``.
    """
    if value is None:
        return None
    return str(value).strip().upper()
