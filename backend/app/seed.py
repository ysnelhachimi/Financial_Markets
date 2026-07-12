"""Amorçage des plans par défaut (idempotent)."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Plan

# Définition des plans (prix mensuels en centimes de MAD).
DEFAULT_PLANS = [
    {
        "code": "free",
        "name": "Gratuit",
        "description": "Accès limité aux indices principaux, 50 requêtes/jour.",
        "price_cents": 0,
        "currency": "MAD",
        "daily_quota": 50,
    },
    {
        "code": "premium",
        "name": "Premium",
        "description": "Accès complet aux données de marché, 5 000 requêtes/jour.",
        "price_cents": 19900,  # 199,00 MAD / mois
        "currency": "MAD",
        "daily_quota": 5000,
    },
    {
        "code": "pro",
        "name": "Pro",
        "description": "Accès illimité + exports, pour usage professionnel.",
        "price_cents": 49900,  # 499,00 MAD / mois
        "currency": "MAD",
        "daily_quota": None,
    },
]


def seed_plans(db: Session) -> None:
    """Insère les plans par défaut absents de la base."""
    for data in DEFAULT_PLANS:
        exists = db.scalar(select(Plan).where(Plan.code == data["code"]))
        if exists is None:
            db.add(Plan(**data))
    db.commit()
