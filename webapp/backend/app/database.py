"""Configuration SQLAlchemy du backend web.

Base et session dédiées aux tables applicatives (utilisateurs, plans,
abonnements, paiements). Les données de marché restent servies par le package
``kanyon`` ; ces tables-ci cohabitent dans la même base si l'on pointe la même
URL, mais gardent leur propre ``Base`` déclarative.
"""
from __future__ import annotations

from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

settings = get_settings()

# ``check_same_thread`` uniquement pertinent pour SQLite.
_connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}

engine = create_engine(settings.database_url, future=True, pool_pre_ping=True, connect_args=_connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


class Base(DeclarativeBase):
    """Base déclarative des modèles applicatifs du backend web."""


def get_db() -> Iterator[Session]:
    """Dépendance FastAPI fournissant une session par requête."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Crée les tables applicatives et amorce les plans par défaut."""
    from app import models  # noqa: F401  (enregistre les modèles)
    from app.seed import seed_plans

    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        seed_plans(db)
