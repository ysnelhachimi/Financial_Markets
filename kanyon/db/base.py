"""Moteur SQLAlchemy et gestion des sessions.

Un unique moteur est construit à partir de :data:`kanyon.config.DATABASE_URL`,
remplaçant les anciens ``engine_postgres`` / ``engine_sqlite`` / ``engine_lite``
codés en dur et incohérents entre les modules.

Le moteur est créé **paresseusement** (à la première utilisation) : importer le
package ne nécessite donc pas d'avoir installé le pilote de base de données
(``psycopg2``...), ce qui permet d'utiliser les modules ``helpers`` ou
``analytics`` sans dépendance à une base.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from kanyon.config import DATABASE_URL

# Base déclarative unique partagée par tous les modèles.
Base = declarative_base()

_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None


def get_engine() -> Engine:
    """Retourne le moteur SQLAlchemy, en le créant à la première demande.

    ``future=True`` active l'API SQLAlchemy 2.0 ; ``pool_pre_ping`` évite les
    connexions mortes lorsque la base ferme les connexions inactives.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
    return _engine


def _get_session_factory() -> sessionmaker:
    """Retourne la fabrique de sessions, initialisée paresseusement."""
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(), future=True, expire_on_commit=False
        )
    return _session_factory


def get_session() -> Session:
    """Retourne une nouvelle session (à fermer par l'appelant)."""
    return _get_session_factory()()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Fournit une session transactionnelle sous forme de gestionnaire de contexte.

    Valide (``commit``) en sortie normale, annule (``rollback``) en cas
    d'exception, et ferme la session dans tous les cas.

    Example:
        >>> with session_scope() as session:
        ...     session.add(obj)
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """Crée toutes les tables déclarées sur le moteur courant.

    Importe :mod:`kanyon.db.models` pour garantir que tous les modèles sont
    enregistrés sur ``Base.metadata`` avant la création.
    """
    from kanyon.db import models  # noqa: F401  (enregistre les modèles)

    Base.metadata.create_all(bind=get_engine())
