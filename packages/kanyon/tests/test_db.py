"""Tests d'intégration de la couche base de données (SQLite sur fichier temporaire).

On rebranche directement le moteur paresseux de :mod:`kanyon.db.base` sur une
base SQLite de fichier (partagée entre connexions, contrairement à
``:memory:``), sans recharger le module afin que les modèles restent
enregistrés sur la même ``Base``.
"""
import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture()
def session(tmp_path):
    """Fournit une session SQLite (fichier temporaire) avec le schéma créé."""
    from kanyon.db import base
    from kanyon.db import models  # noqa: F401  (enregistre les modèles sur Base)

    engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}", future=True)

    # Sauvegarde puis rebranchement des singletons paresseux.
    saved_engine, saved_factory = base._engine, base._session_factory
    base._engine = engine
    base._session_factory = sessionmaker(bind=engine, future=True, expire_on_commit=False)

    base.Base.metadata.create_all(bind=engine)
    sess = base.get_session()
    try:
        yield sess
    finally:
        sess.close()
        engine.dispose()
        base._engine, base._session_factory = saved_engine, saved_factory


def test_schema_cree_toutes_les_tables(session):
    from kanyon.db import base

    assert len(base.Base.metadata.tables) >= 20


def test_insert_et_query_masi_indice(session):
    from kanyon.db.models import LexIndiceAct, MasiIndices
    from kanyon.db.queries import query_masi_indice

    session.add(LexIndiceAct(code="MASI", libelle="MASI"))
    session.add(MasiIndices(seance=dt.date(2021, 6, 14), name="MASI", instrument=12000.5, variation="0.3"))
    session.add(MasiIndices(seance=dt.date(2021, 6, 15), name="MASI", instrument=12050.0, variation="0.4"))
    session.commit()

    df = query_masi_indice("2021-06-01", "2021-06-30", session=session)
    assert len(df) == 2
    assert list(df.columns) == ["seance", "name", "libelle", "instrument"]


def test_autoincrement_id(session):
    from kanyon.db.models import LexIndiceAct

    a = LexIndiceAct(code="A", libelle="A")
    b = LexIndiceAct(code="B", libelle="B")
    session.add_all([a, b])
    session.commit()
    assert a.id is not None and b.id is not None and a.id != b.id


def test_repr_ne_leve_pas(session):
    from kanyon.db.models import MasiIndices

    obj = MasiIndices(seance=dt.date(2021, 6, 14), name="MASI", instrument=1.0, variation="0.1")
    # Les anciens __repr__ renvoyaient un tuple : on vérifie qu'ils produisent
    # désormais bien une chaîne.
    assert isinstance(repr(obj), str)
