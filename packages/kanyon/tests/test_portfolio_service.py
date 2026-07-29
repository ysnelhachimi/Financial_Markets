"""Test du service portefeuille sur données réelles (univers MASI en base)."""
import datetime as dt

import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from kanyon.db.base import Base
from kanyon.db import models  # noqa: F401  (enregistre les modèles)
from kanyon.db.models import MasiVolume
from kanyon.portfolio.service import backtest_equities, optimize_equities


@pytest.fixture()
def session(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'mkt.db'}", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, future=True)
    sess = Session()

    rng = np.random.default_rng(1)
    dates = []
    d = dt.date(2021, 4, 6)
    while len(dates) < 40:
        if d.weekday() < 5:
            dates.append(d)
        d += dt.timedelta(days=1)
    for name, start in [("IAM", 130.0), ("ATW", 470.0), ("BCP", 260.0)]:
        price = start
        for seance in dates:
            price *= 1 + rng.normal(0.0004, 0.012)
            sess.add(MasiVolume(seance=seance, name=name, cours_cloture=round(price, 2)))
    sess.commit()
    try:
        yield sess
    finally:
        sess.close()
        engine.dispose()


def test_optimize_equities_sur_donnees_reelles(session):
    out = optimize_equities("2021-04-06", "2021-07-01", objective="max_sharpe", session=session)
    assert set(out["tickers"]) == {"IAM", "ATW", "BCP"}
    assert out["observations"] == 40
    assert abs(sum(out["result"]["weights"].values()) - 1.0) < 1e-3


def test_backtest_equities_equipondere(session):
    summary = backtest_equities("2021-04-06", "2021-07-01", session=session)
    assert "total_return" in summary and "max_drawdown" in summary


def test_optimize_equities_sans_donnees(session):
    with pytest.raises(ValueError):
        optimize_equities("2010-01-01", "2010-02-01", session=session)
