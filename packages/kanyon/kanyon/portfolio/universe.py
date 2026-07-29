"""Univers investissable à partir des données réelles en base.

Construit les entrées des modules de portefeuille depuis :mod:`kanyon.db` :

* **actions** : matrice de prix (séance × valeur) issue de ``masi_volume`` ;
  estimation des rendements attendus et de la covariance.
* **obligataire** : sensibilité (duration modifiée) par titre, dérivée du pricer
  et de la courbe BKAM.
"""
from __future__ import annotations

import datetime as dt
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from kanyon.db.base import get_session
from kanyon.db.models import MasiVolume, Mcl
from kanyon.helpers import to_date


def equity_prices(date_debut, date_fin, session=None, min_ratio: float = 0.6) -> pd.DataFrame:
    """Matrice de prix des actions (index = séance, colonnes = valeur).

    Args:
        date_debut: Début de période.
        date_fin: Fin de période.
        session: Session SQLAlchemy optionnelle.
        min_ratio: Fraction minimale d'observations non nulles pour retenir une valeur.

    Returns:
        Un DataFrame de cours de clôture, colonnes = noms de valeurs.
    """
    own = session is None
    session = session or get_session()
    try:
        query = (
            session.query(MasiVolume.seance, MasiVolume.name, MasiVolume.cours_cloture)
            .filter(MasiVolume.seance >= to_date(date_debut), MasiVolume.seance <= to_date(date_fin))
            .order_by(MasiVolume.seance)
        )
        frame = pd.read_sql(query.statement, query.session.bind)
    finally:
        if own:
            session.close()

    if frame.empty:
        return pd.DataFrame()

    prices = frame.pivot_table(index="seance", columns="name", values="cours_cloture", aggfunc="last")
    prices = prices.sort_index()
    # Retire les valeurs trop peu observées, complète les trous.
    threshold = int(len(prices) * min_ratio)
    prices = prices.dropna(axis=1, thresh=threshold).ffill().dropna(axis=1)
    return prices


def estimate_inputs(
    prices: pd.DataFrame, periods_per_year: int = 252
) -> Tuple[List[str], np.ndarray, np.ndarray]:
    """Estime rendements attendus (annualisés) et covariance depuis une matrice de prix.

    Args:
        prices: Matrice de prix (séance × valeur).
        periods_per_year: Périodes par an pour l'annualisation.

    Returns:
        ``(tickers, mean, cov)`` prêts pour :func:`kanyon.portfolio.construction.optimize`.

    Raises:
        ValueError: si moins de deux valeurs exploitables.
    """
    if prices.shape[1] < 2:
        raise ValueError("Univers insuffisant : au moins deux valeurs sont requises.")
    returns = prices.pct_change().dropna(how="all").fillna(0.0)
    mean = (returns.mean() * periods_per_year).to_numpy()
    cov = (returns.cov() * periods_per_year).to_numpy()
    return list(prices.columns), mean, cov


def bond_sensitivities(date_valeur, date_courbe, session=None, bump: float = 1e-4) -> Dict[str, float]:
    """Sensibilité (duration modifiée) par titre obligataire vivant.

    Calculée par choc de taux : ``-(P(y+dy) - P(y)) / (P(y) · dy)``, en réévaluant
    chaque titre via le pricer sur la courbe BKAM.

    Args:
        date_valeur: Date de valorisation.
        date_courbe: Date de la courbe des taux.
        session: Session SQLAlchemy optionnelle.
        bump: Choc de taux (défaut 1 pb).

    Returns:
        ``{code_isin: sensibilité}`` (vide si aucune donnée).
    """
    from kanyon.pricer.bonds import price_fixed_bond
    from kanyon.pricer.service import rate_for_maturity

    own = session is None
    session = session or get_session()
    dv = to_date(date_valeur)
    result: Dict[str, float] = {}
    try:
        titles = session.query(Mcl).filter(Mcl.date_echeance >= dv).all()
        for t in titles:
            try:
                de, dj, dm = to_date(str(t.date_emission)), to_date(str(t.date_jouissance)), to_date(str(t.date_echeance))
                tf = float(t.taux_facial) / 100.0
                nominal = float(t.nominal)
                mat_res = (dm - dv).days
                base_rate = rate_for_maturity(date_courbe, mat_res, session)
                p0 = price_fixed_bond(date_valeur=dv, date_emission=de, date_jouissance=dj,
                                      date_echeance=dm, taux_facial=tf, taux_courbe=base_rate, nominal=nominal)["price"]
                p1 = price_fixed_bond(date_valeur=dv, date_emission=de, date_jouissance=dj,
                                      date_echeance=dm, taux_facial=tf, taux_courbe=base_rate + bump, nominal=nominal)["price"]
                if p0:
                    result[str(t.code_isin)] = -(p1 - p0) / (p0 * bump)
            except Exception:
                continue
    finally:
        if own:
            session.close()
    return result
