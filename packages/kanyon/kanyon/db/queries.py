"""Requêtes de lecture renvoyant des ``pandas.DataFrame``.

Chaque fonction accepte une ``session`` SQLAlchemy optionnelle (une session
éphémère est créée puis fermée si aucune n'est fournie) et retourne un
``DataFrame`` prêt à manipuler. Cela remplace l'ancienne API incohérente qui
retournait tantôt un objet ``Query``, tantôt ``query.all()``, tantôt un
``DataFrame``, tout en s'appuyant sur une session globale implicite.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

import pandas as pd
from sqlalchemy import asc, desc
from sqlalchemy.orm import Session

from kanyon.db.base import get_session
from kanyon.db.models import (
    Asfim,
    BkamAdjudicationPrimaire,
    BkamCourbe,
    BkamCourbeTenors,
    BkamEchange,
    BkamMonia,
    BkamTMP,
    BkamTenorsPrimaire,
    BkamTxDirecteur,
    BkamTxDirecteurConseil,
    BkamTxInflation,
    HcpIPC2017,
    LexActions,
    LexIndiceAct,
    MasiComposition,
    MasiIndices,
    MasiVolume,
    Mcl,
    MefPlacementsTresor,
)


@contextmanager
def _resolve_session(session: Optional[Session]) -> Iterator[Session]:
    """Fournit une session, en créant/fermant une session éphémère si besoin."""
    if session is not None:
        yield session
        return
    own = get_session()
    try:
        yield own
    finally:
        own.close()


def _read(query) -> pd.DataFrame:
    """Exécute une requête SQLAlchemy et renvoie un ``DataFrame``."""
    return pd.read_sql(query.statement, query.session.bind)


# ---------------------------------------------------------------------------
# Actions / indice MASI
# ---------------------------------------------------------------------------
def query_cours_valeurs_cm(nom_valeur, date_deb, date_fin, session=None) -> pd.DataFrame:
    """Cours d'une valeur (par libellé) entre deux dates, via la composition."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                MasiComposition.seance,
                MasiComposition.libelle,
                MasiComposition.cours,
            )
            .filter(
                MasiComposition.libelle == nom_valeur,
                MasiComposition.seance >= date_deb,
                MasiComposition.seance <= date_fin,
            )
            .order_by(MasiComposition.seance)
        )
        return _read(query)


def query_masi_composition(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Composition de l'indice MASI enrichie du lexique des actions."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                MasiComposition.seance,
                LexActions.secteur,
                LexActions.ticker,
                MasiComposition.libelle,
                LexActions.profil,
                MasiComposition.nombre_de_titre,
                MasiComposition.cours,
                MasiComposition.facteur_flottant,
                MasiComposition.capi_flottante,
                MasiComposition.poids,
            )
            .filter(
                LexActions.description == MasiComposition.libelle,
                MasiComposition.seance >= date_initiale,
                MasiComposition.seance <= date_finale,
            )
            .order_by(MasiComposition.seance)
        )
        return _read(query)


def query_masi_indice(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Séries d'indices MASI enrichies du libellé (lexique)."""
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    with _resolve_session(session) as s:
        query = (
            s.query(
                MasiIndices.seance,
                MasiIndices.name,
                LexIndiceAct.libelle,
                MasiIndices.instrument,
            )
            .filter(
                MasiIndices.name == LexIndiceAct.code,
                MasiIndices.seance >= date_initiale,
                MasiIndices.seance <= date_finale,
            )
            .order_by(asc(MasiIndices.seance))
        )
        return _read(query)


def query_masi_indice_name(name, date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Série d'un indice MASI donné (par ``name``) entre deux dates."""
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    with _resolve_session(session) as s:
        query = (
            s.query(
                MasiIndices.seance,
                MasiIndices.name,
                MasiIndices.instrument,
            )
            .filter(
                MasiIndices.name == LexIndiceAct.code,
                MasiIndices.name == name,
                MasiIndices.seance >= date_initiale,
                MasiIndices.seance <= date_finale,
            )
            .order_by(asc(MasiIndices.seance))
        )
        return _read(query)


def query_masi_volume(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Volumes échangés par valeur, enrichis du lexique des actions."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                MasiVolume.seance,
                LexActions.ticker,
                MasiVolume.name,
                LexActions.secteur,
                LexActions.cyclicite,
                LexActions.profil,
                MasiVolume.cours_cloture,
                MasiVolume.cours_ajuste,
                MasiVolume.quantite_echange,
                MasiVolume.volume,
            )
            .filter(
                MasiVolume.name == LexActions.description,
                MasiVolume.seance >= date_initiale,
                MasiVolume.seance <= date_finale,
            )
            .order_by(asc(MasiVolume.seance))
        )
        return _read(query)


# ---------------------------------------------------------------------------
# Marché obligataire : primaire
# ---------------------------------------------------------------------------
def query_bkam_adju_primaire(session=None) -> pd.DataFrame:
    """Adjudications primaires BKAM."""
    with _resolve_session(session) as s:
        query = s.query(
            BkamAdjudicationPrimaire.caracteristique,
            BkamAdjudicationPrimaire.date_reglement,
            BkamAdjudicationPrimaire.maturite,
            BkamAdjudicationPrimaire.mnt_adjuge,
            BkamAdjudicationPrimaire.mnt_propose,
            BkamAdjudicationPrimaire.taux_prix_max,
            BkamAdjudicationPrimaire.taux_prix_min,
            BkamAdjudicationPrimaire.taux_prix_moyen_pondere,
            BkamAdjudicationPrimaire.taux_prix_limite,
        )
        return _read(query)


def query_bkam_echange(session=None) -> pd.DataFrame:
    """Opérations d'échange (rachat / remplacement) BKAM."""
    with _resolve_session(session) as s:
        query = s.query(
            BkamEchange.date_reglement,
            BkamEchange.maturites_rachat,
            BkamEchange.date_echeance_rachat,
            BkamEchange.taux_nominal_rachat,
            BkamEchange.montant_propose_rachat,
            BkamEchange.montant_retenu_rachat,
            BkamEchange.maturites_remplacement,
            BkamEchange.date_echeance_remplacement,
            BkamEchange.taux_nominal_remplacement,
            BkamEchange.prix_min_remplacement,
            BkamEchange.prix_max_remplacement,
            BkamEchange.montant_retenu_remplacement,
            BkamEchange.pmp_remplacement,
        )
        return _read(query)


# ---------------------------------------------------------------------------
# Marché obligataire : secondaire (titres Maroclear + courbes)
# ---------------------------------------------------------------------------
def query_titre_all(session=None) -> pd.DataFrame:
    """Référentiel complet des titres Maroclear."""
    with _resolve_session(session) as s:
        query = s.query(
            Mcl.code_isin,
            Mcl.famille_instrument,
            Mcl.categorie_instrument,
            Mcl.libelle_court,
            Mcl.description,
            Mcl.isin_emetteur,
            Mcl.capital_emis,
            Mcl.qt_emise,
            Mcl.nominal,
            Mcl.taux_facial,
            Mcl.type_coupon,
            Mcl.date_emission,
            Mcl.date_jouissance,
            Mcl.date_echeance,
            Mcl.garantie,
            Mcl.nominal_actuel,
            Mcl.periodicite,
            Mcl.type_remboursement,
            Mcl.periodicite_amortissement,
            Mcl.forme_detention,
            Mcl.cote,
            Mcl.mnemonique,
            Mcl.isin_centralisateur,
            Mcl.nom_emetteur,
            Mcl.code_enregistrement,
            Mcl.dates_coupons,
            Mcl.statut_instrument,
        )
        return _read(query)


def query_mcl(session=None) -> pd.DataFrame:
    """Sous-ensemble courant des caractéristiques des titres Maroclear."""
    with _resolve_session(session) as s:
        query = s.query(
            Mcl.code_isin,
            Mcl.nom_emetteur,
            Mcl.famille_instrument,
            Mcl.capital_emis,
            Mcl.qt_emise,
            Mcl.taux_facial,
            Mcl.date_emission,
            Mcl.date_jouissance,
            Mcl.date_echeance,
            Mcl.nominal,
            Mcl.nominal_actuel,
            Mcl.garantie,
            Mcl.dates_coupons,
            Mcl.type_coupon,
            Mcl.periodicite,
            Mcl.type_remboursement,
            Mcl.periodicite_amortissement,
            Mcl.cote,
        )
        return _read(query)


def query_mcl_titre(datem, session=None) -> pd.DataFrame:
    """Titres Maroclear encore vivants à ``datem`` (échéance >= datem)."""
    with _resolve_session(session) as s:
        query = s.query(
            Mcl.code_isin,
            Mcl.nom_emetteur,
            Mcl.famille_instrument,
            Mcl.capital_emis,
            Mcl.qt_emise,
            Mcl.taux_facial,
            Mcl.date_emission,
            Mcl.date_jouissance,
            Mcl.date_echeance,
            Mcl.nominal,
            Mcl.nominal_actuel,
            Mcl.garantie,
            Mcl.dates_coupons,
            Mcl.type_coupon,
            Mcl.periodicite,
            Mcl.type_remboursement,
            Mcl.periodicite_amortissement,
        ).filter(Mcl.date_echeance >= datem)
        return _read(query)


def query_mcl_titre_categorie(datem, categorie, session=None) -> pd.DataFrame:
    """Titres Maroclear vivants d'une catégorie donnée à ``datem``."""
    with _resolve_session(session) as s:
        query = s.query(
            Mcl.code_isin,
            Mcl.nom_emetteur,
            Mcl.categorie_instrument,
            Mcl.famille_instrument,
            Mcl.capital_emis,
            Mcl.qt_emise,
            Mcl.taux_facial,
            Mcl.date_emission,
            Mcl.date_jouissance,
            Mcl.date_echeance,
            Mcl.nominal,
            Mcl.nominal_actuel,
            Mcl.garantie,
            Mcl.dates_coupons,
            Mcl.type_coupon,
            Mcl.periodicite,
            Mcl.type_remboursement,
            Mcl.periodicite_amortissement,
        ).filter(
            Mcl.date_echeance >= datem,
            Mcl.categorie_instrument == categorie,
        )
        return _read(query)


def query_tenors(datem, session=None) -> pd.DataFrame:
    """Courbe des tenors secondaires pour une date de marché donnée."""
    datem = pd.to_datetime(datem)
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamCourbeTenors.date_marche,
                BkamCourbeTenors.tenors_y,
                BkamCourbeTenors.tenors_d,
                BkamCourbeTenors.taux,
            )
            .filter(BkamCourbeTenors.date_marche == datem)
            .order_by(asc(BkamCourbeTenors.date_marche), asc(BkamCourbeTenors.tenors_d))
        )
        return _read(query)


def query_tenors_data(d_i, d_f, session=None) -> pd.DataFrame:
    """Courbe des tenors secondaires entre deux dates."""
    d_i = pd.to_datetime(d_i)
    d_f = pd.to_datetime(d_f)
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamCourbeTenors.date_marche,
                BkamCourbeTenors.tenors_y,
                BkamCourbeTenors.tenors_d,
                BkamCourbeTenors.taux,
            )
            .filter(
                BkamCourbeTenors.date_marche >= d_i,
                BkamCourbeTenors.date_marche <= d_f,
            )
            .order_by(asc(BkamCourbeTenors.date_marche), asc(BkamCourbeTenors.tenors_d))
        )
        return _read(query)


def query_tenors_range(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Courbe des tenors triée par tenor puis par date, entre deux dates."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamCourbeTenors.date_marche,
                BkamCourbeTenors.tenors_y,
                BkamCourbeTenors.tenors_d,
                BkamCourbeTenors.taux,
            )
            .filter(
                BkamCourbeTenors.date_marche >= date_initiale,
                BkamCourbeTenors.date_marche <= date_finale,
            )
            .order_by(asc(BkamCourbeTenors.tenors_d), asc(BkamCourbeTenors.date_marche))
        )
        return _read(query)


def query_courbe(datem, session=None) -> pd.DataFrame:
    """Courbe des taux secondaires pour une date de marché donnée."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamCourbe.date_marche,
                BkamCourbe.date_echeance,
                BkamCourbe.transactions,
                BkamCourbe.taux,
                BkamCourbe.date_valeur,
                BkamCourbe.date_transaction,
            )
            .filter(BkamCourbe.date_marche == datem)
            .order_by(asc(BkamCourbe.date_echeance))
        )
        return _read(query)


def query_courbe_range(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Courbe des taux secondaires entre deux dates."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamCourbe.date_marche,
                BkamCourbe.date_echeance,
                BkamCourbe.transactions,
                BkamCourbe.taux,
                BkamCourbe.date_valeur,
                BkamCourbe.date_transaction,
            )
            .filter(
                BkamCourbe.date_marche >= date_initiale,
                BkamCourbe.date_marche <= date_finale,
            )
            .order_by(asc(BkamCourbe.date_marche))
        )
        return _read(query)


# ---------------------------------------------------------------------------
# Macro : taux directeur et inflation
# ---------------------------------------------------------------------------
def query_taux_directeur_conseil(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Historique du taux directeur (décisions du Conseil) entre deux dates."""
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamTxDirecteurConseil.date,
                BkamTxDirecteurConseil.taux_directeur,
                BkamTxDirecteurConseil.ratio_reserve_obligatoire,
                BkamTxDirecteurConseil.remuneration_reserve,
            )
            .filter(
                BkamTxDirecteurConseil.date >= date_initiale,
                BkamTxDirecteurConseil.date <= date_finale,
            )
            .order_by(asc(BkamTxDirecteurConseil.date))
        )
        return _read(query)


def query_taux_directeur(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Série quotidienne du taux directeur entre deux dates."""
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamTxDirecteur.date,
                BkamTxDirecteur.taux_directeur,
                BkamTxDirecteur.ratio_reserve_obligatoire,
                BkamTxDirecteur.remuneration_reserve,
            )
            .filter(
                BkamTxDirecteur.date >= date_initiale,
                BkamTxDirecteur.date <= date_finale,
            )
            .order_by(asc(BkamTxDirecteur.date))
        )
        return _read(query)


def query_taux_inflation_bkam_mga(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Taux d'inflation BKAM (moyenne glissante annuelle) entre deux dates."""
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamTxInflation.date,
                BkamTxInflation.inflation_rate_mga,
                BkamTxInflation.inflation_rate_sj_mga,
            )
            .filter(
                BkamTxInflation.date >= date_initiale,
                BkamTxInflation.date <= date_finale,
            )
            .order_by(asc(BkamTxInflation.date))
        )
        return _read(query)


def query_taux_inflation_hcp_all(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Indice des prix à la consommation HCP (toutes rubriques) entre deux dates."""
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    with _resolve_session(session) as s:
        query = (
            s.query(
                HcpIPC2017.date,
                HcpIPC2017.produits_alimentaires_boissons,
                HcpIPC2017.boissons_alcoolisees_tabac,
                HcpIPC2017.textiles,
                HcpIPC2017.logement_eau_energies,
                HcpIPC2017.meubles_articles_menage_entretien,
                HcpIPC2017.sante,
                HcpIPC2017.transports,
                HcpIPC2017.communications,
                HcpIPC2017.loisirs_culture,
                HcpIPC2017.enseignement,
                HcpIPC2017.restaurants_hotels,
                HcpIPC2017.biens_services_divers,
                HcpIPC2017.alimentation,
                HcpIPC2017.produits_non_alimentaires,
                HcpIPC2017.ipc,
            )
            .filter(
                HcpIPC2017.date >= date_initiale,
                HcpIPC2017.date <= date_finale,
            )
            .order_by(asc(HcpIPC2017.date))
        )
        return _read(query)


def query_taux_inflation_hcp_cat(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """IPC HCP agrégé (alimentation / non-alimentaire / IPC) entre deux dates."""
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    with _resolve_session(session) as s:
        query = (
            s.query(
                HcpIPC2017.date,
                HcpIPC2017.alimentation,
                HcpIPC2017.produits_non_alimentaires,
                HcpIPC2017.ipc,
            )
            .filter(
                HcpIPC2017.date >= date_initiale,
                HcpIPC2017.date <= date_finale,
            )
            .order_by(asc(HcpIPC2017.date))
        )
        return _read(query)


# ---------------------------------------------------------------------------
# Marché monétaire : MONIA, TMP, placements
# ---------------------------------------------------------------------------
def query_monia(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Taux MONIA et volumes associés entre deux dates."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamMonia.date_marche,
                BkamMonia.taux_monia,
                BkamMonia.volume,
            )
            .filter(
                BkamMonia.date_marche >= date_initiale,
                BkamMonia.date_marche <= date_finale,
            )
            .order_by(asc(BkamMonia.date_marche))
        )
        return _read(query)


def query_tmp(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Taux moyen pondéré interbancaire (TMP) entre deux dates."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamTMP.date_marche,
                BkamTMP.taux_ref,
                BkamTMP.volume,
                BkamTMP.encours,
            )
            .filter(
                BkamTMP.date_marche >= date_initiale,
                BkamTMP.date_marche <= date_finale,
            )
            .order_by(asc(BkamTMP.date_marche))
        )
        return _read(query)


def query_placements_tresor(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Placements du Trésor (par date d'échéance) entre deux dates."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                MefPlacementsTresor.date_pdf,
                MefPlacementsTresor.type_operation,
                MefPlacementsTresor.date_reglement,
                MefPlacementsTresor.date_echeance,
                MefPlacementsTresor.duree_placement,
                MefPlacementsTresor.montant_placement,
                MefPlacementsTresor.tmp_placement,
            )
            .filter(
                MefPlacementsTresor.date_echeance >= date_initiale,
                MefPlacementsTresor.date_echeance <= date_finale,
            )
            .order_by(asc(MefPlacementsTresor.date_echeance))
        )
        return _read(query)


def query_marche_monetaire(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Vue consolidée du marché monétaire (MONIA + TMP + taux directeur)."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamMonia.date_marche,
                BkamMonia.taux_monia,
                BkamMonia.volume.label("volume_monia"),
                BkamTMP.taux_ref.label("taux_interbancaire"),
                BkamTMP.volume.label("volume_tmp"),
                BkamTMP.encours.label("encours_tmp"),
                BkamTxDirecteur.taux_directeur,
                BkamTxDirecteur.ratio_reserve_obligatoire,
                BkamTxDirecteur.remuneration_reserve,
            )
            .filter(
                BkamMonia.date_marche == BkamTxDirecteur.date,
                BkamMonia.date_marche == BkamTMP.date_marche,
                BkamMonia.date_marche >= date_initiale,
                BkamMonia.date_marche <= date_finale,
            )
            .order_by(asc(BkamTxDirecteur.date))
        )
        return _read(query)


def query_tenors_primaire(date_initiale, date_finale, session=None) -> pd.DataFrame:
    """Courbe primaire par tenor (13s -> 30ans) entre deux dates."""
    with _resolve_session(session) as s:
        query = (
            s.query(
                BkamTenorsPrimaire.date_reglement,
                BkamTenorsPrimaire.t_13s,
                BkamTenorsPrimaire.t_26s,
                BkamTenorsPrimaire.t_52s,
                BkamTenorsPrimaire.t_2ans,
                BkamTenorsPrimaire.t_5ans,
                BkamTenorsPrimaire.t_10ans,
                BkamTenorsPrimaire.t_15ans,
                BkamTenorsPrimaire.t_20ans,
                BkamTenorsPrimaire.t_30ans,
            )
            .filter(
                BkamTenorsPrimaire.date_reglement >= date_initiale,
                BkamTenorsPrimaire.date_reglement <= date_finale,
            )
            .order_by(asc(BkamTenorsPrimaire.date_reglement))
        )
        return _read(query)


# ---------------------------------------------------------------------------
# ASFIM : classements de fonds
# ---------------------------------------------------------------------------
_ASFIM_RANKING_COLUMNS = (
    Asfim.date_marche,
    Asfim.code_isin,
    Asfim.code_mcl,
    Asfim.sdg,
    Asfim.denomination,
    Asfim.classification,
    Asfim.periodicite,
    Asfim.affectation,
    Asfim.souscripteurs,
    Asfim.nature_juridique,
    Asfim.actif_net,
    Asfim.valeur_liquidative,
    Asfim.perf_year_to_date,
    Asfim.perf_1s,
    Asfim.perf_1m,
    Asfim.perf_3m,
    Asfim.perf_6m,
    Asfim.perf_1a,
    Asfim.perf_2a,
)


def query_ranking(date_x, session=None) -> pd.DataFrame:
    """Classement des fonds ASFIM pour une date donnée."""
    with _resolve_session(session) as s:
        query = (
            s.query(*_ASFIM_RANKING_COLUMNS)
            .filter(Asfim.date_marche == date_x)
            .order_by(
                asc(Asfim.date_marche),
                asc(Asfim.sdg),
                asc(Asfim.denomination),
                asc(Asfim.actif_net),
            )
        )
        return _read(query)


def query_ranking_all(
    date_x,
    classification,
    periodicite,
    souscripteurs,
    actif_net_min,
    session=None,
) -> pd.DataFrame:
    """Classement des fonds ASFIM filtré et trié par performance YTD."""
    with _resolve_session(session) as s:
        query = (
            s.query(*_ASFIM_RANKING_COLUMNS)
            .filter(
                Asfim.date_marche == date_x,
                Asfim.classification == classification,
                Asfim.periodicite == periodicite,
                Asfim.souscripteurs == souscripteurs,
                Asfim.actif_net >= actif_net_min,
            )
            .order_by(desc(Asfim.perf_year_to_date))
        )
        return _read(query)


def query_ranking_actions_hebdo(date_x, session=None) -> pd.DataFrame:
    """Classement des fonds Actions hebdomadaires FGP (actif net >= 50 M MAD)."""
    return query_ranking_all(
        date_x,
        classification="Actions",
        periodicite="Hebdomadaire",
        souscripteurs="FGP",
        actif_net_min=50_000_000,
        session=session,
    )
