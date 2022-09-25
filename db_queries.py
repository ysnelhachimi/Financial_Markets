import pandas as pd

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_loading import dbsession

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_table import *

from pyramido.pyrkanyon.pyrkanyon.models.helpers.p_factory import format


# ----------------------------------------------------------------
#   Functions returning masi series
# ----------------------------------------------------------------


dbs = dbsession()


def query_cours_valeurs_cm(nom_valeur, date_deb, date_fin, session):
    quer = dbs.query(MasiComposition.seance,
        MasiComposition.libelle,
        MasiComposition.cours
        ).filter(
        MasiComposition.libelle == nom_valeur,
        MasiComposition.seance >= date_deb,
        MasiComposition.seance <= date_fin
        ).order_by(
        MasiComposition.seance
        )

    dquer = quer.all()
    #rv = quer.all()
    #return rv
    return quer

def query_masi_composition(date_initiale, date_finale, session):
    quer = dbs.query(
        MasiComposition.seance,
        LexActions.secteur,
        LexActions.ticker,
        MasiComposition.libelle,
        LexActions.profil,
        MasiComposition.nombre_de_titre,
        MasiComposition.cours,
        MasiComposition.facteur_flottant,
        MasiComposition.capi_flottante,
        MasiComposition.poids
    ).filter(
        LexActions.description == MasiComposition.libelle,
        MasiComposition.seance >= date_initiale,
        MasiComposition.seance <= date_finale
    ).order_by(MasiComposition.seance)
    dquer = quer.all()
    #rv = quer.all()
    #return rv
    return quer

def query_masi_indice(date_initiale, date_finale, session):
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    quer = dbs.query(
        MasiIndices.seance,
        MasiIndices.name,
        LexIndiceAct.libelle,
        MasiIndices.instrument
        ).filter(
        MasiIndices.name == LexIndiceAct.code,
        MasiIndices.seance >= date_initiale,
        MasiIndices.seance <= date_finale
        ).order_by(asc(MasiIndices.seance))
    dquer = quer.all()
    return quer

def query_masi_indice_name(name, date_initiale, date_finale, session):
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    quer = dbs.query(
        MasiIndices.seance,
        MasiIndices.name,
        MasiIndices.instrument
        ).filter(
        MasiIndices.name == LexIndiceAct.code,
        MasiIndices.name == name,
        MasiIndices.seance >= date_initiale,
        MasiIndices.seance <= date_finale
        ).order_by(asc(MasiIndices.seance))

    dquer = quer.all()
    return quer

def query_masi_volume(date_initiale, date_finale, session):
    """
    comment:
    fonction qui retourne le volume par valeur
    """
    quer = dbs.query(
        MasiVolume.seance,
        LexActions.ticker,
        MasiVolume.name,
        LexActions.secteur,
        LexActions.cyclicite,
        LexActions.profil,
        MasiVolume.cours_cloture,
        MasiVolume.cours_ajuste,
        MasiVolume.quantite_echange,
        MasiVolume.volume
        ).filter(
        MasiVolume.name == LexActions.description,
        MasiVolume.seance >= date_initiale,
        MasiVolume.seance <= date_finale).order_by(asc(MasiVolume.seance))
        #rv = pd.DataFrame(quer.all(), columns = quer[0].keys())
    dquer = quer.all()
    return quer

# ----------------------------------------------------------------
#   Functions marché monetaire


# ----------------------------------------------------------------
#   Functions marché oblig
# ----------------------------------------------------------------
#   Functions marché primaire
#
#  1 Function adjudication
# ------------------------
def query_bkam_adju_primaire(session):
    quer = dbs.query(
        BkamAdjudicationPrimaire.caracteristique,
        BkamAdjudicationPrimaire.dateReglement,
        BkamAdjudicationPrimaire.maturite,
        BkamAdjudicationPrimaire.mntAdjuge,
        BkamAdjudicationPrimaire.mntPropose,
        BkamAdjudicationPrimaire.tauxPrixMax,
        BkamAdjudicationPrimaire.tauxPrixMin,
        BkamAdjudicationPrimaire.tauxPrixMoyenPondere,
        BkamAdjudicationPrimaire.tauxPrixlimite
        )
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

#  2 Function echange
# ------------------------
def query_bkam_echange(session):
    quer = dbs.query(
        bkam_echange.date_reglement,
        bkam_echange.maturites_rachat,
        bkam_echange.date_echeance_rachat,
        bkam_echange.Taux_nominal_rachat,
        bkam_echange.Montant_propose_rachat ,
        bkam_echange.Montant_retenu_rachat,
        bkam_echange.maturites_remplacement,
        bkam_echange.date_echeance_remplacement,
        bkam_echange.Taux_nominal_remplacement,
        bkam_echange.Prix_min_remplacement,
        bkam_echange.Prix_max_remplacement,
        bkam_echange.Montant_retenu_remplacement,
        bkam_echange.PMP_remplacement
        )

    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer


# ----------------------------------------------------------------
#   Functions marché secondaire
#
#  1 Function titre
# ------------------------
def query_titre_all(session):
    quer = dbs.query(Mcl.code_isin,
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
        Mcl.statut_instrument
        )

    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

def query_mcl(session):
    quer = dbs.query(
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
        Mcl.cote)
    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return quer

def query_mcl_titre(datem, session):
    """
    comment:
    """
    quer = dbs.query(
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
        Mcl.periodicite_amortissement
        ).filter(Mcl.date_echeance >= datem)
    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return quer

def query_mcl_titre_categorie(datem, categorie, session):
    """
    comment:
    """
    quer = dbs.query(
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
        Mcl.periodicite_amortissement
        ).filter(Mcl.date_echeance >= datem,
        Mcl.categorie_instrument == categorie)
    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return quer
#
#
#  2 Function courbe
# ------------------------

def query_tenors(datem, session):
    datem = pd.to_datetime(datem)
    quer = dbs.query(
        BkamCourbeTenors.date_marche,
        BkamCourbeTenors.tenors_y,
        BkamCourbeTenors.tenors_d,
        BkamCourbeTenors.taux
        ).filter(BkamCourbeTenors.date_marche == datem).order_by(asc(BkamCourbeTenors.date_marche)).order_by(asc(BkamCourbeTenors.tenors_d))
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

def query_tenors_data(d_i, d_f, session):
    d_i = pd.to_datetime(d_i)
    d_f = pd.to_datetime(d_f)
    quer = dbs.query(
        BkamCourbeTenors.date_marche,
        BkamCourbeTenors.tenors_y,
        BkamCourbeTenors.tenors_d,
        BkamCourbeTenors.taux
        ).filter(
        BkamCourbeTenors.date_marche >= d_i,
        BkamCourbeTenors.date_marche <= d_f
        ).order_by(asc(BkamCourbeTenors.date_marche)
        ).order_by(asc(BkamCourbeTenors.tenors_d))

    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return quer

def query_tenors_range(date_initiale, date_finale, session):

    quer = dbs.query(
        BkamCourbeTenors.date_marche,
        BkamCourbeTenors.tenors_y,
        BkamCourbeTenors.tenors_d,
        BkamCourbeTenors.taux
        ).filter(BkamCourbeTenors.date_marche >= date_initiale,
        BkamCourbeTenors.date_marche <= date_finale
        ).order_by(asc(BkamCourbeTenors.tenors_d)).order_by(asc(BkamCourbeTenors.date_marche))
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

def query_courbe(datem, session):
    #datem = pd.to_datetime(datem)
    quer = dbs.query(
        BkamCourbe.date_marche,
        BkamCourbe.date_echeance,
        BkamCourbe.transactions,
        BkamCourbe.taux,
        BkamCourbe.date_valeur,
        BkamCourbe.date_transaction
        ).filter(BkamCourbe.date_marche == datem).order_by(asc(BkamCourbe.date_echeance))
    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

def query_courbe_range(date_initiale, date_finale, session):
    """ yass
    pour importation de la BkamCourbe entre deux dates
    """
    quer = dbs.query(
        BkamCourbe.date_marche,
        BkamCourbe.date_echeance,
        BkamCourbe.transactions,
        BkamCourbe.taux,
        BkamCourbe.date_valeur,
        BkamCourbe.date_transaction
        ).filter(BkamCourbe.date_marche >= date_initiale,
        BkamCourbe.date_marche <= date_finale
        ).order_by(asc(BkamCourbe.date_marche))
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer


# ----------------------------------------------------------------
#   Function macro series
#
#  1 Function taux_directeur series
# ----------------------------------
def query_taux_directeur_conseil(date_initiale, date_finale, session):
    """ yass
    pour importation du taux directeur entre deux dates
    """
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    quer = dbs.query(
        BkamTxDirecteurConseil.date,
        BkamTxDirecteurConseil.taux_directeur,
        BkamTxDirecteurConseil.ratio_reserve_obligatoire,
        BkamTxDirecteurConseil.remuneration_reserve
        ).filter(BkamTxDirecteurConseil.date >= date_initiale,
        BkamTxDirecteurConseil.date <= date_finale
        ).order_by(asc(BkamTxDirecteurConseil.date))
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

def query_taux_directeur(date_initiale, date_finale, session):
    """ yass
    pour importation du taux directeur entre deux dates
    """
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    quer = dbs.query(
        BkamTxDirecteur.date,
        BkamTxDirecteur.taux_directeur,
        BkamTxDirecteur.ratio_reserve_obligatoire,
        BkamTxDirecteur.remuneration_reserve
        ).filter(BkamTxDirecteur.date >= date_initiale,
        BkamTxDirecteur.date <= date_finale
        ).order_by(asc(BkamTxDirecteur.date))
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

#  2 Function ipc series
# ------------------------

def query_taux_inflation_bkam_mga(date_initiale, date_finale, session):
    """ yass
    pour importation du taux directeur entre deux dates
    """
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    quer = dbs.query(
        BkamTxInflation.date,
        BkamTxInflation.inflation_rate_mga,
        BkamTxInflation.inflation_rate_sj_mga
        ).filter(BkamTxInflation.date >= date_initiale,
        BkamTxInflation.date <= date_finale
        ).order_by(asc(BkamTxInflation.date))
    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

def query_taux_inflation_hcp_all(date_initiale, date_finale, session):
    """ yass
    pour importation du taux directeur entre deux dates
    """
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    quer = dbs.query(
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
        HcpIPC2017.ipc
        ).filter(HcpIPC2017.date >= date_initiale,
        HcpIPC2017.date <= date_finale
        ).order_by(asc(HcpIPC2017.date))
    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer

def query_taux_inflation_hcp_cat(date_initiale, date_finale, session):
    """ yass
    pour importation du taux directeur entre deux dates
    """
    date_initiale = pd.to_datetime(date_initiale)
    date_finale = pd.to_datetime(date_finale)
    quer = dbs.query(
        HcpIPC2017.date,
        HcpIPC2017.alimentation,
        HcpIPC2017.produits_non_alimentaires,
        HcpIPC2017.ipc
        ).filter(HcpIPC2017.date >= date_initiale,
        HcpIPC2017.date <= date_finale
        ).order_by(asc(HcpIPC2017.date))
    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer


# ----------------------------------------------------------------
#    Function returning tmp series
#
def query_monia(date_initiale, date_finale, session):
    quer = dbs.query(
        BkamMonia.date_marche,
        BkamMonia.taux_monia,
        BkamMonia.volume
        ).filter(BkamMonia.date_marche >= date_initiale,
        BkamMonia.date_marche <= date_finale
        ).order_by(asc(BkamMonia.date_marche))

    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer


def query_tmp(date_initiale, date_finale, session):
    quer = dbs.query(
        BkamTMP.date_marche,
        BkamTMP.taux_ref,
        BkamTMP.volume,
        BkamTMP.encours
        ).filter(BkamTMP.date_marche >= date_initiale,
        BkamTMP.date_marche <= date_finale
        ).order_by(asc(BkamTMP.date_marche))

    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer


def query_placements_tresor(date_initiale, date_finale, session):
    quer = dbs.query(
        MefPlacementsTresor.date_pdf,
        MefPlacementsTresor.type_operation,
        MefPlacementsTresor.date_reglement,
        MefPlacementsTresor.date_echeance,
        MefPlacementsTresor.duree_placement,
        MefPlacementsTresor.montant_placement,
        MefPlacementsTresor.tmp_placement
        ).filter(
        MefPlacementsTresor.date_echeance >= date_initiale,
        MefPlacementsTresor.date_echeance <= date_finale
        ).order_by(asc(MefPlacementsTresor.date_echeance))

    # dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer


def query_marche_monetaire(date_initiale, date_finale, session):
    quer = dbs.query(
    BkamMonia.date_marche,
    BkamMonia.taux_monia,
    BkamMonia.volume.label('volume_monia'),
    BkamTMP.taux_ref.label('taux_interbancaire'),
    BkamTMP.volume.label('volume_tmp'),
    BkamTMP.encours.label('encours_tmp'),
    BkamTxDirecteur.taux_directeur,
    BkamTxDirecteur.ratio_reserve_obligatoire,
    BkamTxDirecteur.remuneration_reserve
    ).filter(
    BkamMonia.date_marche == BkamTxDirecteur.date,
    BkamMonia.date_marche == BkamTMP.date_marche,
    BkamMonia.date_marche >= date_initiale,
    BkamMonia.date_marche <= date_finale
    ).order_by(asc(BkamTxDirecteur.date)
    )
    #dquer = quer.all()
    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer


def query_tenors_primaire(date_initiale, date_finale, session):
    quer = dbs.query(
        BkamTenorsPrimaire.date_reglement,
        BkamTenorsPrimaire.t_13s,
        BkamTenorsPrimaire.t_26s,
        BkamTenorsPrimaire.t_52s,
        BkamTenorsPrimaire.t_2ans,
        BkamTenorsPrimaire.t_5ans,
        BkamTenorsPrimaire.t_10ans,
        BkamTenorsPrimaire.t_15ans,
        BkamTenorsPrimaire.t_20ans,
        BkamTenorsPrimaire.t_30ans
    ).filter(BkamTenorsPrimaire.date_reglement >= date_initiale,
    BkamTenorsPrimaire.date_reglement <= date_finale
    ).order_by(asc(BkamTenorsPrimaire.date_reglement))

    dquer = pd.read_sql(quer.statement, quer.session.bind)
    return dquer
# ----------------------------------------------------------------
#    Function returning asfim funds series
#
def query_ranking(date_x, session):
    quer = dbs.query(
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
        Asfim.perf_2a
        ).filter(Asfim.date_marche == date_x
        ).order_by(
        asc(Asfim.date_marche)
        ).order_by(
        asc(Asfim.sdg)
        ).order_by(
        asc(Asfim.denomination)
        ).order_by(
        asc(Asfim.actif_net)
        )
    dquer = quer.all()
    return quer

def query_ranking_all(date_x, classification, periodicite, souscripteurs, actif_net_min, session):
    quer = dbs.query(
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
        Asfim.perf_2a
        ).filter(Asfim.date_marche == date_x,
        Asfim.classification == classification,
        Asfim.periodicite == periodicite,
        Asfim.souscripteurs == souscripteurs,
        Asfim.actif_net >= actif_net_min
        ).order_by(
        desc(Asfim.perf_year_to_date)
        )
    dquer = quer.all()
    return quer

def query_ranking_actions_hebdo(date_x, session):
    quer = dbs.query(
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
        Asfim.perf_2a
        ).filter(Asfim.date_marche == date_x,
        Asfim.classification == 'Actions',
        Asfim.periodicite == 'Hebdomadaire',
        Asfim.souscripteurs == 'FGP',
        Asfim.actif_net >= 50000000
        ).order_by(
        desc(Asfim.perf_year_to_date)
        )
    dquer = quer.all()
    return quer


# def query_pays(session):
#     quer = dbs.query(Pays.code
#             Pays.description,
#             Pays.alpha2,
#             Pays.alpha3,
#             Pays.region,
#             Pays.region_code,
#             Pays.sub_region,
#             Pays.sub_region_code,
#             Pays.iso_version)
#     dquer = quer.all()
#     return quer
