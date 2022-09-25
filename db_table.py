import datetime

from sqlalchemy import (
    create_engine, Sequence, ForeignKey, Table, MetaData, Column, desc, asc,
    Date, Integer, String, VARCHAR, Float, Numeric, REAL, Text, DateTime, func
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates


from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_loading import dbsession


Base = declarative_base()

TABLE_ID = Sequence('table_id_seq', start=1)


# ----------------------------------------------------------
# ----------------------------------------------------------
#
# tables masi lexique indice composition & volume
#
# ----------------------------------------------------------
# ----------------------------------------------------------



class LexIndiceAct(Base):

    __tablename__ = 'lex_indice_act'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    code = Column(VARCHAR)
    libelle = Column(VARCHAR)

    def __init__(self,code,libelle):
        self.code=code
        self.libelle=libelle

    def __repr__(self):
        return '[id:%d][code:%s][libelle:%s]' % self.id, self.code,self.libelle

# LexIndiceAct.__table__.create(dbsession().bind)
MASIINDICE_ID = Sequence('masi_indice_id_seq', start=1)

class MasiIndices(Base):
    __tablename__ = 'masi_indices'
    id = Column(Integer, MASIINDICE_ID, primary_key=True, server_default=MASIINDICE_ID.next_value())
    seance = Column(Date)
    name = Column(String)
    instrument = Column(Float)
    variation = Column(VARCHAR)

    def __init__(self,seance,name, instrument, variation):
        self.seance=seance
        self.name=name
        self.instrument=instrument
        self.variation=variation

    def __repr__(self):
        return '[id:%d][seance:%s][name:%s]' % self.id, self.seance,self.name

#MasiIndices.__table__.create(dbsession().bind)

class LexVolumeAct(Base):

    __tablename__ = 'lex_volume_act'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    code = Column(VARCHAR)
    libelle = Column(VARCHAR)

    def __init__(self,code,libelle):
        self.code=code
        self.libelle=libelle

    def __repr__(self):
        return '[id:%d][code:%s][libelle:%s]' % self.id, self.code,self.libelle

#LexVolumeAct.__table__.create(dbsession().bind)

MASIVOLUME_ID = Sequence('masi_volume_id_seq', start=1)

class MasiVolume(Base):

    __tablename__ = 'masi_volume'

    id = Column(Integer, MASIVOLUME_ID, primary_key=True, server_default=MASIVOLUME_ID.next_value())
    seance = Column(Date)
    name = Column(VARCHAR)
    cours_cloture = Column(Float)
    cours_ajuste = Column(Float)
    evolution = Column(Float)
    quantite_echange = Column(Float)
    volume = Column(Float)

    def __init__(self,seance,name, cours_cloture, cours_ajuste, evolution, quantite_echange, volume):
        self.seance=seance
        self.name=name
        self.cours_cloture=cours_cloture
        self.cours_ajuste=cours_ajuste
        self.evolution=evolution
        self.quantite_echange=quantite_echange
        self.volume=volume

    def __repr__(self):
        return '[id:%d][seance:%s][name:%s]' % self.id, self.seance,self.name

# MasiVolume.__table__.create(dbsession().bind)

MASICOMPOSITION_ID = Sequence('masi_composition_id_seq', start=1)

class MasiComposition(Base):

    __tablename__ = 'masi_composition'

    id = Column(Integer, MASICOMPOSITION_ID, primary_key=True, server_default=MASICOMPOSITION_ID.next_value())
    seance = Column('seance', Date)
    code_isin = Column(VARCHAR)
    libelle = Column(VARCHAR)
    nombre_de_titre = Column(Float)
    cours = Column(Float)
    facteur_flottant = Column(Float)
    facteur_de_plafonnement = Column(Float)
    capi_flottante = Column(Float)
    poids = Column(Float)

    def __init__(self,seance, code_isin, libelle, nombre_de_titre, cours, facteur_flottant, capi_flottante, poids):
        self.seance=seance
        self.code_isin=code_isin
        self.libelle=libelle
        self.nombre_de_titre=nombre_de_titre
        self.cours=cours
        self.facteur_flottant=facteur_flottant
        self.capi_flottante=capi_flottante
        self.poids=poids

    def __repr__(self):
        return '[id:%d][seance:%s][libelle:%s][cours:%s]' % self.id, self.seance,self.libelle, self.cours

# masi_composition.__table__.create(dbsession().bind)

## table du lex_actions
class LexActions(Base):

    __tablename__ = 'lex_actions'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    code_isin = Column(VARCHAR)
    code_mcl = Column(VARCHAR)
    ticker = Column(VARCHAR)
    description = Column(VARCHAR)
    devises = Column(VARCHAR)
    secteur = Column(VARCHAR)
    profil = Column(VARCHAR)
    cyclicite = Column(VARCHAR)

    def __init__(self,code_isin, code_mcl, ticker, description, devises, secteur, profil, cyclicite):
        self.code_isin=code_isin
        self.code_mcl=code_mcl
        self.ticker=ticker
        self.description=description
        self.devises=devises
        self.secteur=secteur
        self.profil=profil
        self.cyclicite=cyclicite

    def __repr__(self):
        return '[id:%d][code_isin:%s][description:%s][secteur:%s]' % self.id, self.code_isin,self.description, self.secteur

class ComptesActions(Base):

    __tablename__ = 'masi_comptes_actions'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    class_compte = Column(VARCHAR)
    type_compte = Column(VARCHAR)
    ticker = Column(VARCHAR)
    date_publication = Column(VARCHAR)
    montant_mad = Column(Float)

# ----------------------------------------------------------
# ----------------------------------------------------------
#
# tables macro & monetaire & taux primaire & secondaire
#
# ----------------------------------------------------------
# ----------------------------------------------------------


# ------
# bkam
# ------
class BkamTxDirecteurConseil(Base):

    __tablename__ = 'bkam_tx_directeur_conseil'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date = Column(Date)
    taux_directeur = Column(Float)
    ratio_reserve_obligatoire = Column(Float)
    remuneration_reserve = Column(Float)

    def __init__(self, date, taux_directeur, ratio_reserve_obligatoire, remuneration_reserve):
        self.date=date
        self.taux_directeur=taux_directeur
        self.ratio_reserve_obligatoire=ratio_reserve_obligatoire
        self.remuneration_reserve=remuneration_reserve

    def __repr__(self):
        return '[id:%d][date:%s][taux_directeur:%s]' % self.id, self.date,self.taux_directeur

TABLE_ID_bkamtxdirecteur = Sequence('table_id_bkamtxdirecteur_seq', start=1)

class BkamTxDirecteur(Base):

    __tablename__ = 'bkam_tx_directeur'

    id = Column(Integer, TABLE_ID_bkamtxdirecteur, primary_key=True, server_default=TABLE_ID_bkamtxdirecteur.next_value())
    date = Column(Date)
    taux_directeur = Column(Float)
    ratio_reserve_obligatoire = Column(Float)
    remuneration_reserve = Column(Float)

    def __init__(self, date, taux_directeur, ratio_reserve_obligatoire, remuneration_reserve):
        self.date=date
        self.taux_directeur=taux_directeur
        self.ratio_reserve_obligatoire=ratio_reserve_obligatoire
        self.remuneration_reserve=remuneration_reserve

    def __repr__(self):
        return '[id:%d][date:%s][taux_directeur:%s]' % self.id, self.date,self.taux_directeur

class BkamTxInflation(Base):

    __tablename__ = 'bkam_tx_inflation'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date = Column(Date)
    inflation_rate_sj_mga = Column(Float)
    inflation_rate_mga = Column(Float)

    def __init__(self, date, inflation_rate_sj_mga, inflation_rate_mga):
        self.date=date
        self.inflation_rate_sj_mga=inflation_rate_sj_mga
        self.inflation_rate_mga=inflation_rate_mga

    def __repr__(self):
        return '[id:%d][date:%s][inflation_rate_sj_mga:%s]' % self.id, self.date,self.inflation_rate_sj_mga
#BkamTxInflation.__table__.create(dbsession().bind)

class BkamEchange(Base):
    __tablename__ = 'bkam_echange'
    # fields = ['date_reglement', 'maturites', 'date_echeance', 'Taux_nominal', 'Montant_propose', 'Montant_retenu', 'maturites', 'date_echeance','Taux_nominal', 'Prix_min', 'Prix_max', 'Montant_retenu', 'PMP']
    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_reglement = Column(Date)
    maturites_rachat = Column(VARCHAR)
    date_echeance_rachat = Column(Date)
    Taux_nominal_rachat = Column(Float)
    Montant_propose_rachat = Column(REAL)
    Montant_retenu_rachat = Column(REAL)
    maturites_remplacement = Column(VARCHAR)
    date_echeance_remplacement = Column(Date)
    Taux_nominal_remplacement = Column(Float)
    Prix_min_remplacement = Column(Float)
    Prix_max_remplacement = Column(Float)
    Montant_retenu_remplacement = Column(REAL)
    PMP_remplacement = Column(Float)

    def __init__(
        self, date_reglement, maturites_rachat, date_echeance_rachat,Taux_nominal_rachat,Montant_propose_rachat, Montant_retenu_rachat,
        maturites_remplacement, date_echeance_remplacement, Taux_nominal_remplacement, Prix_min_remplacement, Prix_max_remplacement,
        Montant_retenu_remplacement, PMP_remplacement):
    
        self.date_reglement=date_reglement
        self.maturites_rachat=maturites_rachat
        self.date_echeance_rachat=date_echeance_rachat
        self.Taux_nominal_rachat=Taux_nominal_rachat
        self.Montant_propose_rachat=Montant_propose_rachat
        self.Montant_retenu_rachat=Montant_retenu_rachat
        self.maturites_remplacement=maturites_remplacement
        self.date_echeance_remplacement=date_echeance_remplacement
        self.Taux_nominal_remplacement=Taux_nominal_remplacement
        self.Prix_min_remplacement=Prix_min_remplacement
        self.Prix_max_remplacement=Prix_max_remplacement
        self.Montant_retenu_remplacement=Montant_retenu_remplacement
        self.PMP_remplacement=PMP_remplacement



    def __repr__(self):
        return '[id:%d][date_reglement:%s]' % self.id, self.date_reglement

# BkamEchange.__table__.create(dbsession().bind)

class BkamAdjudicationPrimaire(Base):

    __tablename__ = 'bkam_adjudication_primaire'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    caracteristique = Column(VARCHAR)
    dateReglement = Column(Date)
    maturite = Column(VARCHAR)
    mntAdjuge = Column(Integer)
    mntPropose = Column(Integer)
    tauxPrixMax = Column(Float)
    tauxPrixMin = Column(Float)
    tauxPrixMoyenPondere = Column(Float)
    tauxPrixlimite = Column(Float)

    def __init__(self, caracteristique, dateReglement, maturite, mntAdjuge, mntPropose, tauxPrixMax, tauxPrixMin,
        tauxPrixMoyenPondere, tauxPrixlimite):
        self.caracteristique=caracteristique
        self.dateReglement=dateReglement
        self.maturite=maturite
        self.mntAdjuge=mntAdjuge
        self.mntPropose=mntPropose
        self.tauxPrixMax=tauxPrixMax
        self.tauxPrixMin=tauxPrixMin
        self.tauxPrixMoyenPondere=tauxPrixMoyenPondere
        self.tauxPrixlimite=tauxPrixlimite

    def __repr__(self):
        return '[id:%d][dateReglement:%s][maturite:%s]' % self.id, self.dateReglement,self.maturite
# BkamAdjudicationPrimaire.__table__.create(dbsession().bind)


class BkamTenorsPrimaire(Base):

    __tablename__ = 'bkam_courbe_primaire'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_reglement = Column(Date)
    t_13s = Column(Float)
    t_26s = Column(Float)
    t_52s = Column(Float)
    t_2ans = Column(Float)
    t_5ans = Column(Float)
    t_10ans = Column(Float)
    t_15ans = Column(Float)
    t_20ans = Column(Float)
    t_30ans = Column(Float)

    def __init__(self, dateReglement, t_13s, t_26s, t_52s, t_2ans, t_5ans, t_10ans,
        t_15ans, t_20ans, t_30ans):
        self.dateReglement=dateReglement
        self.t_13s=t_13s
        self.t_26s=t_26s
        self.t_52s=t_52s
        self.t_2ans=t_2ans
        self.t_5ans=t_5ans
        self.t_10ans=t_10ans
        self.t_15ans=t_15ans
        self.t_20ans=t_20ans
        self.t_30ans=t_30ans

    def __repr__(self):
        return '[id:%d][dateReglement:%s]' % self.id, self.dateReglement
#BkamTenorsPrimaire.__table__.create(dbsession().bind)


TABLE_ID_bkamcourbetenors = Sequence('table_id_bkamcourbetenors_seq', start=1)

class BkamCourbeTenors(Base):

    __tablename__ = 'bkam_courbe_tenors'

    id = Column(Integer, TABLE_ID_bkamcourbetenors, primary_key=True, server_default=TABLE_ID_bkamcourbetenors.next_value())
    date_marche = Column(Date)
    tenors_y = Column(VARCHAR)
    tenors_d = Column(Integer)
    taux = Column(Float)

    def __init__(self, date_marche, tenors_y, tenors_d, taux):
        self.date_marche=date_marche
        self.tenors_y=tenors_y
        self.tenors_d=tenors_d
        self.taux=taux

    def __repr__(self):
        return '[id:%d][date_marche:%s]' % self.id, self.date_marche
# BkamCourbeTenors.__table__.create(dbsession().bind)


class BkamCourbe(Base):

    __tablename__ = 'bkam_courbe'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_marche = Column(Date)
    date_echeance = Column(Date)
    transactions = Column(Float)
    taux = Column(Float)
    date_valeur = Column(Date)
    date_transaction = Column(Date)

    def __init__(self, date_marche, date_echeance, transactions, taux, date_valeur,date_transaction):
        self.date_marche=date_marche
        self.date_echeance=date_echeance
        self.transactions=transactions
        self.taux=taux
        self.date_valeur=date_valeur
        self.date_transaction=date_transaction

    def __repr__(self):
        return '[id:%d][date_marche:%s]' % self.id, self.date_marche

    # created_at = Column(Date)
# BkamCourbe.__table__.create(dbsession().bind)


class BkamTMP(Base):
    __tablename__ = 'bkam_tmp'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_marche = Column(Date)
    taux_ref    = Column(Float)
    volume      = Column(Float)
    encours     = Column(Float)

    def __init__(self,date_marche,taux_ref, volume, encours):
        self.date_marche=date_marche
        self.taux_ref=taux_ref
        self.volume=volume
        self.encours=encours

    def __repr__(self):
        return '[id:%d][date_marche:%s][taux_ref:%s]' % self.id, self.date_marche,self.taux_ref

# BkamTMP.__table__.create(dbsession().bind)

class BkamMonia(Base):
    __tablename__ = 'bkam_monia'
    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_marche = Column(Date)
    date_publication = Column(Date)
    taux_monia    = Column(Float)
    volume      = Column(Float)

    def __init__(self,date_marche,date_publication, taux_monia, volume):
        self.date_marche=date_marche
        self.date_publication=date_publication
        self.taux_monia=taux_monia
        self.volume=volume

    def __repr__(self):
        return '[id:%d][date_marche:%s][taux_monia:%s]' % self.id, self.date_marche,self.taux_monia

# BkamMonia.__table__.create(dbsession().bind)


TABLE_ID_bkambb_txchange = Sequence('table_id_bkambb_seq_5', start=1)

class BkamBilletsBanques(Base):
    __tablename__ = 'bkam_billets_banques'
    id = Column(Integer, TABLE_ID_bkambb_txchange, primary_key=True, server_default=TABLE_ID_bkambb_txchange.next_value())
    date_marche = Column(Date)
    devises = Column(String)
    achat_clientele = Column(Float)
    vente_clientele = Column(Float)

    def __init__(self,date_marche,date, libDevise):
        self.date_marche=date_marche
        self.devises=devises

    def __repr__(self):
        return '[id:%d][date_marche:%s][devises:%s]' % self.id, self.date_marche,self.devises

# BkamBilletsBanques.__table__.create(dbsession().bind)


# ------
# hcp
# ------
class HcpIPC2017(Base):

    __tablename__ = 'hcp_ipc_base2017'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date = Column(Date)
    produits_alimentaires_boissons = Column(Float)
    boissons_alcoolisees_tabac = Column(Float)
    textiles = Column(Float)
    logement_eau_energies = Column(Float)
    meubles_articles_menage_entretien = Column(Float)
    sante = Column(Float)
    transports = Column(Float)
    communications = Column(Float)
    loisirs_culture = Column(Float)
    enseignement = Column(Float)
    restaurants_hotels = Column(Float)
    biens_services_divers = Column(Float)
    alimentation = Column(Float)
    produits_non_alimentaires = Column(Float)
    ipc = Column(Float)
#HcpIPC2017.__table__.create(dbsession().bind)

# ------
# mef
# ------
class MefPlacementsTresor(Base):

    __tablename__ = 'mef_placements_tresor'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_pdf = Column(Date)
    type_operation = Column(Text)
    date_reglement = Column(Date)
    date_echeance = Column(Date)
    duree_placement = Column(Text)
    montant_placement = Column(Float)
    tmp_placement = Column(Float)


    def __init__(self, date_pdf, type_operation, date_reglement, date_echeance, duree_placement,montant_placement, tmp_placement):
        self.date_pdf=date_pdf
        self.type_operation=type_operation
        self.date_reglement=date_reglement
        self.date_echeance=date_echeance
        self.duree_placement=duree_placement
        self.montant_placement=montant_placement
        self.tmp_placement=tmp_placement

    def __repr__(self):
        return '[id:%d][date_pdf:%s]' % self.id, self.date_pdf

#MefPlacementsTresor.__table__.create(dbsession().bind)

class MefAdjudicationPrimaire(Base):

    __tablename__ = 'mef_adjudication_primaire'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    code_isin = Column(Text)
    maturite = Column(Text)
    taux_facial = Column(Float)
    date_echeance = Column(Date)
    montant_propose = Column(Float)
    PMP = Column(Float)
    TMP = Column(Float)
    montant_retenu = Column(Float)
    PMP = Column(Float)
    taux_retenu = Column(Float)
    dernier_TMP = Column(Float)
    ecart_dernier_TMP = Column(Float)
# MefAdjudicationPrimaire.__table__.create(dbsession().bind)

# ---------
# Maroclear
# ---------

class Mcl(Base):

    __tablename__ = 'mcl'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    code_isin = Column(VARCHAR)
    famille_instrument = Column(VARCHAR)
    categorie_instrument = Column(VARCHAR)
    libelle_court = Column(VARCHAR)
    description = Column(VARCHAR)
    isin_emetteur = Column(VARCHAR)
    capital_emis = Column(Integer)
    qt_emise = Column(Float)
    nominal = Column(Float)
    taux_facial = Column(VARCHAR)
    type_coupon = Column(VARCHAR)
    date_emission = Column(Date)
    date_jouissance = Column(Date)
    date_echeance = Column(Date)
    garantie = Column(VARCHAR)
    nominal_actuel = Column(Float)
    periodicite = Column(VARCHAR)
    type_remboursement = Column(VARCHAR)
    periodicite_amortissement = Column(VARCHAR)
    forme_detention = Column(VARCHAR)
    cote = Column(VARCHAR)
    mnemonique = Column(VARCHAR)
    isin_centralisateur = Column(VARCHAR)
    nom_emetteur = Column(VARCHAR)
    code_enregistrement = Column(VARCHAR)

#    dates_coupons = Column(VARCHAR)
#    statut_instrument = Column(VARCHAR)
# Mcl.__table__.create(dbsession().bind)


# ---------
# pricer_histo_valos
# ---------

class MclHistoValos(Base):

    __tablename__ = 'mcl_histo_valos'

    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_valeur = Column(Date)
    isin = Column(VARCHAR)
    nominal = Column(Float)
    date_emission = Column(Date)
    date_jouissance = Column(Date)
    date_echeance = Column(Date)
    date_premier_coupon = Column(Date)
    date_coupon_precedant = Column(Date)
    date_coupon_suivant = Column(Date)
    taux_facial = Column(Float)
    spread = Column(Float)
    coupon_couru = Column(Float)
    type_ligne = Column(VARCHAR)
    mat_init = Column(Integer)
    mat_res = Column(Integer)
    taux_courbe = Column(Float)
    prix = Column(Float)
    sensibilite = Column(Float)
    duration = Column(Float)
    convexite = Column(Float)
# MclHistoValos.__table__.create(dbsession().bind)




## table des fichiers de l'asfim
class Asfim(Base):
    __tablename__ = 'asfim'
    id = Column(Integer, TABLE_ID, primary_key=True, server_default=TABLE_ID.next_value())
    date_marche        = Column(String)
    code_isin          = Column(VARCHAR)
    code_mcl           = Column(VARCHAR)
    denomination       = Column(VARCHAR)
    sdg                = Column(String)
    nature_juridique   = Column(VARCHAR)
    classification     = Column(VARCHAR)
    sensibilite        = Column(VARCHAR)
    benchmark          = Column(VARCHAR)
    periodicite        = Column(VARCHAR)
    souscripteurs      = Column(VARCHAR)
    affectation        = Column(VARCHAR)
    frais_souscription = Column(VARCHAR)
    frais_rachat       = Column(VARCHAR)
    frais_gestion      = Column(VARCHAR)
    depositaire        = Column(VARCHAR)
    reseau_placeur     = Column(VARCHAR)
    actif_net          = Column(VARCHAR)
    valeur_liquidative = Column(VARCHAR)
    perf_year_to_date  = Column(VARCHAR)
    perf_1j = Column(VARCHAR)
    perf_1s = Column(VARCHAR)
    perf_1m = Column(VARCHAR)
    perf_3m = Column(VARCHAR)
    perf_6m = Column(VARCHAR)
    perf_1a = Column(VARCHAR)
    perf_2a = Column(VARCHAR)
    perf_3a = Column(VARCHAR)
    perf_5a = Column(VARCHAR)


class Pays(Base):

    """Représente les Pays selon la norme ISO 3361-2
    avec une ségmentation par region et sub-region.

    Attributes:
        id (int): L'ID du Pays
        alpha2 (str): Code alpha-2 du Pays *UNIQUE*
        alpha3 (str): Code alpha-3 du Pays *UNIQUE*
        code (str): Code numérique su Pays
        description (str): Descriptoin du Pays (Anglais)
        iso_version (str): Version de la norme ISO
        region (str): Région du Pays
        region_code (str): Code de la Région
        sub_region (str): Sous-Région
        sub_region_code (str): Code de la Sous-Région
    """

    __tablename__ = 'pays'

    id = Column('id', Integer, primary_key=True)
    code = Column('code', Text, unique=True)
    description = Column('description', Text)
    alpha2 = Column('alpha2', Text, index=True, unique=True)
    alpha3 = Column('alpha3', Text)
    region = Column('region', Text)
    region_code = Column('region_code', Text)
    sub_region = Column('sub_region', Text)
    sub_region_code = Column('sub_region_code', Text)
    iso_version = Column('iso_version', Text)
    created_at = Column('created_at', DateTime, default=datetime.datetime.now, server_default=func.now())

    @classmethod
    def by_alpha2(cls, alpha2: str):
        """Cherche un Pays par son code ALPHA-2

        Args:
            alpha2 (str): Code ALPHA-2 du Pays selon la norme ISO 3361-2

        Returns:
            Pays: Instance Pays ou None if NotFound!
        """
        up = alpha2.strip().upper()
        session = dbsession()
        i = session.query(cls).filter_by(**{'alpha2': up}).first()
        return i

    @classmethod
    def by_alpha3(cls, alpha3: str):
        """Cherche un Pays par son code ALPHA-3

        Args:
            alpha3 (str): Code ALPHA-3 du Pays selon la norme ISO 3361-2

        Returns:
            Pays: Instance Pays ou None if NotFound!
        """
        up = alpha3.strip().upper()
        session = dbsession()
        i = session.query(cls).filter_by(**{'alpha3': up}).first()
        return i

    @validates('description', 'region', 'sub_region', 'alpha2', 'alpha3')
    def _upper_key(self, key, value):
        return upper_string(value)

#Pays.__table__.create(dbsession().bind)


TABLE_ID_asfimfonds = Sequence('table_id_asfimfonds_seq', start=1)

class AsfimFonds(Base):
    __tablename__ = 'asfim_fonds'

    id = Column(Integer, TABLE_ID_asfimfonds, primary_key=True, server_default=TABLE_ID_asfimfonds.next_value())
    code_isin = Column(VARCHAR)
    code_mcl = Column(VARCHAR)
    description = Column(VARCHAR)
    forme_juridique = Column(VARCHAR)
    classification = Column(VARCHAR)
    sdg = Column(VARCHAR)
    depositaire = Column(VARCHAR)
    reseau_placeur = Column(VARCHAR)
    periodicite = Column(VARCHAR)
    indice_benchmark = Column(VARCHAR)
    souscripteurs = Column(VARCHAR)
    affectation_resultats = Column(VARCHAR)
    frais_gestion = Column(Float)
    frais_rachat = Column(Float)
    frais_souscription = Column(Float)
    created_at = Column(Date)

    def __init__(self, code_isin, description, classification, sdg):
        self.code_isin=code_isin
        self.description=description
        self.classification=classification
        self.sdg=sdg

    def __repr__(self):
        return '[id:%d][code_isin:%s][description:%s][classification:%s][sdg:%s]' % self.id, self.description, self.classification, self.sdg
# BkamCourbeTenors.__table__.create(dbsession().bind)



TABLE_ID_asfimperf = Sequence('table_id_asfimperf_seq', start=1)
class AsfimPerformances(Base):
    __tablename__ = 'asfim_performances'

    id = Column(Integer, TABLE_ID_asfimperf, primary_key=True, server_default=TABLE_ID_asfimperf.next_value())
    id_fonds = Column(Integer, ForeignKey("asfim_fonds.id"))
    date_marche = Column(Date)
    actif_net = Column(Float)
    vl = Column(Float)
    perf_ytd = Column(Float)
    perf_1s = Column(Float)
    perf_1m = Column(Float)
    perf_3m = Column(Float)
    perf_6m = Column(Float)
    perf_1a = Column(Float)
    perf_2a = Column(Float)
    perf_3a = Column(Float)
    perf_5a = Column(Float)

    def __init__(self, id_fonds, date_marche, actif_net, vl, perf_ytd):
        self.id_fonds=id_fonds
        self.date_marche=date_marche
        self.actif_net=actif_net
        self.vl=vl
        self.perf_ytd=perf_ytd

    def __repr__(self):
        return '[id:%d][id_fonds:%s][date_marche:%s][actif_net:%s][perf_ytd:%s]' % self.id, self.id_fonds, self.date_marche, self.actif_net, self.perf_ytd
# BkamCourbeTenors.__table__.create(dbsession().bind)



class AsfimFonds_b(Base):
    __tablename__ = 'fonds_market'

    id = Column(Integer, TABLE_ID_asfimfonds, primary_key=True, server_default=TABLE_ID_asfimfonds.next_value())
    code_isin = Column(VARCHAR)
    code_mcl = Column(VARCHAR)
    description = Column(VARCHAR)
    forme_juridique = Column(VARCHAR)
    classification = Column(VARCHAR)
    sdg = Column(VARCHAR)
    depositaire = Column(VARCHAR)
    reseau_placeur = Column(VARCHAR)
    periodicite = Column(VARCHAR)
    indice_benchmark = Column(VARCHAR)
    souscripteurs = Column(VARCHAR)
    affectation_resultats = Column(VARCHAR)
    frais_gestion = Column(Float)
    frais_rachat = Column(Float)
    frais_souscription = Column(Float)
    created_at = Column(Date)

    def __init__(self, code_isin, description, classification, sdg):
        self.code_isin=code_isin
        self.description=description
        self.classification=classification
        self.sdg=sdg

    def __repr__(self):
        return '[id:%d][code_isin:%s][description:%s][classification:%s][sdg:%s]' % self.id, self.description, self.classification, self.sdg
# BkamCourbeTenors.__table__.create(dbsession().bind)

class AsfimPerformances_b(Base):
    __tablename__ = 'indicateur_fonds'

    id = Column(Integer, primary_key=True)
    id_fonds = Column(Integer, ForeignKey("asfim_fonds.id"))
    date_marche = Column(Date)
    actif_net = Column(Float)
    vl = Column(Float)
    debut_annee = Column(Float)
    perf_1s = Column(Float)
    perf_1m = Column(Float)
    perf_3m = Column(Float)
    perf_6m = Column(Float)
    perf_1a = Column(Float)
    perf_2a = Column(Float)
    perf_3a = Column(Float)
    perf_5a = Column(Float)

    def __init__(self, id_fonds, date_marche, actif_net, vl, perf_ytd):
        self.id_fonds=id_fonds
        self.date_marche=date_marche
        self.actif_net=actif_net
        self.vl=vl
        self.perf_ytd=perf_ytd

    def __repr__(self):
        return '[id:%d][id_fonds:%s][date_marche:%s][actif_net:%s][perf_ytd:%s]' % self.id, self.id_fonds, self.date_marche, self.actif_net, self.perf_ytd
# BkamCourbeTenors.__table__.create(dbsession().bind)

# dquer_asfim['code_isin']=dquer_asfim['code_isin'].apply(lambda x: str(x))
# dquer_asfim['code_mcl']=dquer_asfim['code_mcl'].apply(lambda x: str(x))
# dquer_asfim['description']=dquer_asfim['description'].apply(lambda x: str(x))
# dquer_asfim['forme_juridique']=dquer_asfim['forme_juridique'].apply(lambda x: str(x))
# dquer_asfim['classification']=dquer_asfim['classification'].apply(lambda x: str(x))
# dquer_asfim['sdg']=dquer_asfim['sdg'].apply(lambda x: str(x))
# dquer_asfim['depositaire']=dquer_asfim['depositaire'].apply(lambda x: str(x))
# dquer_asfim['reseau_placeur']=dquer_asfim['reseau_placeur'].apply(lambda x: str(x))
# dquer_asfim['periodicite']=dquer_asfim['periodicite'].apply(lambda x: str(x))
# dquer_asfim['indice_benchmark']=dquer_asfim['indice_benchmark'].apply(lambda x: str(x))
# dquer_asfim['souscripteurs']=dquer_asfim['souscripteurs'].apply(lambda x: str(x))
# dquer_asfim['affectation_resultats']=dquer_asfim['affectation_resultats'].apply(lambda x: str(x))
# dquer_asfim['frais_gestion']=dquer_asfim['frais_gestion'].apply(lambda x: str(x))
# dquer_asfim['frais_rachat']=dquer_asfim['frais_rachat'].apply(lambda x: str(x))
# dquer_asfim['frais_souscription']=dquer_asfim['frais_souscription'].apply(lambda x: str(x))
# dquer_asfim['created_at']=dquer_asfim['created_at'].apply(lambda x: datetime.date.today())
