"""Modèles ORM SQLAlchemy.

Regroupe les tables des marchés marocains :

* Actions / indice MASI (lexiques, indices, volumes, composition, comptes).
* Marché monétaire et obligataire (BKAM : taux directeur, MONIA, TMP, courbes,
  adjudications, échanges, billets de banque).
* Macro (HCP : IPC base 2017).
* Trésor / MEF (placements, adjudications primaires).
* Maroclear (référentiel titres, historique de valorisation).
* ASFIM (référentiel et performances des fonds).
* Référentiel des pays (ISO 3166).

Les modèles utilisent le constructeur par mots-clés fourni par défaut par
SQLAlchemy (``Model(col=valeur, ...)``) : les anciens ``__init__`` positionnels,
sources de plusieurs bugs (arguments manquants ou décalés), ont été retirés.
"""
from __future__ import annotations

import datetime

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    REAL,
    String,
    Text,
    VARCHAR,
    func,
)
from sqlalchemy.orm import validates

from kanyon.db.base import Base
from kanyon.helpers import upper_string


# ---------------------------------------------------------------------------
# Actions / indice MASI (lexiques, indices, volumes, composition, comptes)
# ---------------------------------------------------------------------------
class LexIndiceAct(Base):
    __tablename__ = "lex_indice_act"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(VARCHAR)
    libelle = Column(VARCHAR)

    def __repr__(self) -> str:
        return f"<LexIndiceAct id={self.id} code={self.code!r} libelle={self.libelle!r}>"


class MasiIndices(Base):
    __tablename__ = "masi_indices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    seance = Column(Date)
    name = Column(String)
    instrument = Column(Float)
    variation = Column(VARCHAR)

    def __repr__(self) -> str:
        return f"<MasiIndices id={self.id} seance={self.seance} name={self.name!r}>"


class LexVolumeAct(Base):
    __tablename__ = "lex_volume_act"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(VARCHAR)
    libelle = Column(VARCHAR)

    def __repr__(self) -> str:
        return f"<LexVolumeAct id={self.id} code={self.code!r} libelle={self.libelle!r}>"


class MasiVolume(Base):
    __tablename__ = "masi_volume"

    id = Column(Integer, primary_key=True, autoincrement=True)
    seance = Column(Date)
    name = Column(VARCHAR)
    cours_cloture = Column(Float)
    cours_ajuste = Column(Float)
    evolution = Column(Float)
    quantite_echange = Column(Float)
    volume = Column(Float)

    def __repr__(self) -> str:
        return f"<MasiVolume id={self.id} seance={self.seance} name={self.name!r}>"


class MasiComposition(Base):
    __tablename__ = "masi_composition"

    id = Column(Integer, primary_key=True, autoincrement=True)
    seance = Column(Date)
    code_isin = Column(VARCHAR)
    libelle = Column(VARCHAR)
    nombre_de_titre = Column(Float)
    cours = Column(Float)
    facteur_flottant = Column(Float)
    facteur_de_plafonnement = Column(Float)
    capi_flottante = Column(Float)
    poids = Column(Float)

    def __repr__(self) -> str:
        return (
            f"<MasiComposition id={self.id} seance={self.seance} "
            f"libelle={self.libelle!r} cours={self.cours}>"
        )


class LexActions(Base):
    __tablename__ = "lex_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code_isin = Column(VARCHAR)
    code_mcl = Column(VARCHAR)
    ticker = Column(VARCHAR)
    description = Column(VARCHAR)
    devises = Column(VARCHAR)
    secteur = Column(VARCHAR)
    profil = Column(VARCHAR)
    cyclicite = Column(VARCHAR)

    def __repr__(self) -> str:
        return (
            f"<LexActions id={self.id} code_isin={self.code_isin!r} "
            f"description={self.description!r} secteur={self.secteur!r}>"
        )


class ComptesActions(Base):
    __tablename__ = "masi_comptes_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    class_compte = Column(VARCHAR)
    type_compte = Column(VARCHAR)
    ticker = Column(VARCHAR)
    date_publication = Column(VARCHAR)
    montant_mad = Column(Float)


# ---------------------------------------------------------------------------
# BKAM : marché monétaire et obligataire
# ---------------------------------------------------------------------------
class BkamTxDirecteurConseil(Base):
    __tablename__ = "bkam_tx_directeur_conseil"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    taux_directeur = Column(Float)
    ratio_reserve_obligatoire = Column(Float)
    remuneration_reserve = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamTxDirecteurConseil id={self.id} date={self.date} taux={self.taux_directeur}>"


class BkamTxDirecteur(Base):
    __tablename__ = "bkam_tx_directeur"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    taux_directeur = Column(Float)
    ratio_reserve_obligatoire = Column(Float)
    remuneration_reserve = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamTxDirecteur id={self.id} date={self.date} taux={self.taux_directeur}>"


class BkamTxInflation(Base):
    __tablename__ = "bkam_tx_inflation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    inflation_rate_sj_mga = Column(Float)
    inflation_rate_mga = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamTxInflation id={self.id} date={self.date} inflation={self.inflation_rate_mga}>"


class BkamEchange(Base):
    __tablename__ = "bkam_echange"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_reglement = Column(Date)
    maturites_rachat = Column(VARCHAR)
    date_echeance_rachat = Column(Date)
    taux_nominal_rachat = Column(Float)
    montant_propose_rachat = Column(REAL)
    montant_retenu_rachat = Column(REAL)
    maturites_remplacement = Column(VARCHAR)
    date_echeance_remplacement = Column(Date)
    taux_nominal_remplacement = Column(Float)
    prix_min_remplacement = Column(Float)
    prix_max_remplacement = Column(Float)
    montant_retenu_remplacement = Column(REAL)
    pmp_remplacement = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamEchange id={self.id} date_reglement={self.date_reglement}>"


class BkamAdjudicationPrimaire(Base):
    __tablename__ = "bkam_adjudication_primaire"

    id = Column(Integer, primary_key=True, autoincrement=True)
    caracteristique = Column(VARCHAR)
    date_reglement = Column(Date)
    maturite = Column(VARCHAR)
    mnt_adjuge = Column(Integer)
    mnt_propose = Column(Integer)
    taux_prix_max = Column(Float)
    taux_prix_min = Column(Float)
    taux_prix_moyen_pondere = Column(Float)
    taux_prix_limite = Column(Float)

    def __repr__(self) -> str:
        return (
            f"<BkamAdjudicationPrimaire id={self.id} "
            f"date_reglement={self.date_reglement} maturite={self.maturite!r}>"
        )


class BkamTenorsPrimaire(Base):
    __tablename__ = "bkam_courbe_primaire"

    id = Column(Integer, primary_key=True, autoincrement=True)
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

    def __repr__(self) -> str:
        return f"<BkamTenorsPrimaire id={self.id} date_reglement={self.date_reglement}>"


class BkamCourbeTenors(Base):
    __tablename__ = "bkam_courbe_tenors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_marche = Column(Date)
    tenors_y = Column(VARCHAR)
    tenors_d = Column(Integer)
    taux = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamCourbeTenors id={self.id} date_marche={self.date_marche}>"


class BkamCourbe(Base):
    __tablename__ = "bkam_courbe"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_marche = Column(Date)
    date_echeance = Column(Date)
    transactions = Column(Float)
    taux = Column(Float)
    date_valeur = Column(Date)
    date_transaction = Column(Date)

    def __repr__(self) -> str:
        return f"<BkamCourbe id={self.id} date_marche={self.date_marche}>"


class BkamTMP(Base):
    __tablename__ = "bkam_tmp"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_marche = Column(Date)
    taux_ref = Column(Float)
    volume = Column(Float)
    encours = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamTMP id={self.id} date_marche={self.date_marche} taux_ref={self.taux_ref}>"


class BkamMonia(Base):
    __tablename__ = "bkam_monia"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_marche = Column(Date)
    date_publication = Column(Date)
    taux_monia = Column(Float)
    volume = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamMonia id={self.id} date_marche={self.date_marche} taux_monia={self.taux_monia}>"


class BkamBilletsBanques(Base):
    __tablename__ = "bkam_billets_banques"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_marche = Column(Date)
    devises = Column(String)
    achat_clientele = Column(Float)
    vente_clientele = Column(Float)

    def __repr__(self) -> str:
        return f"<BkamBilletsBanques id={self.id} date_marche={self.date_marche} devises={self.devises!r}>"


# ---------------------------------------------------------------------------
# HCP : macro
# ---------------------------------------------------------------------------
class HcpIPC2017(Base):
    __tablename__ = "hcp_ipc_base2017"

    id = Column(Integer, primary_key=True, autoincrement=True)
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


# ---------------------------------------------------------------------------
# MEF / Trésor
# ---------------------------------------------------------------------------
class MefPlacementsTresor(Base):
    __tablename__ = "mef_placements_tresor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_pdf = Column(Date)
    type_operation = Column(Text)
    date_reglement = Column(Date)
    date_echeance = Column(Date)
    duree_placement = Column(Text)
    montant_placement = Column(Float)
    tmp_placement = Column(Float)

    def __repr__(self) -> str:
        return f"<MefPlacementsTresor id={self.id} date_pdf={self.date_pdf}>"


class MefAdjudicationPrimaire(Base):
    __tablename__ = "mef_adjudication_primaire"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code_isin = Column(Text)
    maturite = Column(Text)
    taux_facial = Column(Float)
    date_echeance = Column(Date)
    montant_propose = Column(Float)
    montant_retenu = Column(Float)
    pmp = Column(Float)
    tmp = Column(Float)
    taux_retenu = Column(Float)
    dernier_tmp = Column(Float)
    ecart_dernier_tmp = Column(Float)


# ---------------------------------------------------------------------------
# Maroclear
# ---------------------------------------------------------------------------
class Mcl(Base):
    __tablename__ = "mcl"

    id = Column(Integer, primary_key=True, autoincrement=True)
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
    # Colonnes référencées par les requêtes (query_titre_all / query_mcl) :
    # rétablies ici, elles étaient utilisées mais restées commentées.
    dates_coupons = Column(VARCHAR)
    statut_instrument = Column(VARCHAR)

    def __repr__(self) -> str:
        return f"<Mcl id={self.id} code_isin={self.code_isin!r}>"


class MclHistoValos(Base):
    __tablename__ = "mcl_histo_valos"

    id = Column(Integer, primary_key=True, autoincrement=True)
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


# ---------------------------------------------------------------------------
# ASFIM : fonds
# ---------------------------------------------------------------------------
class Asfim(Base):
    __tablename__ = "asfim"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_marche = Column(String)
    code_isin = Column(VARCHAR)
    code_mcl = Column(VARCHAR)
    denomination = Column(VARCHAR)
    sdg = Column(String)
    nature_juridique = Column(VARCHAR)
    classification = Column(VARCHAR)
    sensibilite = Column(VARCHAR)
    benchmark = Column(VARCHAR)
    periodicite = Column(VARCHAR)
    souscripteurs = Column(VARCHAR)
    affectation = Column(VARCHAR)
    frais_souscription = Column(VARCHAR)
    frais_rachat = Column(VARCHAR)
    frais_gestion = Column(VARCHAR)
    depositaire = Column(VARCHAR)
    reseau_placeur = Column(VARCHAR)
    actif_net = Column(VARCHAR)
    valeur_liquidative = Column(VARCHAR)
    perf_year_to_date = Column(VARCHAR)
    perf_1j = Column(VARCHAR)
    perf_1s = Column(VARCHAR)
    perf_1m = Column(VARCHAR)
    perf_3m = Column(VARCHAR)
    perf_6m = Column(VARCHAR)
    perf_1a = Column(VARCHAR)
    perf_2a = Column(VARCHAR)
    perf_3a = Column(VARCHAR)
    perf_5a = Column(VARCHAR)


class AsfimFonds(Base):
    __tablename__ = "asfim_fonds"

    id = Column(Integer, primary_key=True, autoincrement=True)
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

    def __repr__(self) -> str:
        return (
            f"<AsfimFonds id={self.id} code_isin={self.code_isin!r} "
            f"description={self.description!r}>"
        )


class AsfimPerformances(Base):
    __tablename__ = "asfim_performances"

    id = Column(Integer, primary_key=True, autoincrement=True)
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

    def __repr__(self) -> str:
        return (
            f"<AsfimPerformances id={self.id} id_fonds={self.id_fonds} "
            f"date_marche={self.date_marche}>"
        )


# ---------------------------------------------------------------------------
# Référentiel des pays (ISO 3166)
# ---------------------------------------------------------------------------
class Pays(Base):
    """Représente les Pays selon la norme ISO 3166, segmentés par région.

    Attributes:
        id: Identifiant du Pays.
        alpha2: Code alpha-2 (unique).
        alpha3: Code alpha-3.
        code: Code numérique du Pays.
        description: Description du Pays (anglais).
        iso_version: Version de la norme ISO.
        region / region_code: Région et son code.
        sub_region / sub_region_code: Sous-région et son code.
    """

    __tablename__ = "pays"

    id = Column(Integer, primary_key=True)
    code = Column(Text, unique=True)
    description = Column(Text)
    alpha2 = Column(Text, index=True, unique=True)
    alpha3 = Column(Text)
    region = Column(Text)
    region_code = Column(Text)
    sub_region = Column(Text)
    sub_region_code = Column(Text)
    iso_version = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.now, server_default=func.now())

    @classmethod
    def by_alpha2(cls, alpha2: str, session=None):
        """Cherche un Pays par son code ALPHA-2.

        Args:
            alpha2: Code ALPHA-2 selon la norme ISO 3166.
            session: Session SQLAlchemy optionnelle (créée si absente).

        Returns:
            L'instance ``Pays`` ou ``None`` si introuvable.
        """
        from kanyon.db.base import get_session

        own_session = session is None
        session = session or get_session()
        try:
            return session.query(cls).filter_by(alpha2=upper_string(alpha2)).first()
        finally:
            if own_session:
                session.close()

    @classmethod
    def by_alpha3(cls, alpha3: str, session=None):
        """Cherche un Pays par son code ALPHA-3.

        Args:
            alpha3: Code ALPHA-3 selon la norme ISO 3166.
            session: Session SQLAlchemy optionnelle (créée si absente).

        Returns:
            L'instance ``Pays`` ou ``None`` si introuvable.
        """
        from kanyon.db.base import get_session

        own_session = session is None
        session = session or get_session()
        try:
            return session.query(cls).filter_by(alpha3=upper_string(alpha3)).first()
        finally:
            if own_session:
                session.close()

    @validates("description", "region", "sub_region", "alpha2", "alpha3")
    def _upper_key(self, key, value):
        return upper_string(value)
