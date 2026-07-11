"""kanyon - accès aux données des marchés financiers marocains.

Le package couvre l'ensemble de la chaîne :

    extraction (web scraping) -> stockage (SQLAlchemy / PostgreSQL) -> manipulation (pandas).

Sous-packages :
    kanyon.config      Configuration centralisée (variables d'environnement).
    kanyon.db          Modèles ORM, session et requêtes.
    kanyon.helpers     Fonctions utilitaires (parsing dates / nombres).
    kanyon.imports     Extraction des données depuis les sources publiques.
    kanyon.analytics   Indicateurs de gestion de portefeuille (ratios, VaR...).
"""

__version__ = "0.2.0"

__all__ = ["__version__"]
