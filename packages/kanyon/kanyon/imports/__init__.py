"""Extraction des données de marché depuis les sources publiques.

Chaque module cible une source (composition MASI, indices, volumes) et expose
une fonction ``extract_implement_*`` qui importe de façon *incrémentale* :
seules les dates manquantes en base sont récupérées.
"""
