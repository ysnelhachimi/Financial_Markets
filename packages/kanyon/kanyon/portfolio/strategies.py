"""Stratégies par catégorie d'OPCVM (classification AMMC) — M4.

Chaque catégorie définit son objectif d'optimisation par défaut, ses bornes
d'exposition par classe d'actifs et ses garde-fous (sensibilité...). Les seuils
sont **indicatifs et paramétrables** : ils doivent être calés sur les circulaires
AMMC en vigueur avant tout usage réglementaire.

Catégories : Actions, Diversifié, Obligations Moyen/Long Terme (OMLT),
Obligations Court Terme (OCT), Monétaire.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class CategoryStrategy:
    """Paramètres de stratégie d'une catégorie d'OPCVM."""

    code: str
    label: str
    objective: str  # objectif d'optimisation par défaut (cf. construction.optimize)
    # Bornes d'exposition (min, max) par classe d'actifs : "actions", "obligataire", "monetaire".
    asset_bounds: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    # Bornes de sensibilité (duration modifiée) globale du portefeuille.
    sensitivity_bounds: Optional[Tuple[float, float]] = None
    description: str = ""


# Définitions indicatives (à valider avec les textes AMMC en vigueur).
STRATEGIES: Dict[str, CategoryStrategy] = {
    "actions": CategoryStrategy(
        code="actions",
        label="Actions",
        objective="max_sharpe",
        asset_bounds={"actions": (0.60, 1.00), "obligataire": (0.0, 0.40), "monetaire": (0.0, 0.40)},
        description="Au moins 60 % d'actions ; recherche du meilleur couple rendement/risque.",
    ),
    "diversifie": CategoryStrategy(
        code="diversifie",
        label="Diversifié",
        objective="max_sharpe",
        asset_bounds={"actions": (0.10, 0.90), "obligataire": (0.10, 0.90), "monetaire": (0.0, 0.50)},
        description="Allocation tactique entre actions et taux, sous bornes par classe.",
    ),
    "omlt": CategoryStrategy(
        code="omlt",
        label="Obligations Moyen/Long Terme",
        objective="target_return",
        asset_bounds={"actions": (0.0, 0.10), "obligataire": (0.70, 1.00), "monetaire": (0.0, 0.30)},
        sensitivity_bounds=(1.0, 12.0),
        description="Obligataire long ; sensibilité élevée, pilotée par une cible.",
    ),
    "oct": CategoryStrategy(
        code="oct",
        label="Obligations Court Terme",
        objective="min_variance",
        asset_bounds={"actions": (0.0, 0.05), "obligataire": (0.50, 1.00), "monetaire": (0.0, 0.50)},
        sensitivity_bounds=(0.0, 2.0),
        description="Obligataire court ; sensibilité bornée, portage et roll-down.",
    ),
    "monetaire": CategoryStrategy(
        code="monetaire",
        label="Monétaire",
        objective="min_variance",
        asset_bounds={"actions": (0.0, 0.0), "obligataire": (0.0, 0.30), "monetaire": (0.70, 1.00)},
        sensitivity_bounds=(0.0, 0.5),
        description="Titres courts (< 1 an) ; sensibilité très faible, échelle de maturités.",
    ),
}


def get_strategy(code: str) -> CategoryStrategy:
    """Retourne la stratégie d'une catégorie (par code).

    Raises:
        KeyError: si la catégorie est inconnue.
    """
    key = code.strip().lower()
    if key not in STRATEGIES:
        raise KeyError(f"Catégorie OPCVM inconnue : {code!r}. Choix : {list(STRATEGIES)}")
    return STRATEGIES[key]


def group_bounds_from_strategy(
    strategy: CategoryStrategy, asset_class: Dict[str, str]
) -> Dict[str, tuple]:
    """Traduit les bornes de classe d'actifs en contraintes de groupe pour l'optimiseur.

    Args:
        strategy: Stratégie de catégorie.
        asset_class: Association ``ticker -> classe`` ("actions"/"obligataire"/"monetaire").

    Returns:
        Un mapping ``classe -> (tickers, min, max)`` exploitable par
        :func:`kanyon.portfolio.construction.optimize`.
    """
    groups: Dict[str, tuple] = {}
    for cls, (gmin, gmax) in strategy.asset_bounds.items():
        members = [t for t, c in asset_class.items() if c == cls]
        if members:
            groups[cls] = (members, gmin, gmax)
    return groups
