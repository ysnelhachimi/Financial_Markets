"""Conformité prudentielle des portefeuilles OPCVM (M8).

Contrôle automatique des ratios réglementaires à partir des positions :

* **division des risques** : plafond par émetteur et règle 5 %/40 % ;
* **emprise** : part maximale détenue d'une même émission/émetteur ;
* **sensibilité** : bornes de sensibilité globale selon la catégorie ;
* **liquidité** : plafond d'actifs illiquides.

Les seuils sont **paramétrables** (:class:`ComplianceLimits`) et **indicatifs** :
ils doivent être calés sur les circulaires AMMC en vigueur. La plateforme calcule
et alerte ; la responsabilité réglementaire reste au client.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class Holding:
    """Une ligne de portefeuille."""

    ticker: str
    issuer: str
    weight: float                 # poids dans le portefeuille (fraction)
    asset_class: str = "obligataire"
    sensitivity: float = 0.0      # sensibilité de la ligne
    emprise: float = 0.0          # part détenue de l'émission (fraction)
    liquid: bool = True
    issuer_group: Optional[str] = None  # groupe émetteur (agrégation)


@dataclass
class ComplianceLimits:
    """Seuils prudentiels (indicatifs, à valider avec l'AMMC)."""

    max_per_issuer: float = 0.10          # plafond par émetteur
    concentration_threshold: float = 0.05  # seuil "grosse ligne" (règle 5/40)
    concentration_cap: float = 0.40        # plafond du cumul des grosses lignes
    max_emprise: float = 0.10             # emprise maximale par émission
    max_illiquid: float = 0.10            # part maximale d'actifs illiquides
    sensitivity_bounds: Optional[Tuple[float, float]] = None


@dataclass
class Check:
    """Résultat d'un contrôle unitaire."""

    name: str
    status: str          # "ok" | "breach"
    value: float
    limit: float
    detail: str = ""


@dataclass
class ComplianceReport:
    """Rapport de conformité d'un portefeuille."""

    compliant: bool
    checks: List[Check] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "compliant": self.compliant,
            "checks": [
                {
                    "name": c.name,
                    "status": c.status,
                    "value": round(c.value, 6),
                    "limit": round(c.limit, 6),
                    "detail": c.detail,
                }
                for c in self.checks
            ],
        }


def _issuer_key(h: Holding) -> str:
    return h.issuer_group or h.issuer


def check_compliance(holdings: Sequence[Holding], limits: ComplianceLimits) -> ComplianceReport:
    """Contrôle les ratios prudentiels d'un portefeuille.

    Args:
        holdings: Positions du portefeuille.
        limits: Seuils applicables.

    Returns:
        Un :class:`ComplianceReport` (``compliant`` = aucun dépassement).
    """
    checks: List[Check] = []

    # 1) Division des risques : plafond par émetteur (agrégé par groupe).
    by_issuer: Dict[str, float] = defaultdict(float)
    for h in holdings:
        by_issuer[_issuer_key(h)] += h.weight
    if by_issuer:
        worst_issuer, worst_weight = max(by_issuer.items(), key=lambda kv: kv[1])
        checks.append(
            Check(
                name="Plafond par émetteur",
                status="ok" if worst_weight <= limits.max_per_issuer + 1e-9 else "breach",
                value=worst_weight,
                limit=limits.max_per_issuer,
                detail=f"Émetteur le plus concentré : {worst_issuer}",
            )
        )

    # 2) Règle 5 %/40 % : cumul des lignes (par émetteur) dépassant le seuil.
    big = sum(w for w in by_issuer.values() if w > limits.concentration_threshold)
    checks.append(
        Check(
            name=f"Cumul des lignes > {limits.concentration_threshold:.0%}",
            status="ok" if big <= limits.concentration_cap + 1e-9 else "breach",
            value=big,
            limit=limits.concentration_cap,
        )
    )

    # 3) Emprise maximale par émission.
    if holdings:
        worst = max(holdings, key=lambda h: h.emprise)
        checks.append(
            Check(
                name="Emprise maximale",
                status="ok" if worst.emprise <= limits.max_emprise + 1e-9 else "breach",
                value=worst.emprise,
                limit=limits.max_emprise,
                detail=f"Ligne : {worst.ticker}",
            )
        )

    # 4) Liquidité : part d'actifs illiquides.
    illiquid = sum(h.weight for h in holdings if not h.liquid)
    checks.append(
        Check(
            name="Actifs illiquides",
            status="ok" if illiquid <= limits.max_illiquid + 1e-9 else "breach",
            value=illiquid,
            limit=limits.max_illiquid,
        )
    )

    # 5) Sensibilité globale (si bornes fournies).
    if limits.sensitivity_bounds is not None:
        lo, hi = limits.sensitivity_bounds
        port_sens = sum(h.weight * h.sensitivity for h in holdings)
        within = lo - 1e-9 <= port_sens <= hi + 1e-9
        checks.append(
            Check(
                name="Sensibilité globale",
                status="ok" if within else "breach",
                value=port_sens,
                limit=hi,
                detail=f"Bornes [{lo}, {hi}]",
            )
        )

    compliant = all(c.status == "ok" for c in checks)
    return ComplianceReport(compliant=compliant, checks=checks)
