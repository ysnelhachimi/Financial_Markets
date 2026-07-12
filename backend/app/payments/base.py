"""Interface commune des prestataires de paiement."""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Mapping


@dataclass
class CheckoutRequest:
    """Données nécessaires pour initier un paiement."""

    order_id: str
    amount_cents: int
    currency: str
    email: str
    # URLs de retour (succès / échec) côté frontend.
    return_ok_url: str
    return_fail_url: str


@dataclass
class CheckoutResult:
    """Résultat d'initiation : formulaire à soumettre au prestataire."""

    payment_url: str
    fields: Mapping[str, str] = field(default_factory=dict)


class PaymentProvider(abc.ABC):
    """Contrat que doit respecter tout prestataire de paiement."""

    name: str = "base"

    @abc.abstractmethod
    def create_checkout(self, request: CheckoutRequest) -> CheckoutResult:
        """Prépare une transaction et retourne le formulaire de redirection."""

    @abc.abstractmethod
    def verify_callback(self, payload: Mapping[str, str]) -> bool:
        """Vérifie l'authenticité et le succès d'un callback prestataire.

        Args:
            payload: Paramètres reçus du prestataire (form-urlencoded).

        Returns:
            ``True`` si la signature est valide et le paiement approuvé.
        """

    @abc.abstractmethod
    def order_id_from_callback(self, payload: Mapping[str, str]) -> str:
        """Extrait la référence de commande (order_id) d'un callback."""
