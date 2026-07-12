"""Abstraction des prestataires de paiement.

L'interface :class:`~app.payments.base.PaymentProvider` découple la logique
métier du prestataire concret (CMI/PayZone en production, un provider factice en
test). Un autre prestataire (Stripe...) peut être ajouté sans toucher aux
routes.
"""
from __future__ import annotations

from app.config import get_settings
from app.payments.base import PaymentProvider
from app.payments.cmi import CmiProvider
from app.payments.fake import FakeProvider


def get_payment_provider() -> PaymentProvider:
    """Retourne l'instance de provider selon la configuration."""
    settings = get_settings()
    if settings.payment_provider == "fake":
        return FakeProvider()
    return CmiProvider()


__all__ = ["PaymentProvider", "CmiProvider", "FakeProvider", "get_payment_provider"]
