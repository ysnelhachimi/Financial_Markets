"""Prestataire de paiement factice (développement et tests).

Ne contacte aucun service externe : il « approuve » tout paiement dont le
callback contient ``status=success``. Utile pour dérouler le parcours
d'abonnement de bout en bout sans identifiants CMI.
"""
from __future__ import annotations

from typing import Mapping

from app.config import get_settings
from app.payments.base import CheckoutRequest, CheckoutResult, PaymentProvider


class FakeProvider(PaymentProvider):
    """Prestataire simulé, pratique en local et en CI."""

    name = "fake"

    def create_checkout(self, request: CheckoutRequest) -> CheckoutResult:
        settings = get_settings()
        # Page de simulation servie par le backend lui-même.
        url = f"{settings.public_base_url}/api/billing/fake/pay"
        return CheckoutResult(
            payment_url=url,
            fields={"oid": request.order_id, "amount": f"{request.amount_cents / 100:.2f}"},
        )

    def verify_callback(self, payload: Mapping[str, str]) -> bool:
        return payload.get("status") == "success"

    def order_id_from_callback(self, payload: Mapping[str, str]) -> str:
        return payload.get("oid", "")
