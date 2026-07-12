"""Prestataire CMI / PayZone (Maroc).

CMI s'appuie sur la plateforme « est3Dgate » : le paiement se fait par
redirection (POST d'un formulaire signé), puis CMI rappelle nos URLs de retour
avec un ``HASH`` à vérifier.

Signature (HASH ver3) : on trie les paramètres postés (hors ``HASH`` et
``encoding``) par nom (insensible à la casse), on échappe chaque valeur
(``\\`` -> ``\\\\``, ``|`` -> ``\\|``), on les joint par ``|``, on ajoute la
clé marchande (``storekey``) échappée, puis SHA-512 encodé en base64.

Références de configuration : ``KWEB_CMI_CLIENT_ID``, ``KWEB_CMI_STORE_KEY``,
``KWEB_CMI_GATEWAY_URL``.
"""
from __future__ import annotations

import base64
import hashlib
import secrets
from typing import Mapping

from app.config import get_settings
from app.payments.base import CheckoutRequest, CheckoutResult, PaymentProvider

# Code ISO 4217 numérique du dirham marocain.
CURRENCY_MAD = "504"


def _escape(value: str) -> str:
    """Échappe une valeur selon la spécification de hachage CMI."""
    return str(value).replace("\\", "\\\\").replace("|", "\\|")


def compute_hash(params: Mapping[str, str], store_key: str) -> str:
    """Calcule le HASH ver3 CMI pour un ensemble de paramètres."""
    keys = sorted((k for k in params if k.lower() not in ("hash", "encoding")), key=str.lower)
    plaintext = "|".join(_escape(params[k]) for k in keys)
    plaintext += "|" + _escape(store_key)
    digest = hashlib.sha512(plaintext.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


class CmiProvider(PaymentProvider):
    """Implémentation du prestataire CMI."""

    name = "cmi"

    def __init__(self) -> None:
        self.settings = get_settings()

    def create_checkout(self, request: CheckoutRequest) -> CheckoutResult:
        s = self.settings
        # Montant CMI : décimal à 2 chiffres (ex. "199.00").
        amount = f"{request.amount_cents / 100:.2f}"
        currency = CURRENCY_MAD if request.currency.upper() == "MAD" else request.currency

        fields: dict[str, str] = {
            "clientid": s.cmi_client_id,
            "oid": request.order_id,
            "amount": amount,
            "currency": currency,
            "okUrl": request.return_ok_url,
            "failUrl": request.return_fail_url,
            "callbackUrl": f"{s.public_base_url}/api/billing/cmi/callback",
            "shopurl": s.frontend_base_url,
            "email": request.email,
            "TranType": "Auth",
            "storetype": "3D_PAY_HOSTING",
            "hashAlgorithm": "ver3",
            "lang": "fr",
            "encoding": "UTF-8",
            "rnd": secrets.token_hex(16),
        }
        fields["HASH"] = compute_hash(fields, s.cmi_store_key)
        return CheckoutResult(payment_url=s.cmi_gateway_url, fields=fields)

    def verify_callback(self, payload: Mapping[str, str]) -> bool:
        received = payload.get("HASH") or payload.get("hash")
        if not received:
            return False
        expected = compute_hash(payload, self.settings.cmi_store_key)
        # Comparaison en temps constant.
        if not secrets.compare_digest(received, expected):
            return False
        # Paiement approuvé : ProcReturnCode "00" et/ou Response "Approved".
        proc = payload.get("ProcReturnCode")
        response = (payload.get("Response") or "").lower()
        return proc == "00" or response == "approved"

    def order_id_from_callback(self, payload: Mapping[str, str]) -> str:
        return payload.get("oid", "")
