"""Sécurité : hachage des mots de passe et jetons JWT."""
from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import secrets
from typing import Any, Optional

import jwt

from app.config import get_settings

settings = get_settings()

# Hachage des mots de passe : PBKDF2-HMAC-SHA256 (bibliothèque standard).
# Format stocké : ``pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>``.
_PBKDF2_ALGO = "pbkdf2_sha256"
_PBKDF2_ITERATIONS = 260_000
_SALT_BYTES = 16


def hash_password(password: str) -> str:
    """Hache un mot de passe en clair (PBKDF2-SHA256, sel aléatoire)."""
    salt = secrets.token_bytes(_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS)
    return f"{_PBKDF2_ALGO}${_PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(plain: str, hashed: str) -> bool:
    """Vérifie un mot de passe en clair contre son hachage (temps constant)."""
    try:
        algo, iterations, salt_hex, digest_hex = hashed.split("$")
        if algo != _PBKDF2_ALGO:
            return False
        expected = bytes.fromhex(digest_hex)
        computed = hashlib.pbkdf2_hmac(
            "sha256", plain.encode("utf-8"), bytes.fromhex(salt_hex), int(iterations)
        )
    except (ValueError, AttributeError):
        return False
    return hmac.compare_digest(computed, expected)


def create_access_token(subject: str, expires_minutes: Optional[int] = None) -> str:
    """Crée un jeton JWT signé pour un sujet donné (identifiant utilisateur)."""
    expire = dt.datetime.now(dt.timezone.utc) + dt.timedelta(
        minutes=expires_minutes or settings.access_token_expire_minutes
    )
    payload: dict[str, Any] = {"sub": str(subject), "exp": expire}
    return jwt.encode(payload, settings.secret_key, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> Optional[str]:
    """Décode un jeton et retourne le sujet, ou ``None`` si invalide/expiré."""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.jwt_algorithm])
        return payload.get("sub")
    except jwt.PyJWTError:
        return None
