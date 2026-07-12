"""Configuration du backend (variables d'environnement, aucun secret en dur).

Toutes les valeurs sensibles (clé JWT, identifiants CMI) sont lues depuis
l'environnement. Voir ``webapp/backend/.env.example``.
"""
from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Paramètres applicatifs chargés depuis l'environnement / un fichier .env."""

    model_config = SettingsConfigDict(env_prefix="KWEB_", env_file=".env", extra="ignore")

    # Application
    app_name: str = "Kanyon Markets"
    environment: str = "development"
    debug: bool = True

    # Base de données (par défaut SQLite local pour démarrer sans serveur).
    database_url: str = "sqlite:///./kanyon_web.db"

    # Sécurité / JWT
    secret_key: str = "change-me-in-production"  # OBLIGATOIRE à surcharger en prod
    access_token_expire_minutes: int = 60 * 24  # 24 h
    jwt_algorithm: str = "HS256"

    # CORS (origines du frontend autorisées)
    cors_origins: List[str] = Field(default_factory=lambda: ["http://localhost:5173"])

    # Essai gratuit (jours) offert à l'inscription sur le plan d'essai.
    trial_days: int = 14

    # --- Paiement CMI / PayZone ---
    payment_provider: str = "cmi"  # "cmi" | "fake"
    cmi_store_key: str = ""        # clé secrète marchand (storekey)
    cmi_client_id: str = ""        # identifiant marchand (clientid)
    cmi_gateway_url: str = "https://payment.cmi.co.ma/fim/est3Dgate"
    # URL publique du backend, utilisée pour les callbacks CMI.
    public_base_url: str = "http://localhost:8000"
    # URL du frontend vers laquelle rediriger après paiement.
    frontend_base_url: str = "http://localhost:5173"


@lru_cache
def get_settings() -> Settings:
    """Retourne les paramètres (mémoïsés)."""
    return Settings()
