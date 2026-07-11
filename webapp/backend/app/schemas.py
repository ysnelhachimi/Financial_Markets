"""Schémas Pydantic (validation entrée/sortie de l'API)."""
from __future__ import annotations

import datetime as dt
from typing import Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field


# --- Auth / utilisateurs ---
class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    full_name: Optional[str] = None


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    email: EmailStr
    full_name: Optional[str] = None
    is_admin: bool


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


# --- Plans ---
class PlanOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    code: str
    name: str
    description: Optional[str] = None
    price_cents: int
    currency: str
    daily_quota: Optional[int] = None


# --- Abonnements ---
class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: str
    current_period_end: dt.datetime
    cancel_at_period_end: bool
    plan: PlanOut


class SubscribeRequest(BaseModel):
    plan_code: str


class CheckoutSession(BaseModel):
    """Réponse d'initiation de paiement : formulaire à poster vers le prestataire."""

    payment_url: str
    fields: dict[str, str]
    provider_ref: str
