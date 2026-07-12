"""Authentification : inscription, connexion, profil.

À l'inscription, un abonnement d'essai (plan ``free``) est ouvert pour
``KWEB_TRIAL_DAYS`` jours afin de permettre la découverte du produit.
"""
from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.deps import get_current_user
from app.models import Plan, Subscription, SubscriptionStatus, User
from app.schemas import Token, UserCreate, UserOut
from app.security import create_access_token, hash_password, verify_password

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register(payload: UserCreate, db: Session = Depends(get_db)) -> User:
    """Crée un compte et ouvre un essai gratuit."""
    existing = db.scalar(select(User).where(User.email == payload.email))
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email déjà utilisé.")

    user = User(
        email=payload.email,
        full_name=payload.full_name,
        hashed_password=hash_password(payload.password),
    )
    db.add(user)
    db.flush()  # obtient user.id

    free_plan = db.scalar(select(Plan).where(Plan.code == "free"))
    if free_plan is not None:
        trial_days = get_settings().trial_days
        db.add(
            Subscription(
                user_id=user.id,
                plan_id=free_plan.id,
                status=SubscriptionStatus.trialing,
                current_period_end=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=trial_days),
            )
        )
    db.commit()
    db.refresh(user)
    return user


@router.post("/login", response_model=Token)
def login(
    form: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db),
) -> Token:
    """Authentifie (username = email) et retourne un jeton JWT."""
    user = db.scalar(select(User).where(User.email == form.username))
    if user is None or not verify_password(form.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email ou mot de passe incorrect.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return Token(access_token=create_access_token(subject=user.id))


@router.get("/me", response_model=UserOut)
def me(user: User = Depends(get_current_user)) -> User:
    """Retourne le profil de l'utilisateur authentifié."""
    return user
