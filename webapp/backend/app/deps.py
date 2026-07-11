"""Dépendances FastAPI : utilisateur courant et contrôle d'abonnement."""
from __future__ import annotations

import datetime as dt
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Subscription, User
from app.security import decode_access_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    """Retourne l'utilisateur authentifié à partir du jeton JWT."""
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Identifiants invalides",
        headers={"WWW-Authenticate": "Bearer"},
    )
    subject = decode_access_token(token)
    if subject is None:
        raise credentials_exc
    user = db.get(User, int(subject))
    if user is None or not user.is_active:
        raise credentials_exc
    return user


def get_current_admin(user: User = Depends(get_current_user)) -> User:
    """Exige un utilisateur administrateur."""
    if not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Accès administrateur requis")
    return user


def active_subscription(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Subscription:
    """Exige un abonnement en cours (essai ou payé) — mur payant.

    Raises:
        HTTPException 402: si aucun abonnement valide n'est trouvé.
    """
    now = dt.datetime.now(dt.timezone.utc)
    subs = db.scalars(
        select(Subscription).where(Subscription.user_id == user.id)
    ).all()
    current = next((s for s in subs if s.is_current(now)), None)
    if current is None:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Abonnement requis pour accéder à cette ressource.",
        )
    return current


def optional_user(
    db: Session = Depends(get_db),
    token: Optional[str] = Depends(OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)),
) -> Optional[User]:
    """Retourne l'utilisateur si un jeton valide est fourni, sinon ``None``."""
    if not token:
        return None
    subject = decode_access_token(token)
    if subject is None:
        return None
    return db.get(User, int(subject))
