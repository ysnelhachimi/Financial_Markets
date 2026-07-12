"""Consultation des plans et de l'abonnement courant."""
from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user
from app.models import Plan, Subscription, User
from app.schemas import PlanOut, SubscriptionOut

router = APIRouter(prefix="/api", tags=["plans"])


@router.get("/plans", response_model=list[PlanOut])
def list_plans(db: Session = Depends(get_db)) -> list[Plan]:
    """Liste publique des plans actifs (page de tarification)."""
    return list(db.scalars(select(Plan).where(Plan.is_active).order_by(Plan.price_cents)))


@router.get("/subscription", response_model=SubscriptionOut | None)
def current_subscription(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Subscription | None:
    """Abonnement en cours de l'utilisateur (ou ``null``)."""
    now = dt.datetime.now(dt.timezone.utc)
    subs = db.scalars(select(Subscription).where(Subscription.user_id == user.id)).all()
    # Priorité à un abonnement valide ; sinon le plus récent.
    current = next((s for s in subs if s.is_current(now)), None)
    if current is not None:
        return current
    return max(subs, key=lambda s: s.created_at, default=None)
