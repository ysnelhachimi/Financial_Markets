"""Back-office administrateur (réservé aux comptes ``is_admin``).

Fournit une vue synthétique des utilisateurs, abonnements et paiements pour le
pilotage commercial du produit.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_admin
from app.models import Payment, PaymentStatus, Subscription, SubscriptionStatus, User

router = APIRouter(prefix="/api/admin", tags=["admin"], dependencies=[Depends(get_current_admin)])


@router.get("/stats")
def stats(db: Session = Depends(get_db)) -> dict:
    """Indicateurs clés : comptes, abonnements actifs, revenu mensuel récurrent."""
    users = db.scalar(select(func.count()).select_from(User)) or 0
    active = db.scalar(
        select(func.count()).select_from(Subscription).where(
            Subscription.status.in_([SubscriptionStatus.active, SubscriptionStatus.trialing])
        )
    ) or 0
    # MRR : somme des prix des plans des abonnements payants actifs (centimes).
    mrr_cents = db.scalar(
        select(func.coalesce(func.sum(Payment.amount_cents), 0)).where(
            Payment.status == PaymentStatus.succeeded
        )
    ) or 0
    return {
        "users": int(users),
        "active_subscriptions": int(active),
        "revenue_collected_cents": int(mrr_cents),
        "currency": "MAD",
    }


@router.get("/users")
def list_users(db: Session = Depends(get_db)) -> list[dict]:
    """Liste des utilisateurs avec le statut de leur abonnement le plus récent."""
    users = db.scalars(select(User).order_by(User.created_at.desc())).all()
    result = []
    for user in users:
        sub = max(user.subscriptions, key=lambda s: s.created_at, default=None)
        result.append(
            {
                "id": user.id,
                "email": user.email,
                "full_name": user.full_name,
                "is_admin": user.is_admin,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "subscription_status": sub.status.value if sub else None,
                "plan": sub.plan.code if sub else None,
            }
        )
    return result
