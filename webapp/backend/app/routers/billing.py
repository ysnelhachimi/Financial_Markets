"""Parcours d'abonnement et de paiement.

Flux nominal (plan payant) :
    1. ``POST /api/billing/subscribe`` crée un abonnement en attente et un
       paiement ``pending``, puis renvoie le formulaire à poster au prestataire.
    2. Le frontend poste ce formulaire vers le prestataire (redirection CMI).
    3. Le prestataire rappelle ``/api/billing/cmi/callback`` : on vérifie la
       signature, on marque le paiement ``succeeded`` et on active/prolonge
       l'abonnement d'un mois.

Le plan ``free`` est activé immédiatement, sans paiement.
"""
from __future__ import annotations

import datetime as dt
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.deps import get_current_user
from app.models import (
    Payment,
    PaymentStatus,
    Plan,
    Subscription,
    SubscriptionStatus,
    User,
)
from app.payments import get_payment_provider
from app.payments.base import CheckoutRequest
from app.schemas import CheckoutSession, SubscribeRequest

router = APIRouter(prefix="/api/billing", tags=["billing"])

# Durée d'une période d'abonnement mensuelle.
PERIOD = dt.timedelta(days=30)


def _get_or_create_subscription(db: Session, user: User, plan: Plan) -> Subscription:
    """Retourne l'abonnement de l'utilisateur (créé si absent), lié au plan."""
    sub = db.scalar(
        select(Subscription).where(Subscription.user_id == user.id).order_by(Subscription.created_at.desc())
    )
    now = dt.datetime.now(dt.timezone.utc)
    if sub is None:
        sub = Subscription(
            user_id=user.id,
            plan_id=plan.id,
            status=SubscriptionStatus.past_due,
            current_period_end=now,
        )
        db.add(sub)
        db.flush()
    else:
        sub.plan_id = plan.id
    return sub


@router.post("/subscribe", response_model=CheckoutSession | None)
def subscribe(
    payload: SubscribeRequest,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Initie un abonnement à un plan.

    Pour le plan gratuit, active immédiatement. Pour un plan payant, crée un
    paiement en attente et renvoie le formulaire de redirection prestataire.
    """
    settings = get_settings()
    plan = db.scalar(select(Plan).where(Plan.code == payload.plan_code, Plan.is_active))
    if plan is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan introuvable.")

    sub = _get_or_create_subscription(db, user, plan)

    # Plan gratuit : activation immédiate, aucun paiement.
    if plan.price_cents == 0:
        sub.status = SubscriptionStatus.active
        sub.current_period_end = dt.datetime.now(dt.timezone.utc) + PERIOD
        db.commit()
        return None

    order_id = "KW-" + secrets.token_hex(12)
    payment = Payment(
        subscription_id=sub.id,
        amount_cents=plan.price_cents,
        currency=plan.currency,
        status=PaymentStatus.pending,
        provider=settings.payment_provider,
        provider_ref=order_id,
    )
    db.add(payment)
    db.commit()

    provider = get_payment_provider()
    result = provider.create_checkout(
        CheckoutRequest(
            order_id=order_id,
            amount_cents=plan.price_cents,
            currency=plan.currency,
            email=user.email,
            return_ok_url=f"{settings.frontend_base_url}/billing/success",
            return_fail_url=f"{settings.frontend_base_url}/billing/failure",
        )
    )
    return CheckoutSession(payment_url=result.payment_url, fields=dict(result.fields), provider_ref=order_id)


def _activate_from_payment(db: Session, order_id: str) -> bool:
    """Marque le paiement réussi et prolonge l'abonnement d'un mois."""
    payment = db.scalar(select(Payment).where(Payment.provider_ref == order_id))
    if payment is None:
        return False
    if payment.status == PaymentStatus.succeeded:
        return True  # idempotent (callback rejoué)

    payment.status = PaymentStatus.succeeded
    sub = db.get(Subscription, payment.subscription_id)
    now = dt.datetime.now(dt.timezone.utc)
    base = sub.current_period_end
    if base.tzinfo is None:
        base = base.replace(tzinfo=dt.timezone.utc)
    start = max(base, now)  # prolonge sans perdre les jours restants
    sub.status = SubscriptionStatus.active
    sub.current_period_end = start + PERIOD
    db.commit()
    return True


@router.post("/cmi/callback")
async def cmi_callback(request: Request, db: Session = Depends(get_db)):
    """Callback serveur-à-serveur du prestataire CMI (form-urlencoded)."""
    form = dict((await request.form()).items())
    provider = get_payment_provider()
    if not provider.verify_callback(form):
        # CMI attend la chaîne "FAILURE" en cas de rejet.
        return HTMLResponse("FAILURE", media_type="text/plain")
    order_id = provider.order_id_from_callback(form)
    _activate_from_payment(db, order_id)
    # CMI attend "ACTION=POSTAUTH" / "APPROVED" — on renvoie "ACTION=POSTAUTH".
    return HTMLResponse("ACTION=POSTAUTH", media_type="text/plain")


@router.post("/cancel", response_model=None)
def cancel(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Annule le renouvellement à la fin de la période courante."""
    sub = db.scalar(
        select(Subscription).where(Subscription.user_id == user.id).order_by(Subscription.created_at.desc())
    )
    if sub is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Aucun abonnement.")
    sub.cancel_at_period_end = True
    db.commit()
    return {"detail": "Renouvellement annulé ; accès maintenu jusqu'à la fin de la période."}


# --- Prestataire factice (développement uniquement) ---
@router.get("/fake/pay", response_class=HTMLResponse)
def fake_pay_page(oid: str, amount: str = "") -> str:
    """Page de simulation de paiement (provider factice)."""
    return f"""
    <html><body style="font-family:sans-serif;max-width:480px;margin:4rem auto">
      <h2>Paiement simulé</h2>
      <p>Commande <code>{oid}</code> — montant {amount} MAD</p>
      <form method="post" action="/api/billing/fake/callback">
        <input type="hidden" name="oid" value="{oid}"/>
        <input type="hidden" name="status" value="success"/>
        <button type="submit" style="padding:.6rem 1rem">Payer</button>
      </form>
    </body></html>
    """


@router.post("/fake/callback")
async def fake_callback(request: Request, db: Session = Depends(get_db)):
    """Callback du provider factice : active l'abonnement puis redirige."""
    form = dict((await request.form()).items())
    provider = get_payment_provider()
    settings = get_settings()
    if provider.verify_callback(form):
        _activate_from_payment(db, provider.order_id_from_callback(form))
        return RedirectResponse(f"{settings.frontend_base_url}/billing/success", status_code=303)
    return RedirectResponse(f"{settings.frontend_base_url}/billing/failure", status_code=303)
