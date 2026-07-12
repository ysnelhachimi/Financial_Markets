"""Tests du job de renouvellement des abonnements."""
import datetime as dt


def _make_sub(db, price_cents, days_offset, status, cancel=False):
    from app.models import Plan, Subscription, SubscriptionStatus, User
    from app.security import hash_password

    suffix = f"{price_cents}-{days_offset}-{cancel}"
    user = User(email=f"u{suffix}@x.com", hashed_password=hash_password("password123"))
    db.add(user)
    db.flush()
    plan = Plan(code=f"p{suffix}", name="P", price_cents=price_cents, currency="MAD")
    db.add(plan)
    db.flush()
    sub = Subscription(
        user_id=user.id,
        plan_id=plan.id,
        status=status,
        current_period_end=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=days_offset),
        cancel_at_period_end=cancel,
    )
    db.add(sub)
    db.flush()
    return sub


def test_run_renewals(client):
    from app.database import SessionLocal
    from app.jobs.renewals import run_renewals
    from app.models import SubscriptionStatus

    with SessionLocal() as db:
        # Échu, gratuit, non annulé -> renouvelé.
        s_free = _make_sub(db, 0, -1, SubscriptionStatus.active)
        # Échu, payant, non annulé -> past_due.
        s_paid = _make_sub(db, 19900, -1, SubscriptionStatus.active)
        # Échu mais annulé -> canceled.
        s_cancel = _make_sub(db, 19900, -1, SubscriptionStatus.active, cancel=True)
        # Non échu -> inchangé.
        s_future = _make_sub(db, 19900, 10, SubscriptionStatus.active)
        db.commit()
        free_id, paid_id, cancel_id, future_id = s_free.id, s_paid.id, s_cancel.id, s_future.id

        summary = run_renewals(db, now=dt.datetime.now(dt.timezone.utc))
        assert summary == {"renewed_free": 1, "past_due": 1, "canceled": 1}

        from app.models import Subscription
        assert db.get(Subscription, free_id).status == SubscriptionStatus.active
        assert db.get(Subscription, paid_id).status == SubscriptionStatus.past_due
        assert db.get(Subscription, cancel_id).status == SubscriptionStatus.canceled
        assert db.get(Subscription, future_id).status == SubscriptionStatus.active
        # Le gratuit renouvelé a une nouvelle période dans le futur.
        end = db.get(Subscription, free_id).current_period_end
        if end.tzinfo is None:
            end = end.replace(tzinfo=dt.timezone.utc)
        assert end > dt.datetime.now(dt.timezone.utc)
