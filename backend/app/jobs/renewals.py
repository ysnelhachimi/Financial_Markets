"""Cycle de vie des abonnements : expiration et renouvellement.

À exécuter périodiquement (cron / worker) :

    python -m app.jobs.renewals

Règles appliquées aux abonnements dont la période est échue :

* ``cancel_at_period_end`` → passage en ``canceled`` ;
* plan gratuit → renouvellement automatique (nouvelle période) ;
* plan payant → passage en ``past_due`` (en attente d'un paiement de
  renouvellement — à déclencher via CMI si la tokenisation est activée).

La fonction :func:`run_renewals` prend ``now`` en paramètre : elle est donc
déterministe et testable.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Subscription, SubscriptionStatus

logger = logging.getLogger(__name__)

PERIOD = dt.timedelta(days=30)


def run_renewals(session: Session, now: Optional[dt.datetime] = None) -> Dict[str, int]:
    """Traite les abonnements échus et retourne un récapitulatif.

    Args:
        session: Session SQLAlchemy.
        now: Instant de référence (par défaut, l'heure UTC courante).

    Returns:
        Un dictionnaire ``{renewed_free, past_due, canceled}``.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    summary = {"renewed_free": 0, "past_due": 0, "canceled": 0}

    subs = session.scalars(select(Subscription)).all()
    for sub in subs:
        end = sub.current_period_end
        if end.tzinfo is None:
            end = end.replace(tzinfo=dt.timezone.utc)
        # On ne traite que les périodes échues, encore actives ou en essai.
        if end > now or sub.status not in (SubscriptionStatus.trialing, SubscriptionStatus.active):
            continue

        if sub.cancel_at_period_end:
            sub.status = SubscriptionStatus.canceled
            summary["canceled"] += 1
        elif sub.plan.price_cents == 0:
            sub.status = SubscriptionStatus.active
            sub.current_period_end = now + PERIOD
            summary["renewed_free"] += 1
        else:
            sub.status = SubscriptionStatus.past_due
            summary["past_due"] += 1

    session.commit()
    logger.info(
        "Renouvellements : %d gratuits renouvelés, %d en attente de paiement, %d annulés.",
        summary["renewed_free"], summary["past_due"], summary["canceled"],
    )
    return summary


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    with SessionLocal() as session:
        run_renewals(session)


if __name__ == "__main__":  # pragma: no cover
    main()
