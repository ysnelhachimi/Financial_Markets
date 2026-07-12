# Architecture

## Vue d'ensemble

```
Navigateur ──► Frontend React (nginx) ──/api──► Backend FastAPI ──► PostgreSQL
                                                     │
                                                     ├─► package kanyon (données + pricer)
                                                     └─► Prestataire paiement (CMI / PayZone)
```

## Composants

### `packages/kanyon` — moteur de calcul (Python)
- `db/` : modèles ORM (SQLAlchemy 2.0), sessions (moteur paresseux configurable),
  requêtes renvoyant des `DataFrame`.
- `helpers/` : parsing dates/nombres, `add_years`.
- `imports/` : extraction des données (Bourse de Casablanca, BKAM) — Selenium/HTTP.
- `analytics/` : ratios de gestion (Sharpe, Treynor, VaR, CVaR).
- `pricer/` : **cœur métier**.
  - `curve.py` : interpolation de courbe (fonctions pures, données injectées).
  - `dates.py`, `coupons.py` : tenors, dates de coupon, coupon couru.
  - `bonds.py` : typage des titres, `price_fixed_bond`.
  - `service.py` : orchestration (lecture base → calcul).

Le cœur mathématique est **pur** (pas d'accès base), donc unitairement testable ;
seule la couche `service` touche la base.

### `backend` — API (FastAPI)
- `routers/auth.py` : inscription, connexion (JWT HS256), profil.
- `routers/plans.py` : formules et abonnement courant.
- `routers/billing.py` : souscription, callback paiement, annulation.
- `routers/market.py` : données de marché (protégées).
- `routers/pricer.py` : valorisation obligataire (protégée).
- `payments/` : abstraction `PaymentProvider` + `CmiProvider` + `FakeProvider`.
- `deps.py` : `get_current_user`, `active_subscription` (mur payant → HTTP 402).

### `frontend` — SPA (React + Vite)
- Pages : tarifs, inscription/connexion, tableau de bord (données), pricer,
  portail d'abonnement, retours de paiement.
- `context/AuthContext` : état d'authentification (JWT en `localStorage`).
- Servi par nginx en production, qui proxifie `/api` vers le backend.

## Modèle de données (backend)

- `User` — comptes.
- `Plan` — formules (prix mensuel en centimes de MAD, quota).
- `Subscription` — abonnement d'un utilisateur (statut, fin de période).
- `Payment` — transactions liées à un abonnement.

Les tables applicatives et les tables de données de marché (`kanyon`) peuvent
cohabiter dans la même base PostgreSQL.

## Sécurité

- Mots de passe : PBKDF2-HMAC-SHA256 (bibliothèque standard).
- Jetons : JWT HS256 (PyJWT), clé `KWEB_SECRET_KEY`.
- Paiement CMI : signature HASH ver3 (SHA-512/base64) vérifiée sur les callbacks.
- Toute configuration sensible via variables d'environnement (aucun secret en dur).
