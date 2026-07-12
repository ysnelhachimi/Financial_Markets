# Kanyon Markets

**Plateforme SaaS de données et de pricing obligataire des marchés financiers
marocains.** Données MASI, courbe des taux BKAM et **pricer obligataire**
professionnel, accessibles par abonnement mensuel (paiement CMI / PayZone en
dirham).

> Monorepo prêt à déployer : package de calcul `kanyon`, API FastAPI, frontend
> React et stack Docker complète.

---

## Proposition de valeur

- **Données de marché** : indices MASI et sectoriels, volumes, composition,
  marché monétaire (MONIA, TMP, taux directeur), courbe des taux.
- **Pricer obligataire** : interpolation de la courbe BKAM (conventions
  monétaire/actuarielle), coupons, coupon couru, prix pied de coupon et prix
  plein — le cœur de valeur, réservé aux abonnés.
- **Monétisation intégrée** : plans mensuels (gratuit / premium / pro),
  essai gratuit, mur payant, paiement récurrent CMI, portail d'abonnement.

## Architecture

```
kanyon-markets/
├── packages/kanyon/   Package Python : données (ORM, requêtes) + pricer + analytics
├── backend/           API FastAPI : auth JWT, abonnements, paiement CMI, endpoints protégés
├── frontend/          SPA React (Vite) : tarifs, dashboard, pricer, portail d'abonnement
├── scripts/           Seed de données de démonstration
├── docker-compose.yml PostgreSQL + backend + frontend
└── docs/              Architecture & déploiement
```

Détails : [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Démarrage rapide (Docker)

```bash
cp .env.example .env          # adapter le mot de passe DB et la clé secrète
docker compose up --build
```

- Frontend : http://localhost:8080
- API + documentation interactive : http://localhost:8000/docs

Amorcer des données de démonstration (courbe, titres, indices) :

```bash
docker compose exec backend python /app/scripts/seed_demo.py
# ou en local : KANYON_DATABASE_URL=... python scripts/seed_demo.py
```

## Démarrage en local (sans Docker)

```bash
# 1) Package de calcul + backend
pip install -e "packages/kanyon[postgres]"
pip install -r backend/requirements.txt
cp backend/.env.example backend/.env
(cd backend && uvicorn app.main:app --reload)      # http://localhost:8000

# 2) Frontend
cd frontend && npm install && npm run dev           # http://localhost:5173
```

En développement, `KWEB_PAYMENT_PROVIDER=fake` permet de dérouler tout le
parcours d'abonnement sans identifiants CMI.

## Parcours utilisateur (vendable de bout en bout)

1. Inscription → essai gratuit automatique.
2. Choix d'une formule → paiement CMI (redirection signée) → activation.
3. Accès aux données et au pricer derrière le mur payant.
4. Gestion / annulation de l'abonnement depuis le portail.

## Tests

```bash
pytest -q packages/kanyon      # données + pricer
(cd backend && pytest -q)      # API, abonnements, pricer
(cd frontend && npm run build) # build de production
```

## Passage en production

Voir [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) : identifiants marchands CMI,
clé secrète, base PostgreSQL managée, renouvellement mensuel des abonnements,
peuplement des données via `kanyon.imports`.

---

© Kanyon Markets — logiciel **propriétaire**. Tous droits réservés. Voir
[`LICENSE`](LICENSE).
