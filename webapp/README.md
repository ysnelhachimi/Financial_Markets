# Kanyon Markets — site payant (abonnements mensuels)

Site web transformant l'application de données/pricing marché marocain (packages
`kanyon` + moteur *Yield Harbor*) en produit à abonnement mensuel.

- **Backend** : FastAPI (auth JWT, plans, abonnements, mur payant, paiement
  CMI/PayZone, API de données protégées).
- **Frontend** : React (Vite) — *en cours*.

## Backend

### Installation

```bash
cd webapp/backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e ../..           # installe le package kanyon (données/pricer)
cp .env.example .env           # puis adapter (clé secrète, CMI, bases...)
```

### Lancement

```bash
uvicorn app.main:app --reload  # http://localhost:8000  (docs : /docs)
```

### Configuration (variables `KWEB_*`)

Aucun secret n'est codé en dur (cf. `.env.example`). En développement,
`KWEB_PAYMENT_PROVIDER=fake` déroule le parcours de paiement sans identifiants
CMI. En production, passer à `cmi` et renseigner `KWEB_CMI_CLIENT_ID` /
`KWEB_CMI_STORE_KEY`.

### Parcours d'abonnement

1. `POST /api/auth/register` → compte + essai gratuit (`KWEB_TRIAL_DAYS`).
2. `POST /api/auth/login` → jeton JWT.
3. `GET /api/plans` → formules (gratuit / premium / pro, prix en MAD).
4. `POST /api/billing/subscribe {plan_code}` → active le gratuit, ou renvoie le
   formulaire de paiement CMI pour un plan payant.
5. CMI rappelle `POST /api/billing/cmi/callback` (signature vérifiée) →
   activation/prolongation d'un mois.
6. Les endpoints `GET /api/market/*` exigent un abonnement en cours (HTTP 402
   sinon).

### Tests

```bash
cd webapp/backend && pytest
```

## Sécurité

- Mots de passe : PBKDF2-HMAC-SHA256 (bibliothèque standard).
- Jetons : JWT HS256 (PyJWT), clé via `KWEB_SECRET_KEY`.
- Signature CMI : HASH ver3 (SHA-512/base64) vérifiée sur les callbacks.
