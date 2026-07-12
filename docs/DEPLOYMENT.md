# Déploiement en production

## 1. Prérequis

- Un serveur (VM/conteneurs) avec Docker, ou un hébergeur conteneurs.
- Une base **PostgreSQL** managée (recommandé) ou le conteneur `db` fourni.
- Un compte marchand **CMI / PayZone** (identifiants `clientid` et `storekey`).
- Un nom de domaine + certificat TLS (terminaison HTTPS via un reverse proxy).

## 2. Variables d'environnement

| Variable | Rôle |
|---|---|
| `KWEB_SECRET_KEY` | Clé de signature JWT — **obligatoire**, forte (`openssl rand -hex 32`). |
| `KWEB_DATABASE_URL` | URL PostgreSQL des tables applicatives. |
| `KANYON_DATABASE_URL` | URL PostgreSQL des données de marché. |
| `KWEB_PAYMENT_PROVIDER` | `cmi` en production. |
| `KWEB_CMI_CLIENT_ID` / `KWEB_CMI_STORE_KEY` | Identifiants marchands CMI. |
| `KWEB_PUBLIC_BASE_URL` | URL publique du backend (callbacks CMI). |
| `KWEB_FRONTEND_BASE_URL` | URL publique du frontend (retours de paiement). |
| `KWEB_CORS_ORIGINS` | Origines autorisées (JSON), ex. `["https://app.exemple.ma"]`. |
| `KWEB_TRIAL_DAYS` | Durée de l'essai gratuit. |

## 3. Mise en service

```bash
cp .env.example .env         # renseigner les valeurs de production
docker compose up --build -d
```

Le backend crée son schéma et amorce les plans au démarrage. Pour ajuster les
tarifs, modifier `backend/app/seed.py` (prix en centimes de MAD) avant le
premier lancement, ou mettre à jour la table `plans`.

## 4. Paiement CMI

1. Renseigner `clientid` / `storekey` fournis par CMI.
2. Déclarer l'URL de callback `https://<backend>/api/billing/cmi/callback`
   dans le back-office CMI.
3. Tester une transaction en environnement de recette CMI avant la production.

### Renouvellement mensuel
CMI ne gère pas nativement l'abonnement récurrent. Deux options :
- **Re-facturation planifiée** : une tâche (cron/worker) repère les abonnements
  arrivant à échéance et déclenche un nouveau paiement.
- **Tokenisation** : si le contrat CMI l'autorise, enregistrer un jeton de carte
  pour débiter automatiquement chaque mois.

Le modèle `Subscription.current_period_end` et le champ `cancel_at_period_end`
sont prévus pour piloter ce cycle.

## 5. Données de marché

Peupler la base :
- **Démo** : `python scripts/seed_demo.py`.
- **Production** : extractions du package `kanyon.imports` (indices, volumes,
  composition) et import de la courbe BKAM / du référentiel Maroclear, planifiés
  quotidiennement.

## 6. Sauvegardes & supervision

- Sauvegardes régulières de PostgreSQL (données + abonnements = actifs critiques).
- Supervision de `/api/health` et des logs backend.
- Rotation de `KWEB_SECRET_KEY` = invalidation des sessions (à planifier).
