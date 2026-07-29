# Kanyon Markets — Vision produit & modèle vendable

> Document stratégique : positionnement, offre, monétisation et feuille de route
> de la plateforme, orientée **marché financier marocain** et **rentabilité**.

---

## 1. La phrase qui vend (positionnement)

**« La première plateforme marocaine qui réunit la donnée de marché, le pricing
obligataire, la construction de portefeuille et la conformité AMMC — dans un seul
outil, par abonnement. »**

Aujourd'hui, les sociétés de gestion, assureurs, caisses de retraite et
trésoreries d'entreprise marocains font ce travail dans des **fichiers Excel
éparpillés**, avec des données ressaisies à la main et des ratios prudentiels
calculés hors-ligne. Kanyon Markets industrialise tout cela.

---

## 2. Le problème (marché marocain)

- Les **données** (courbe BKAM, MASI, référentiel Maroclear, VL ASFIM) sont
  publiques mais **dispersées, non structurées, non historisées**.
- Le **pricing obligataire** (courbe, sensibilité, prix) est refait dans des
  classeurs Excel maison, sources d'erreurs et non auditables.
- La **construction et le suivi de portefeuille** (allocation, backtest, stress)
  n'ont pas d'outil local abordable — les solutions internationales
  (Bloomberg PORT, BarraOne) coûtent des dizaines de milliers d'euros/an et sont
  **mal adaptées aux spécificités marocaines** (catégories OPCVM AMMC, conventions
  de taux locales).
- La **conformité prudentielle** (ratios AMMC de division des risques, d'emprise,
  de sensibilité) est vérifiée manuellement et **a posteriori**.

**L'insight** : un acteur local, moins cher, parlant « marocain » (AMMC, BKAM,
MAD, catégories OPCVM), avec conformité intégrée, a un espace réel.

---

## 3. Clients cibles & personas (par ordre de rentabilité)

| Segment | Qui | Douleur | Budget outillage |
|---|---|---|---|
| **Sociétés de gestion (OPCVM)** | ~15–20 SDG au Maroc | Pricing, allocation, ratios AMMC, reporting | Élevé |
| **Compagnies d'assurance** | Directions financières | Allocation actif/passif, stress, VaR | Élevé |
| **Caisses de retraite / prévoyance** | CIMR, CMR, RCAR… | Suivi de mandats, conformité | Élevé |
| **Trésoreries d'entreprise** | Grands groupes | Placement, courbe, valorisation | Moyen |
| **Banques (salles, ALM)** | Desks taux | Courbe, pricing, stress | Élevé |
| **Family offices / conseillers** | Gestion privée | Construction & backtest simples | Moyen |
| **Étudiants / analystes / formation** | Écoles, CFA | Apprentissage, données | Faible (volume) |

Le cœur de rentabilité = **sociétés de gestion + assurances + retraites**
(gros contrats annuels, faible churn, besoin de conformité).

---

## 4. L'offre en modules (la colonne vertébrale du produit)

Chaque module est une **brique activable par abonnement** (montée en gamme
naturelle → revenu croissant par client).

| # | Module | Contenu | Statut |
|---|---|---|---|
| **M1** | **Données de marché** | MASI/sectoriels, volumes, composition, monétaire (MONIA/TMP/taux directeur), courbe BKAM | ✅ livré |
| **M2** | **Pricer taux & obligataire** | Interpolation courbe (monétaire/actuarielle), coupons, sensibilité, prix pied/plein de coupon | ✅ livré |
| **M3** | **Construction de portefeuille** | Portefeuille cible entre **deux courbes** (scénarios de taux) ou entre **paniers d'actions** ; optimisation sous contraintes | ✅ livré (`kanyon.portfolio.construction`) |
| **M4** | **Stratégies par catégorie OPCVM** | Règles d'allocation propres à **Actions, Diversifié, OMLT, OCT, Monétaire** | ✅ livré (`kanyon.portfolio.strategies`) |
| **M5** | **Backtesting** | Rejeu historique d'une stratégie, courbe de performance, indicateurs | ✅ livré (`kanyon.portfolio.backtest`) |
| **M6** | **Stress testing** | Chocs de taux (translation/pentification), chocs actions, VaR/CVaR stressées | ✅ livré (`kanyon.portfolio.stress`) |
| **M7** | **Ratios de gestion & risque** | Sharpe, Treynor, bêta, VaR (paramétrique/historique/Monte-Carlo), CVaR | ✅ livré (`kanyon.analytics`) |
| **M8** | **Réglementation & ratios prudentiels AMMC** | Division des risques, emprise, sensibilité, liquidité — **contrôle automatique** | ✅ livré (`kanyon.portfolio.compliance`) |
| **M9** | **Reporting / factsheets** | Fiches de fonds imprimables (HTML/PDF), allocation, stress, conformité | ✅ livré (`kanyon.reporting`) |

> **M8 est l'argument de vente n°1** : personne ne vend au Maroc de la conformité
> OPCVM **intégrée au calcul**. C'est ce qui justifie le prix et fidélise.

---

## 5. Détail des modules demandés

### M3 — Construction de portefeuille cible

Objectif : proposer une allocation cible et la faire **converger** vers un objectif.

- **Entre deux courbes de taux** : l'utilisateur définit une courbe de départ et
  une courbe d'arrivée (scénario) ; le module construit le portefeuille
  obligataire cible (choix des maturités/lignes) qui optimise le couple
  rendement/sensibilité sous le scénario, et propose les arbitrages.
- **Entre paniers d'actions** : construction d'un portefeuille actions cible à
  partir d'un univers (composition MASI), avec objectif (max Sharpe, min
  variance, tracking d'un indice) sous contraintes (poids max/min, secteur).
- **Optimisation** : moyenne-variance (Markowitz), min-variance, risk-parity,
  sous contraintes linéaires (budget, bornes, secteurs, catégories).

### M4 — Stratégies par catégorie OPCVM (classification AMMC)

Chaque catégorie a ses **règles d'allocation et ses garde-fous** :

| Catégorie | Univers dominant | Levier de stratégie | Contrainte clé (indicative) |
|---|---|---|---|
| **Actions** | ≥ 60 % actions | Sélection, secteurs, bêta cible | Exposition actions minimale |
| **Diversifié** | Actions + taux | Allocation tactique actions/taux | Bornes par classe d'actifs |
| **OMLT** (Oblig. Moyen/Long Terme) | Obligations longues | Duration/sensibilité cible | Sensibilité élevée |
| **OCT** (Oblig. Court Terme) | Obligations courtes | Portage, roll-down | Sensibilité bornée (courte) |
| **Monétaire** | Titres < 1 an | Portage, échelle de maturités | Sensibilité ≤ seuil monétaire, WAM/WAL |

> Les seuils exacts (sensibilité, WAM/WAL, expositions) sont **paramétrables** et
> doivent être calés sur les **circulaires AMMC en vigueur** — le module les
> applique automatiquement une fois configurés.

### M5 — Backtesting

- Rejeu d'une stratégie sur l'historique (données M1) : rebalancements
  périodiques, frais, courbe de valeur liquidative simulée.
- Sorties : performance cumulée, volatilité, max drawdown, Sharpe/Treynor,
  contribution par ligne/secteur.

### M6 — Stress testing

- **Taux** : translation parallèle (+100/+200 pb), pentification/aplatissement,
  chocs par tenor → repricing du portefeuille obligataire via M2.
- **Actions** : choc de marché (-10/-20 %), choc sectoriel, choc de bêta.
- **Agrégé** : VaR/CVaR stressées, perte potentielle par scénario, tableau de
  résistance par catégorie de portefeuille.

### M8 — Réglementation & ratios prudentiels (AMMC)

Contrôle **automatique** à chaque construction/rebalancement :

- **Division des risques** : plafond par émetteur (règle type 5 % / 10 % / 40 %),
  agrégation par groupe.
- **Ratio d'emprise** : part maximale détenue d'une même émission/émetteur.
- **Sensibilité / duration** : bornes selon la catégorie (OCT/OMLT/Monétaire).
- **Liquidité & trésorerie** : plafonds d'illiquide, de liquidités, d'emprunt.
- **Sortie** : tableau de conformité (vert/orange/rouge), alertes de dépassement,
  piste d'audit horodatée.

> Positionné comme **module de conformité**, pas de conseil réglementaire : la
> plateforme calcule et alerte ; la responsabilité réglementaire reste au client.
> Les seuils sont configurables et à valider avec les textes AMMC en vigueur.

---

## 6. Modèle de revenus (comment on gagne de l'argent)

**SaaS B2B par abonnement annuel**, structuré pour maximiser le revenu par
client (ARPU) :

1. **Par palier (tiers)** — déjà en place :
   - *Découverte* (gratuit limité) → acquisition.
   - *Premium* (données + pricer + analytics) → cœur PME/analystes.
   - *Pro / Entreprise* (construction + backtest + stress + conformité + API +
     multi-utilisateurs) → sociétés de gestion, assurances, retraites.
2. **Par module** : les modules M3–M9 sont des **options facturées** (montée en
   gamme).
3. **Par siège** (utilisateur nommé) pour les grands comptes.
4. **API metering** : facturation à l'usage pour l'intégration dans les SI
   clients (valorisation batch, reporting automatisé).
5. **Add-ons** : reporting sur-mesure, historique étendu, support prioritaire.

**Ordres de grandeur (à calibrer sur le marché) :** un contrat société de gestion
« Pro + conformité » peut se situer bien au-dessus d'un abonnement individuel,
avec un **coût marginal quasi nul** (même code, mêmes données) → marge brute
élevée, typique du SaaS.

---

## 7. Pourquoi c'est rentable

- **Coût marginal ~ nul** : une fois la donnée ingérée et le moteur écrit, chaque
  nouveau client coûte quasi zéro → marge brute SaaS (70–90 %).
- **Revenus récurrents** (abonnement annuel) + **faible churn** (outil de
  conformité = collant, difficile à quitter en cours d'année).
- **Montée en gamme** intégrée (modules) → ARPU croissant sans réacquisition.
- **Barrière à l'entrée** : la combinaison données locales + pricing marocain +
  conformité AMMC est longue à répliquer.
- **Marché captif** : peu d'acteurs, besoin réglementaire réel, alternatives
  internationales chères et inadaptées.

---

## 8. Avantage concurrentiel (moat)

1. **Localisation profonde** : conventions de taux marocaines, catégories OPCVM
   AMMC, référentiel Maroclear, courbe BKAM — là où Bloomberg/Barra sont
   génériques et chers.
2. **Conformité intégrée** (M8) : argument unique sur le marché local.
3. **Prix local** : une fraction du coût des solutions internationales.
4. **Données historisées** : plus le temps passe, plus l'historique (donc la
   valeur du backtest) grandit — effet cumulatif.

---

## 9. Go-to-market (Maroc)

1. **Design partners** : 2–3 sociétés de gestion en pilote (tarif préférentiel
   contre retours produit et références).
2. **Preuve par la conformité** : démontrer le contrôle automatique des ratios
   AMMC → déclencheur d'achat.
3. **Contenu & crédibilité** : notes de marché (courbe, MASI) publiées → visibilité
   auprès des desks.
4. **Écoles & CFA Society Morocco** : offre formation (volume + futurs
   prescripteurs).
5. **Expansion** : assurances, retraites, trésoreries, puis Afrique francophone
   (UEMOA/CEMAC) — mêmes briques, autre courbe/réglementation.

---

## 10. Feuille de route par phases

- **Phase 0 (fait)** : données (M1), pricer (M2), ratios (M7), abonnements &
  paiement CMI, back-office, import courbe BKAM.
- **Phase 1** : **M3 construction** (paniers actions + entre deux courbes),
  **M5 backtest**. → débloque l'offre « Pro ».
- **Phase 2** : **M6 stress testing**, **M4 stratégies par catégorie OPCVM**.
- **Phase 3** : **M8 conformité AMMC** (le différenciant), **M9 reporting/factsheets**.
- **Phase 4** : historisation étendue, connecteurs SI clients, expansion régionale.

---

## 11. Cartographie technique (où ça vit dans le code)

Nouveau sous-package `kanyon.portfolio` (moteur pur, testable), exposé via l'API
derrière le mur payant, et par module dans le frontend :

```
packages/kanyon/kanyon/portfolio/
├── universe.py       Univers investissable RÉEL (prix MASI, sensibilités obligataires depuis la base)
├── service.py        Orchestration DB : optimize_equities / backtest_equities
├── construction.py   Optimisation (Markowitz, min-variance, risk-parity, cible)
├── strategies.py     Règles par catégorie OPCVM (Actions/Diversifié/OMLT/OCT/Monétaire)
├── backtest.py       Rejeu historique + métriques
├── stress.py         Chocs taux/actions + VaR/CVaR stressées
└── compliance.py     Ratios prudentiels AMMC (division, emprise, sensibilité, liquidité)
packages/kanyon/kanyon/reporting/
├── factsheet.py      Fiche HTML imprimable
└── pdf.py            Fiche PDF (généré côté serveur, fpdf2)
```

Le moteur de portefeuille est **câblé sur les données réelles** : `optimize_equities`
et `backtest_equities` construisent l'univers depuis `masi_volume`. Endpoints :
`GET /api/portfolio/equities/{optimize,backtest}` et `POST /api/reporting/factsheet.pdf`.

- `kanyon.analytics` (M7) fournit déjà les ratios de risque réutilisés par le
  backtest et le stress.
- `kanyon.pricer` (M2) est réutilisé par la construction obligataire et le stress
  de taux (repricing).

---

## 12. Avertissement

La plateforme **calcule, simule et alerte** ; elle ne fournit ni conseil en
investissement ni validation réglementaire. Les seuils prudentiels sont
**paramétrables** et doivent être calés sur les **textes AMMC/ACAPS/BAM en
vigueur** par le client, sous sa responsabilité.
