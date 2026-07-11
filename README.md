# Kanyon — données des marchés financiers marocains

Kanyon couvre l'ensemble de la chaîne de traitement des données de marché
marocaines : **extraction** (web scraping des sources publiques), **stockage**
(SQLAlchemy / PostgreSQL) et **manipulation** (pandas), jusqu'au calcul
d'**indicateurs de gestion de portefeuille**.

Sources couvertes : Bourse de Casablanca (indice MASI, indices sectoriels,
composition, volumes), Bank Al-Maghrib (taux directeur, MONIA, TMP, courbes,
adjudications), HCP (IPC), MEF/Trésor, Maroclear et ASFIM (fonds).

## Structure

```
kanyon/
├── config.py            Configuration centralisée (variables d'environnement)
├── db/
│   ├── base.py          Moteur SQLAlchemy, sessions, init_db()
│   ├── models.py        Modèles ORM (toutes les tables)
│   └── queries.py       Requêtes de lecture -> pandas.DataFrame
├── helpers/
│   └── parsing.py       Conversions dates / nombres / texte
├── imports/
│   ├── masi_indice.py   Extraction des indices
│   ├── masi_volume.py   Extraction des volumes par valeur
│   └── masi_compo.py    Extraction de la composition (pondérations)
└── analytics/
    └── ratios.py        Bêta, Sharpe, Treynor, VaR, Expected Shortfall
tests/                   Tests unitaires (pytest)
```

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# ou, en package installable :
pip install -e ".[postgres,scraping,dev]"
```

## Configuration

Aucun secret n'est codé en dur. Copiez `.env.example` en `.env` et adaptez, ou
exportez les variables d'environnement :

| Variable              | Rôle                                   | Défaut                                             |
|-----------------------|----------------------------------------|----------------------------------------------------|
| `KANYON_DATABASE_URL` | URL SQLAlchemy de la base              | `postgresql+psycopg2://localhost:5432/db_kanyon`   |
| `KANYON_DOWNLOAD_DIR` | Répertoire des fichiers téléchargés    | `./data/masi_files`                                |
| `KANYON_HEADLESS`     | Mode headless de Chrome (`1`/`0`)      | `1`                                                |

## Utilisation

Initialiser le schéma :

```python
from kanyon.db import init_db
init_db()
```

Importer des données (format de date : `dd/mm/YYYY`, import incrémental) :

```python
from kanyon.imports.masi_indice import extract_implement_indice
from kanyon.imports.masi_volume import extract_implement_volume
from kanyon.imports.masi_compo import extract_implement_compo

extract_implement_indice("12/06/2021", "08/07/2021")
extract_implement_volume("12/06/2021", "08/07/2021")
extract_implement_compo("12/06/2021", "08/07/2021")
```

Interroger la base (retourne des `DataFrame`) :

```python
from kanyon.db.queries import query_masi_indice, query_marche_monetaire

df_indice = query_masi_indice("2021-06-12", "2021-07-08")
df_mm = query_marche_monetaire("2021-06-12", "2021-07-08")
```

Calculer les indicateurs de gestion :

```python
from kanyon.analytics import compute_indicators

indicateurs = compute_indicators(
    registre=registre,             # {code_fonds: DataFrame[perf, perf_bench]}
    variances_bench=variances,
    means=means, stds=stds,
    risk_free=0.02,
)
```

## Tests

```bash
pytest
```

## Notes techniques

- **ORM** : SQLAlchemy 2.0, un unique moteur configurable via l'environnement.
- **Scraping** : Selenium 4 (`webdriver-manager` gère le pilote Chrome).
- **Import incrémental** : seules les dates manquantes en base sont récupérées.
