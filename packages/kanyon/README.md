# kanyon

Package Python cœur de **Kanyon Markets** : accès aux données des marchés
financiers marocains et **pricing obligataire**.

Sous-packages :

- `kanyon.config` — configuration par variables d'environnement.
- `kanyon.db` — modèles ORM, sessions, requêtes (→ `pandas.DataFrame`).
- `kanyon.helpers` — parsing dates / nombres, `add_years`.
- `kanyon.imports` — extraction des données (Selenium/HTTP).
- `kanyon.analytics` — ratios de gestion (Sharpe, Treynor, VaR, CVaR).
- `kanyon.pricer` — interpolation de la courbe des taux et pricing obligataire.

## Installation

```bash
pip install -e ".[postgres]"
```

## Pricer — exemple

```python
import datetime as dt
from kanyon.pricer import price_fixed_bond

res = price_fixed_bond(
    date_valeur=dt.date(2021, 6, 15),
    date_emission=dt.date(2015, 6, 15),
    date_jouissance=dt.date(2015, 6, 15),
    date_echeance=dt.date(2025, 6, 15),
    taux_facial=0.03, taux_courbe=0.03, nominal=100,
)
# {'price': 100.0, 'dirty_price': ..., 'coupon_couru': ..., 'type': 'obligation_ordinaire', ...}
```

Avec des données en base, `kanyon.pricer.service` construit la courbe et
valorise un titre par ISIN :

```python
from kanyon.pricer.service import interpolate_tenors, price_isin

tenors = interpolate_tenors("2021-07-01")
valo = price_isin("0000123", date_courbe="2021-07-01", date_valeur="2021-07-01")
```

## Tests

```bash
pytest
```

© Kanyon Markets — logiciel propriétaire. Tous droits réservés.
