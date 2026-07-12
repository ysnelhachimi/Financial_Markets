"""Rendu d'une fiche de portefeuille (factsheet) en HTML imprimable — M9.

Le rendu est **pur** (aucune dépendance externe, aucune I/O) : il produit une
page HTML autonome (CSS inline, styles d'impression) que le client peut afficher
ou imprimer en PDF. Le contexte est assemblé en amont à partir des modules de
portefeuille (allocation, backtest, stress, conformité).
"""
from __future__ import annotations

import datetime as dt
import html
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class FactsheetContext:
    """Données d'une fiche de portefeuille."""

    fund_name: str
    as_of: str
    category: str = ""
    currency: str = "MAD"
    allocation: List[Dict] = field(default_factory=list)   # [{label, weight}]
    metrics: Dict[str, float] = field(default_factory=dict)  # total_return, volatility, sharpe, max_drawdown
    stress: List[Dict] = field(default_factory=list)         # [{name, total_pnl}]
    compliance: Optional[Dict] = None                        # {compliant, checks:[...]}
    notes: str = ""


def _esc(value) -> str:
    return html.escape(str(value))


def _pct(value) -> str:
    try:
        return f"{float(value) * 100:.2f} %"
    except (TypeError, ValueError):
        return "—"


def _metric_rows(metrics: Dict[str, float]) -> str:
    labels = {
        "total_return": "Performance",
        "cagr": "Performance annualisée",
        "volatility": "Volatilité (ann.)",
        "sharpe": "Ratio de Sharpe",
        "max_drawdown": "Perte maximale",
    }
    rows = []
    for key, label in labels.items():
        if key not in metrics:
            continue
        val = metrics[key]
        display = f"{val:.2f}" if key == "sharpe" else _pct(val)
        rows.append(f"<tr><td>{label}</td><td class='num'>{_esc(display)}</td></tr>")
    return "\n".join(rows) or "<tr><td colspan='2' class='muted'>Non disponible</td></tr>"


def _allocation_rows(allocation: List[Dict]) -> str:
    if not allocation:
        return "<tr><td colspan='2' class='muted'>Non disponible</td></tr>"
    return "\n".join(
        f"<tr><td>{_esc(a.get('label'))}</td><td class='num'>{_pct(a.get('weight'))}</td></tr>"
        for a in allocation
    )


def _stress_rows(stress: List[Dict]) -> str:
    if not stress:
        return "<tr><td colspan='2' class='muted'>Non disponible</td></tr>"
    rows = []
    for s in stress:
        pnl = s.get("total_pnl", 0.0)
        cls = "neg" if pnl < 0 else "pos"
        rows.append(f"<tr><td>{_esc(s.get('name'))}</td><td class='num {cls}'>{_pct(pnl)}</td></tr>")
    return "\n".join(rows)


def _compliance_block(compliance: Optional[Dict]) -> str:
    if not compliance:
        return "<p class='muted'>Contrôle de conformité non disponible.</p>"
    ok = compliance.get("compliant")
    badge = (
        "<span class='badge pos'>Conforme</span>" if ok else "<span class='badge neg'>Non conforme</span>"
    )
    rows = []
    for c in compliance.get("checks", []):
        status_cls = "pos" if c.get("status") == "ok" else "neg"
        rows.append(
            f"<tr><td>{_esc(c.get('name'))}</td>"
            f"<td class='num'>{_pct(c.get('value'))}</td>"
            f"<td class='num'>{_pct(c.get('limit'))}</td>"
            f"<td class='{status_cls}'>{'OK' if c.get('status') == 'ok' else 'DÉPASSÉ'}</td></tr>"
        )
    return (
        f"<p>Statut global : {badge}</p>"
        "<table><thead><tr><th>Ratio</th><th>Valeur</th><th>Limite</th><th>Statut</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def render_factsheet_html(ctx: FactsheetContext) -> str:
    """Rend la fiche de portefeuille en HTML autonome et imprimable."""
    generated = dt.date.today().isoformat()
    return f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"/>
<title>Fiche — {_esc(ctx.fund_name)}</title>
<style>
  :root {{ --ink:#0f1720; --muted:#5b6b7d; --line:#d7dee7; --accent:#2f9e6b; }}
  * {{ box-sizing:border-box; }}
  body {{ font-family: Arial, Helvetica, sans-serif; color:var(--ink); margin:0; padding:32px; }}
  h1 {{ margin:0 0 2px; font-size:22px; }}
  h2 {{ font-size:14px; text-transform:uppercase; letter-spacing:.05em; color:var(--accent); margin:22px 0 8px; }}
  .sub {{ color:var(--muted); margin:0 0 16px; }}
  .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th, td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); }}
  th {{ color:var(--muted); font-weight:600; }}
  td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .muted {{ color:var(--muted); }}
  .pos {{ color:#1c7c4f; }} .neg {{ color:#c0392b; }}
  .badge {{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; color:#fff; }}
  .badge.pos {{ background:#1c7c4f; }} .badge.neg {{ background:#c0392b; }}
  .foot {{ margin-top:28px; font-size:11px; color:var(--muted); border-top:1px solid var(--line); padding-top:10px; }}
  @media print {{ body {{ padding:0; }} h2 {{ page-break-after:avoid; }} }}
</style></head>
<body>
  <header>
    <h1>{_esc(ctx.fund_name)}</h1>
    <p class="sub">Fiche de portefeuille · {_esc(ctx.category)} · au {_esc(ctx.as_of)} · {_esc(ctx.currency)}</p>
  </header>

  <div class="grid">
    <section>
      <h2>Indicateurs de performance</h2>
      <table><tbody>{_metric_rows(ctx.metrics)}</tbody></table>
    </section>
    <section>
      <h2>Allocation</h2>
      <table><thead><tr><th>Poste</th><th class="num">Poids</th></tr></thead>
      <tbody>{_allocation_rows(ctx.allocation)}</tbody></table>
    </section>
  </div>

  <section>
    <h2>Résistance aux chocs (stress testing)</h2>
    <table><thead><tr><th>Scénario</th><th class="num">Impact</th></tr></thead>
    <tbody>{_stress_rows(ctx.stress)}</tbody></table>
  </section>

  <section>
    <h2>Conformité prudentielle (AMMC)</h2>
    {_compliance_block(ctx.compliance)}
  </section>

  {f'<section><h2>Notes</h2><p>{_esc(ctx.notes)}</p></section>' if ctx.notes else ''}

  <p class="foot">Document généré le {generated} par Kanyon Markets. Les seuils
  prudentiels sont indicatifs et paramétrables ; ce document ne constitue ni un
  conseil en investissement ni une validation réglementaire.</p>
</body></html>"""
