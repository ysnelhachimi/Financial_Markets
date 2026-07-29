"""Rendu PDF d'une fiche de portefeuille (factsheet) — M9 (PDF serveur).

Utilise ``fpdf2`` (PDF pur Python, sans dépendance système) pour produire un PDF
propre directement à partir du :class:`~kanyon.reporting.factsheet.FactsheetContext`
— plus fiable qu'une conversion HTML→PDF pour un document structuré.
"""
from __future__ import annotations

import datetime as dt
from typing import Dict, List, Optional

from kanyon.reporting.factsheet import FactsheetContext

# Palette (RGB).
_INK = (15, 23, 32)
_MUTED = (91, 107, 125)
_ACCENT = (47, 158, 107)
_POS = (28, 124, 79)
_NEG = (192, 57, 43)
_LINE = (215, 222, 231)


def _pct(value) -> str:
    try:
        return f"{float(value) * 100:.2f} %"
    except (TypeError, ValueError):
        return "-"


def render_factsheet_pdf(ctx: FactsheetContext) -> bytes:
    """Rend la fiche de portefeuille en PDF (octets)."""
    from fpdf import FPDF

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    epw = pdf.w - 2 * pdf.l_margin  # largeur utile

    def section_title(text: str) -> None:
        pdf.ln(3)
        pdf.set_font("helvetica", "B", 11)
        pdf.set_text_color(*_ACCENT)
        pdf.cell(0, 7, text, new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(*_INK)

    def kv_table(rows: List[tuple], colored: bool = False) -> None:
        pdf.set_font("helvetica", "", 10)
        for left, right, *rest in rows:
            status = rest[0] if rest else None
            pdf.set_draw_color(*_LINE)
            pdf.cell(epw * 0.7, 7, str(left), border="B")
            if colored and status is not None:
                pdf.set_text_color(*(_POS if status == "ok" else _NEG))
            pdf.cell(epw * 0.3, 7, str(right), border="B", align="R", new_x="LMARGIN", new_y="NEXT")
            pdf.set_text_color(*_INK)

    # En-tête
    pdf.set_font("helvetica", "B", 18)
    pdf.cell(0, 9, ctx.fund_name, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("helvetica", "", 10)
    pdf.set_text_color(*_MUTED)
    sub = f"Fiche de portefeuille  -  {ctx.category or 'N/A'}  -  au {ctx.as_of}  -  {ctx.currency}"
    pdf.cell(0, 6, sub, new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(*_INK)

    # Indicateurs
    section_title("Indicateurs de performance")
    labels = {
        "total_return": "Performance", "cagr": "Performance annualisee",
        "volatility": "Volatilite (ann.)", "sharpe": "Ratio de Sharpe",
        "max_drawdown": "Perte maximale",
    }
    metric_rows = []
    for key, label in labels.items():
        if key in ctx.metrics:
            val = ctx.metrics[key]
            metric_rows.append((label, f"{val:.2f}" if key == "sharpe" else _pct(val)))
    kv_table(metric_rows or [("Non disponible", "-")])

    # Allocation
    section_title("Allocation")
    alloc_rows = [(a.get("label"), _pct(a.get("weight"))) for a in ctx.allocation]
    kv_table(alloc_rows or [("Non disponible", "-")])

    # Stress
    section_title("Resistance aux chocs (stress testing)")
    stress_rows = [
        (s.get("name"), _pct(s.get("total_pnl")), "ok" if s.get("total_pnl", 0) >= 0 else "breach")
        for s in ctx.stress
    ]
    kv_table(stress_rows or [("Non disponible", "-")], colored=True)

    # Conformité
    section_title("Conformite prudentielle (AMMC)")
    if ctx.compliance:
        pdf.set_font("helvetica", "B", 10)
        ok = ctx.compliance.get("compliant")
        pdf.set_text_color(*(_POS if ok else _NEG))
        pdf.cell(0, 7, "Statut global : " + ("Conforme" if ok else "Non conforme"),
                 new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(*_INK)
        comp_rows = [
            (c.get("name"), f"{_pct(c.get('value'))} / {_pct(c.get('limit'))}",
             c.get("status"))
            for c in ctx.compliance.get("checks", [])
        ]
        kv_table(comp_rows, colored=True)
    else:
        kv_table([("Non disponible", "-")])

    if ctx.notes:
        section_title("Notes")
        pdf.set_font("helvetica", "", 10)
        pdf.multi_cell(0, 6, ctx.notes)

    # Pied de page
    pdf.ln(6)
    pdf.set_font("helvetica", "I", 8)
    pdf.set_text_color(*_MUTED)
    pdf.multi_cell(
        0, 4,
        f"Document genere le {dt.date.today().isoformat()} par Kanyon Markets. "
        "Les seuils prudentiels sont indicatifs et parametrables ; ce document ne "
        "constitue ni un conseil en investissement ni une validation reglementaire.",
    )

    return bytes(pdf.output())
