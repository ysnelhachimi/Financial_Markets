const pptxgen = require("pptxgenjs");
const p = new pptxgen();
p.layout = "LAYOUT_WIDE"; // 13.33 x 7.5
p.author = "Kanyon Markets";

// Palette
const PETROL = "0E1E22", INK = "0C1A1C", MUTED = "5C6F70", LINE = "DBE6E4";
const JADE = "1F8F66", JADE_BR = "2FA574", GOLD = "B5892E", WHITE = "FFFFFF", PANEL = "F4F8F7";
const SANS = "Calibri", HEAD = "Century Schoolbook";

const shadow = () => ({ type: "outer", color: "12332B", blur: 8, offset: 3, angle: 90, opacity: 0.18 });

function node(slide, x, y, w, h, title, items, opts) {
  opts = opts || {};
  slide.addShape(p.ShapeType.roundRect, {
    x, y, w, h, rectRadius: 0.09, fill: { color: opts.fill || WHITE },
    line: { color: opts.border || LINE, width: 1 }, shadow: shadow(),
  });
  slide.addText(title, {
    x: x + 0.12, y: y + 0.12, w: w - 0.24, h: 0.32, margin: 0, align: "left",
    fontFace: HEAD, fontSize: 13, bold: true, color: opts.titleColor || INK,
  });
  if (items && items.length) {
    slide.addText(
      items.map((t, i) => ({ text: t, options: { breakLine: i < items.length - 1 } })),
      { x: x + 0.12, y: y + 0.5, w: w - 0.22, h: h - 0.6, margin: 0, align: "left",
        fontFace: SANS, fontSize: 9.5, color: MUTED, lineSpacingMultiple: 1.12 }
    );
  }
}

function arrow(slide, x1, y1, x2, y2, label) {
  slide.addShape(p.ShapeType.line, {
    x: x1, y: y1, w: x2 - x1, h: y2 - y1,
    line: { color: JADE_BR, width: 2.25, endArrowType: "triangle" },
  });
  if (label) {
    slide.addText(label, {
      x: (x1 + x2) / 2 - 0.55, y: Math.min(y1, y2) - 0.28, w: 1.1, h: 0.24, margin: 0,
      align: "center", fontFace: SANS, fontSize: 8.5, italic: true, color: JADE,
    });
  }
}

/* ---------- Slide 1 : Titre ---------- */
let s = p.addSlide();
s.background = { color: PETROL };
s.addShape(p.ShapeType.rect, { x: 0.9, y: 2.35, w: 0.16, h: 0.16, fill: { color: JADE_BR } });
s.addText([
  { text: "Kanyon", options: { color: WHITE } },
  { text: "Markets", options: { color: JADE_BR } },
], { x: 1.2, y: 2.15, w: 11, h: 0.7, margin: 0, fontFace: HEAD, fontSize: 40, bold: true });
s.addText("Schéma de l'application", {
  x: 1.2, y: 3.0, w: 11, h: 0.9, margin: 0, fontFace: HEAD, fontSize: 30, bold: true, color: "CFE7DC",
});
s.addText("Plateforme SaaS de données et de pricing des marchés financiers marocains — architecture technique.", {
  x: 1.2, y: 3.9, w: 10.5, h: 0.5, margin: 0, fontFace: SANS, fontSize: 15, color: "8FB3AC",
});
s.addText("Bourse de Casablanca · BKAM · AMMC · paiement CMI (MAD)", {
  x: 1.2, y: 6.5, w: 10, h: 0.4, margin: 0, fontFace: SANS, fontSize: 12, color: GOLD,
});

/* ---------- Slide 2 : Architecture ---------- */
s = p.addSlide();
s.background = { color: WHITE };
s.addText("Architecture — de la source à l'utilisateur", {
  x: 0.5, y: 0.35, w: 12.3, h: 0.6, margin: 0, fontFace: HEAD, fontSize: 26, bold: true, color: INK,
});
s.addText("Un flux de données unique, un moteur de calcul central, une API protégée par abonnement, un frontend.", {
  x: 0.5, y: 0.95, w: 12.3, h: 0.4, margin: 0, fontFace: SANS, fontSize: 13, color: MUTED,
});

const NY = 2.35, NH = 1.95, NW = 2.15, GAP = 0.4;
const X = i => 0.5 + i * (NW + GAP);

node(s, X(0), NY, NW, NH, "Sources publiques",
  ["· Bourse de Casablanca", "· Bank Al-Maghrib (BKAM)", "· Maroclear (titres)"],
  { fill: PANEL });
node(s, X(1), NY, NW, NH, "kanyon — moteur",
  ["· données & ORM", "· pricer obligataire", "· portefeuille", "· analytics · reporting"],
  { titleColor: JADE, border: JADE_BR });
node(s, X(2), NY, NW, NH, "API FastAPI",
  ["· auth JWT", "· abonnements", "· mur payant (402)", "· endpoints protégés"],
  { titleColor: JADE });
node(s, X(3), NY, NW, NH, "Frontend React",
  ["· tarifs · connexion", "· tableau de bord", "· pricer · portefeuille", "· fiches (HTML/PDF)"],
  { titleColor: JADE });
node(s, X(4), NY, NW, NH, "Utilisateurs",
  ["· sociétés de gestion", "· assurances", "· caisses de retraite", "· trésoreries · banques"],
  { fill: PANEL });

const midY = NY + NH / 2;
arrow(s, X(0) + NW, midY, X(1), midY, "extraction");
arrow(s, X(1) + NW, midY, X(2), midY);
arrow(s, X(2) + NW, midY, X(3), midY);
arrow(s, X(3) + NW, midY, X(4), midY);

// Data plane
const DY = 5.15, DH = 1.15;
node(s, X(1), DY, NW, DH, "PostgreSQL",
  ["Données de marché historisées"], { fill: "EAF3EF", border: JADE_BR, titleColor: JADE });
node(s, X(2), DY, NW, DH, "CMI / PayZone",
  ["Paiement récurrent en MAD"], { fill: "F6EFDD", border: "E0CF9B", titleColor: GOLD });

// vertical connectors
s.addShape(p.ShapeType.line, { x: X(1) + NW / 2, y: NY + NH, w: 0, h: DY - (NY + NH),
  line: { color: JADE_BR, width: 2, beginArrowType: "triangle", endArrowType: "triangle" } });
s.addShape(p.ShapeType.line, { x: X(2) + NW / 2, y: NY + NH, w: 0, h: DY - (NY + NH),
  line: { color: GOLD, width: 2, beginArrowType: "triangle", endArrowType: "triangle" } });

s.addText("Comptes & abonnements + données de marché cohabitent dans PostgreSQL.", {
  x: 0.5, y: 6.55, w: 12.3, h: 0.35, margin: 0, fontFace: SANS, fontSize: 11, italic: true, color: MUTED,
});

/* ---------- Slide 3 : Modules ---------- */
s = p.addSlide();
s.background = { color: WHITE };
s.addText("Neuf modules, une chaîne intégrée", {
  x: 0.5, y: 0.35, w: 12.3, h: 0.6, margin: 0, fontFace: HEAD, fontSize: 26, bold: true, color: INK,
});
const mods = [
  ["M1", "Données de marché", "MASI, volumes, composition, monétaire, courbe BKAM"],
  ["M2", "Pricer obligataire", "Courbe, coupons, sensibilité, prix pied & plein"],
  ["M3", "Construction", "Optimisation & portefeuille cible entre deux courbes"],
  ["M4", "Stratégies OPCVM", "Actions, Diversifié, OMLT, OCT, Monétaire"],
  ["M5", "Backtesting", "Rejeu historique, VL simulée, drawdown"],
  ["M6", "Stress testing", "Chocs taux & actions, VaR stressée"],
  ["M7", "Ratios de risque", "Sharpe, Treynor, VaR, CVaR"],
  ["M8", "Conformité AMMC", "Division, emprise, sensibilité, liquidité"],
  ["M9", "Reporting", "Fiches de fonds HTML & PDF"],
];
const CW = 3.95, CH = 1.55, CGX = 0.28, CGY = 0.26, CX0 = 0.5, CY0 = 1.25;
mods.forEach((m, i) => {
  const col = i % 3, row = Math.floor(i / 3);
  const x = CX0 + col * (CW + CGX), y = CY0 + row * (CH + CGY);
  s.addShape(p.ShapeType.roundRect, { x, y, w: CW, h: CH, rectRadius: 0.08,
    fill: { color: WHITE }, line: { color: LINE, width: 1 }, shadow: shadow() });
  s.addShape(p.ShapeType.ellipse, { x: x + 0.2, y: y + 0.22, w: 0.62, h: 0.62, fill: { color: "EAF3EF" } });
  s.addText(m[0], { x: x + 0.2, y: y + 0.22, w: 0.62, h: 0.62, margin: 0, align: "center",
    valign: "middle", fontFace: HEAD, fontSize: 14, bold: true, color: JADE });
  s.addText(m[1], { x: x + 0.95, y: y + 0.24, w: CW - 1.1, h: 0.34, margin: 0, align: "left",
    fontFace: SANS, fontSize: 14, bold: true, color: INK });
  s.addText(m[2], { x: x + 0.95, y: y + 0.58, w: CW - 1.1, h: 0.6, margin: 0, align: "left",
    fontFace: SANS, fontSize: 10.5, color: MUTED });
  s.addText("✓ livré", { x: x + 0.2, y: y + CH - 0.34, w: 1.2, h: 0.26, margin: 0, align: "left",
    fontFace: SANS, fontSize: 9.5, bold: true, color: JADE_BR });
});

/* ---------- Slide 4 : Flux de valeur ---------- */
s = p.addSlide();
s.background = { color: PETROL };
s.addText("Le flux de valeur", {
  x: 0.5, y: 0.4, w: 12.3, h: 0.6, margin: 0, fontFace: HEAD, fontSize: 26, bold: true, color: WHITE,
});
s.addText("De la donnée brute à la fiche de fonds conforme — en une chaîne.", {
  x: 0.5, y: 1.0, w: 12.3, h: 0.4, margin: 0, fontFace: SANS, fontSize: 13, color: "8FB3AC",
});
const steps = [
  ["Données", "MASI · BKAM"],
  ["Pricing", "courbe · sensibilité"],
  ["Construction", "allocation cible"],
  ["Backtest", "VL simulée"],
  ["Stress", "chocs taux/actions"],
  ["Conformité", "ratios AMMC"],
  ["Fiche PDF", "reporting"],
];
const SW = 1.62, SH = 1.5, SGAP = 0.14, SX0 = 0.5, SYY = 3.1;
steps.forEach((st, i) => {
  const x = SX0 + i * (SW + SGAP);
  const feat = st[0] === "Conformité";
  s.addShape(p.ShapeType.roundRect, { x, y: SYY, w: SW, h: SH, rectRadius: 0.09,
    fill: { color: feat ? JADE : "16302F" }, line: { color: feat ? JADE : "27474A", width: 1 } });
  s.addText(st[0], { x: x + 0.08, y: SYY + 0.28, w: SW - 0.16, h: 0.4, margin: 0, align: "center",
    fontFace: HEAD, fontSize: 13.5, bold: true, color: feat ? "05130E" : WHITE });
  s.addText(st[1], { x: x + 0.08, y: SYY + 0.78, w: SW - 0.16, h: 0.5, margin: 0, align: "center",
    fontFace: SANS, fontSize: 9.5, color: feat ? "0B2019" : "9FC1BA" });
  if (i < steps.length - 1) {
    s.addShape(p.ShapeType.line, { x: x + SW, y: SYY + SH / 2, w: SGAP, h: 0,
      line: { color: GOLD, width: 2, endArrowType: "triangle" } });
  }
});
s.addText("Chaque étape est un module facturable — la conformité AMMC est le différenciant.", {
  x: 0.5, y: 5.2, w: 12.3, h: 0.4, margin: 0, align: "center", fontFace: SANS, fontSize: 12,
  italic: true, color: GOLD,
});

p.writeFile({ fileName: "/home/user/Financial_Markets/marketing/kanyon-architecture.pptx" })
  .then(f => console.log("OK:", f));
