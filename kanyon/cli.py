"""Interface en ligne de commande du projet kanyon.

Expose les opérations courantes sans écrire de script :

    kanyon init-db
    kanyon import indice --debut 12/06/2021 --fin 08/07/2021
    kanyon import volume  --debut 12/06/2021 --fin 08/07/2021
    kanyon import compo   --debut 12/06/2021 --fin 08/07/2021
    kanyon import all     --debut 12/06/2021 --fin 08/07/2021

La configuration (URL de base, répertoire de téléchargement) est lue depuis
l'environnement via :mod:`kanyon.config`.
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional, Sequence

from kanyon import __version__


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def _cmd_init_db(_args: argparse.Namespace) -> int:
    from kanyon.db import init_db

    init_db()
    logging.getLogger("kanyon.cli").info("Schéma de base de données créé.")
    return 0


# Table de correspondance sous-commande d'import -> fonction d'extraction.
_IMPORTERS = {
    "indice": ("kanyon.imports.masi_indice", "extract_implement_indice"),
    "volume": ("kanyon.imports.masi_volume", "extract_implement_volume"),
    "compo": ("kanyon.imports.masi_compo", "extract_implement_compo"),
}


def _resolve_importer(name: str):
    import importlib

    module_name, func_name = _IMPORTERS[name]
    return getattr(importlib.import_module(module_name), func_name)


def _cmd_import(args: argparse.Namespace) -> int:
    targets = list(_IMPORTERS) if args.source == "all" else [args.source]
    for target in targets:
        importer = _resolve_importer(target)
        importer(args.debut, args.fin)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Construit le parseur d'arguments de la CLI."""
    parser = argparse.ArgumentParser(prog="kanyon", description=__doc__)
    parser.add_argument("--version", action="version", version=f"kanyon {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="journalisation détaillée")

    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-db", help="crée les tables de la base de données")
    p_init.set_defaults(func=_cmd_init_db)

    p_import = sub.add_parser("import", help="importe des données de marché")
    p_import.add_argument(
        "source",
        choices=[*_IMPORTERS, "all"],
        help="source à importer (indice, volume, compo ou all)",
    )
    p_import.add_argument("--debut", required=True, help="date initiale (dd/mm/YYYY)")
    p_import.add_argument("--fin", required=True, help="date finale (dd/mm/YYYY)")
    p_import.set_defaults(func=_cmd_import)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Point d'entrée de la CLI.

    Args:
        argv: Arguments (par défaut ``sys.argv[1:]``).

    Returns:
        Le code de sortie du processus.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
