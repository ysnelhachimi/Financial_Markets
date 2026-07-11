"""Tests de l'interface en ligne de commande (sans accès réseau ni base)."""
import pytest

from kanyon import cli


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert "kanyon" in capsys.readouterr().out


def test_import_source_invalide():
    with pytest.raises(SystemExit):
        cli.main(["import", "inconnu", "--debut", "01/01/2021", "--fin", "02/01/2021"])


def test_import_indice_appelle_importer(monkeypatch):
    appels = []

    def faux_importer(module_name, func_name):
        def _run(debut, fin):
            appels.append((func_name, debut, fin))
        # On renvoie une fonction jouant le rôle de extract_implement_*.
        return _run

    # _resolve_importer renvoie directement la fonction d'extraction.
    monkeypatch.setattr(
        cli,
        "_resolve_importer",
        lambda name: (lambda debut, fin: appels.append((name, debut, fin))),
    )

    assert cli.main(["import", "indice", "--debut", "12/06/2021", "--fin", "08/07/2021"]) == 0
    assert appels == [("indice", "12/06/2021", "08/07/2021")]


def test_import_all_appelle_les_trois(monkeypatch):
    appels = []
    monkeypatch.setattr(
        cli,
        "_resolve_importer",
        lambda name: (lambda debut, fin: appels.append(name)),
    )

    assert cli.main(["import", "all", "--debut", "12/06/2021", "--fin", "08/07/2021"]) == 0
    assert set(appels) == {"indice", "volume", "compo"}


def test_init_db_appelle_init(monkeypatch):
    appele = {"ok": False}

    def faux_init_db():
        appele["ok"] = True

    import kanyon.db as db

    monkeypatch.setattr(db, "init_db", faux_init_db)
    assert cli.main(["init-db"]) == 0
    assert appele["ok"] is True
