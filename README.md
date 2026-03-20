# cvstogitmigration

Python-2.7-Werkzeug zur bewussten Snapshot-Migration lokaler CVS-Repositories nach Git und Bitbucket Server / Data Center.

## Strategie

Die Migration uebernimmt absichtlich nicht die CVS-Historie. Statt `cvs2git` oder `git cvsimport` wird pro erkanntem CVS-Repository nur der finale Dateistand exportiert, in ein neues Git-Repository geschrieben, mit genau einem Commit versehen und danach per SSH nach Bitbucket gepusht.

Diese Strategie ist fuer die hier geforderte Einmalmigration robuster und einfacher, weil:

- keine Historien-Rekonstruktion noetig ist
- kein Mapping komplexer CVS-Branches und Tags noetig ist
- pro CVS-Repository exakt ein reproduzierbarer Git-Commit entsteht
- Bitbucket-Repositories direkt per REST-API sichergestellt werden koennen

## Erkennung lokaler CVS-Repositories

Ein CVS-Repository wird als jedes Verzeichnis erkannt, das ein direktes Unterverzeichnis `CVSROOT` besitzt. Unterhalb des konfigurierten Projektpfads werden alle solchen Verzeichnisse rekursiv gesucht.

## Snapshot-Erzeugung

Der Snapshot wird direkt aus den lokalen RCS-Dateien erzeugt:

1. Alle `,v`-Dateien ausserhalb von `CVSROOT` werden gesucht.
2. Inhalte in `Attic` werden absichtlich ignoriert, weil geloeschte Dateien nicht in den finalen Snapshot gehoeren sollen.
3. Fuer jede aktive `,v`-Datei wird der HEAD-Inhalt mit `co -p` exportiert.
4. Daraus entsteht ein sauberes Arbeitsverzeichnis ohne CVS-Metadaten.

## Git-Initialisierung

Fuer jedes Snapshot-Arbeitsverzeichnis wird:

1. `git init` ausgefuehrt
2. der konfigurierte Branch (`main` oder `master`) gesetzt
3. genau ein Commit erzeugt
4. `origin` auf die konfigurierte Bitbucket-SSH-URL gesetzt
5. per `git push` nach Bitbucket uebertragen

## Committer-Aufloesung

Das Werkzeug versucht optional, aus den HEAD-Revisionen der Snapshot-Dateien genau einen CVS-Autor zu erkennen. Wenn exakt ein Autor gefunden und in `author_map` konfiguriert ist, wird dieser als Git-Autor und Committer verwendet.

In allen anderen Faellen wird bewusst auf den konfigurierten Fallback gesetzt:

- Name: `John Doe`
- E-Mail: `john.doe@example.com`

## Bitbucket-Integration

Das Skript unterstuetzt Bitbucket Server / Data Center per REST-API:

- Projekt pruefen oder optional anlegen
- Repository pruefen oder anlegen
- Git-Remote per SSH-URL setzen
- Push ueber vorhandene SSH-Key-Authentifizierung

Die API-Kommunikation nutzt `requests`, falls verfuegbar, sonst `urllib2`.

## Projektstruktur

```text
cvstogitmigration/
|-- config.example.json
|-- cvstogitmigration/
|   |-- __init__.py
|   |-- cli.py
|   `-- migrator.py
|-- tests/
|   |-- __init__.py
|   `-- test_migrator.py
|-- requirements.txt
|-- requirements-dev.txt
|-- setup.py
|-- tox.ini
`-- MANIFEST.in
```

## Voraussetzungen

- Python 2.7
- `git`
- `co` aus dem RCS-Paket
- Netzwerkzugriff auf Bitbucket Server / Data Center
- funktionierende SSH-Key-Authentifizierung fuer Git-Push

## Ausfuehrung

```bash
source .venv/bin/activate
python -m cvstogitmigration.cli --config config.example.json --dry-run --verbose
python -m cvstogitmigration.cli --config config.example.json --skip-existing
```

## Reports

Im konfigurierten `report_root` werden erzeugt:

- `migration.log`
- `migration-report.json`
- `migration-report.md`

Diese Dateien dokumentieren pro Repository:

- Erkennung des CVS-Repositories
- Snapshot-Erzeugung
- Committer-Aufloesung inklusive John-Doe-Fallback
- Bitbucket-API-Aufrufe
- ausgefuehrte Git-Schritte
- Erfolg, Fehler oder Skip-Grund
