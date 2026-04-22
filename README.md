# cvstogitmigration

Python-2.7-Werkzeug zur vollstaendigen Historienmigration lokaler CVS-Repositories nach Git und Bitbucket Server / Data Center.

## Strategie

Die Migration uebernimmt ausdruecklich die CVS-Historie. Pro erkanntem CVS-Repository wird die komplette rekonstruierbare Commit-Historie mit Branches und Tags nach Git ueberfuehrt und danach nach Bitbucket gepusht.

Als Standardwerkzeug wird `cvs-fast-export` verwendet und in Python 2.7 orchestriert. Diese Wahl ist fuer reale Historienmigrationen sinnvoll, weil:

- es direkt einen Git-fast-import-Stream erzeugt
- es Author-Mapping ueber ein Authormap-File unterstuetzt
- es Branches und Tags aus CVS historisch rekonstruiert
- es laut eigener Dokumentation deutlich schneller als aeltere Alternativen arbeitet
- es direkt in einen bare Git-Import fuer Bitbucket eingebunden werden kann

## Erkennung lokaler CVS-Repositories

Ein CVS-Repository wird als jedes Verzeichnis erkannt, das ein direktes Unterverzeichnis `CVSROOT` besitzt. Unterhalb des konfigurierten Projektpfads werden alle solchen Verzeichnisse rekursiv gesucht.

## Historienimport

Die Migration arbeitet mit den lokalen `,v`-Dateien des CVS-Repositories:

1. Alle `,v`-Dateien ausserhalb von `CVSROOT` werden erfasst.
2. Alle CVS-Autoren werden aus der Historie gesammelt.
3. Es wird ein `cvs-fast-export`-Authormap erzeugt.
4. `cvs-fast-export` erzeugt einen Git-fast-import-Stream inklusive Historie, Branches und Tags.
5. Der Stream wird in ein bare Git-Repository importiert.
6. Branch-/Tag-Mappings werden optional nachgezogen.
7. Danach wird das Ergebnis validiert und nach Bitbucket gepusht.

## Author-Mapping

Das Werkzeug erzeugt fuer jedes CVS-Repository ein konkretes Authormap-File:

- bekannte Benutzer kommen aus `author_map`
- LDAP-Benutzer koennen optional ueber `ldap.author_map` oder `ldap.users` geladen werden
- unbekannte Benutzer werden explizit auf `John Doe <john.doe@example.com>` gemappt
- verwendete Fallbacks werden im Report dokumentiert

Wenn sowohl LDAP- als auch `author_map`-Eintraege vorhanden sind, gewinnt `author_map` als expliziter Override.

## Bitbucket-Integration

Das Skript unterstuetzt Bitbucket Server / Data Center per REST-API:

- Projekt pruefen oder optional anlegen
- Repository pruefen oder anlegen
- Branches und Tags vollstaendig pushen
- API-Aufrufe im Report protokollieren

Die API-Kommunikation nutzt `requests`, falls verfuegbar, sonst `urllib2`.

## Technische Grenzen

CVS und Git haben unterschiedliche Historienmodelle. Das Werkzeug dokumentiert diese Unterschiede transparent und uebernimmt Warnungen des Migrationswerkzeugs in den Report. Einige CVS-Artefakte lassen sich technisch nicht immer 1:1 in Git abbilden, insbesondere bei uneindeutigen Tags, branch-spezifischen Sonderfaellen oder historisch inkonsistenten Repositories.

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
- `cvs-fast-export`
- Netzwerkzugriff auf Bitbucket Server / Data Center
- funktionierende SSH-Key-Authentifizierung fuer Git-Push

## Ausfuehrung

```bash
source .venv/bin/activate
python -m cvstogitmigration.cli --config config.example.json --dry-run --verbose
python -m cvstogitmigration.cli --config config.example.json --skip-existing
python -m cvstogitmigration.cli --config config.example.json --force
```

## Reports

Im konfigurierten `report_root` werden erzeugt:

- `migration.log`
- `migration-report.json`
- `migration-report.md`

Diese Dateien dokumentieren pro Repository:

- Erkennung des CVS-Repositories
- Wahl und Ausfuehrung des Migrationswerkzeugs
- Committer-Aufloesung inklusive John-Doe-Fallback
- erkannte Branches und Tags
- Import-Validierung
- Bitbucket-API-Aufrufe
- ausgefuehrte Git-Schritte
- Erfolg, Fehler oder Skip-Grund
