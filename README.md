# cvstogitmigration

Helper zum Migrieren von CVS nach Git.

## Projektstruktur

```text
cvstogitmigration/
|-- cvstogitmigration/
|   |-- __init__.py
|   `-- cli.py
|-- tests/
|   |-- __init__.py
|   `-- test_cli.py
|-- requirements.txt
|-- requirements-dev.txt
|-- setup.py
|-- tox.ini
`-- MANIFEST.in
```

## Entwicklung

```bash
source .venv/bin/activate
pip install -r requirements-dev.txt
tox
```
