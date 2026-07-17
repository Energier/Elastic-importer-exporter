# Elasticsearch 6.8 Migrator

Zestaw skryptów do migracji indeksów Elasticsearch 6.8:

- `eksporter.py` - eksport z klastra SOURCE do plików lokalnych
- `importer.py` - import z plików lokalnych do klastra DEST
- `elastic_env.py` - loader konfiguracji z `.env`

## Wymagania

- Python 3.8+
- pakiety z `requirements.txt`

Przykładowa instalacja:

```bash
pip install -r requirements.txt
```

Uwaga: `python-dotenv` nie jest wymagany, bo projekt używa własnego loadera `.env` w `elastic_env.py`.

## Konfiguracja

1. Uzupełnij plik `.env`:
   - `SOURCE_ES_LINK` (dla eksportera),
   - `DEST_ES_LINK` (dla importera).
2. Przygotuj listy indeksów:
   - `INPUT_FILE` - lista indeksów do eksportu,
   - `IMPORT_INPUT_FILE` - lista indeksów do importu (może być inna).

Format CSV:

```csv
index_name
my-index-1
my-index-2
```

## Eksport

```bash
python eksporter.py
```

Wyniki trafiają do `OUTPUT_DIRECTORY` (domyślnie `elastic_export`), w tym:

- `*_mapping.json`
- `*_settings.json`
- `*_aliases.json`
- `*_create_index.json`
- `*_documents.csv`
- `*_documents.ndjson`
- raport `export_summary.csv`

## Import

```bash
python importer.py
```

Importer czyta dane z katalogu eksportu i:

- tworzy indeks docelowy,
- ładuje NDJSON przez Bulk API,
- opcjonalnie odtwarza aliasy,
- zapisuje raport `import_summary.csv`.

Przed importem może też walidować spójność plików:

- obecność wymaganych plików (`create_index.json`, `documents.ndjson`, opcjonalnie `aliases.json`),
- poprawność struktury `create_index.json`,
- poprawność składni i par linii w NDJSON,
- zgodność liczby dokumentów między NDJSON i CSV,
- zgodność liczników z `export_summary.csv` (jeśli dostępny).

Po imporcie może dodatkowo wykonać walidację na klastrze DEST:

- zapisuje `dest_count_before` i `dest_count_after`,
- dla nowo utworzonego indeksu sprawdza zgodność `DEST == documents_imported`,
- dla istniejącego indeksu (`kept`) sprawdza, czy liczba dokumentów nie spadła,
- zapisuje `post_validation_status` do raportu (`OK` / `WARNING` / `ERROR`).

## Dry Mode (tryb testowy)

### Eksporter

Ustaw w `.env`:

```env
EXPORT_DRY_RUN=true
```

Tryb testowy eksportera:

- łączy się z SOURCE,
- sprawdza istnienie indeksów i liczbę dokumentów,
- **nie zapisuje** plików mapping/settings/aliases/CSV/NDJSON.

### Importer

Ustaw w `.env`:

```env
IMPORT_DRY_RUN=true
```

Tryb testowy importera:

- łączy się z DEST,
- waliduje pliki `create_index.json` i `documents.ndjson`,
- zlicza dokumenty z NDJSON,
- pokazuje plan akcji (`would_create`, `would_keep_existing`, itp.),
- **nie wykonuje** create/delete indeksu, bulk importu ani zmian aliasów.

## Najważniejsze opcje `.env`

- `SOURCE_ES_LINK`, `SOURCE_ES_VERIFY_CERTS`
- `DEST_ES_LINK`, `DEST_ES_VERIFY_CERTS`
- `INPUT_FILE`, `IMPORT_INPUT_FILE`
- `OUTPUT_DIRECTORY`
- `EXPORT_DRY_RUN`, `IMPORT_DRY_RUN`
- `IMPORT_OVERWRITE_EXISTING`
- `IMPORT_APPLY_ALIASES`
- `IMPORT_VALIDATE_FILES`
- `IMPORT_VALIDATE_COUNTS`
- `IMPORT_VALIDATE_AFTER_UPLOAD`
- `IMPORT_POST_VALIDATE_STRICT`

## Stabilność dla długich zadań

Skrypty mają retry i backoff dla operacji sieciowych oraz timeoutów.

- Retry klienta HTTP: `ES_CLIENT_MAX_RETRIES`
- Retry operacji API (exists/count/get/create/delete): `ES_OPERATION_MAX_RETRIES`
- Backoff retry: `ES_RETRY_BACKOFF_SECONDS`, `ES_RETRY_BACKOFF_MAX_SECONDS`
- Retry pełnego przebiegu scroll w eksporcie: `ES_SCAN_MAX_RETRIES`
- Retry chunków bulk przy imporcie:
  - `IMPORT_BULK_MAX_RETRIES`
  - `IMPORT_BULK_INITIAL_BACKOFF_SECONDS`
  - `IMPORT_BULK_MAX_BACKOFF_SECONDS`
- Timeout żądań: `REQUEST_TIMEOUT`

## Kody wyjścia

Skrypty zwracają:

- `0` - sukces,
- `1` - ostrzeżenia,
- `2+` - błędy (szczegóły na stderr).
