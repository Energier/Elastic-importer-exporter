#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Eksport wielu indeksów Elasticsearch.

Dla każdego indeksu program tworzy:

1. mapping.json
2. settings.json
3. aliases.json
4. create_index.json
5. documents.csv
6. documents.ndjson

CSV:
- służy do przeglądania i analizy danych,
- zawiera spłaszczone dokumenty.

NDJSON:
- służy do bezstratnego importu przez Elasticsearch Bulk API,
- zachowuje oryginalny _source,
- zachowuje _id,
- zachowuje _type (ważne dla Elasticsearch 6.8),
- zachowuje _routing, jeżeli występuje,
- zachowuje strukturę obiektów, tablice, liczby, bool i null.

Plik wejściowy indexes.csv:

index_name
dbms-people
data-gov-pkd
"""

import csv
import json
import os
import re
import sys
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import urllib3
from elasticsearch import Elasticsearch, helpers
from elasticsearch import exceptions as es_exceptions
from elastic_env import (
    configure_console_output_encoding,
    get_env_bool,
    get_env_int,
    get_env_str,
    load_env_file,
)


# ============================================================
# KONFIGURACJA
# ============================================================

SCRIPT_DIRECTORY = os.path.dirname(
    os.path.abspath(__file__)
)

# Wczytanie konfiguracji z .env
load_env_file(
    os.path.join(SCRIPT_DIRECTORY, ".env")
)
configure_console_output_encoding()

# Dane logowania do źródłowego Elasticsearch
SOURCE_ES_LINK = get_env_str(
    "SOURCE_ES_LINK",
    get_env_str("ES_LINK", ""),
)

# Plik wejściowy z listą indeksów
INPUT_FILE = get_env_str("INPUT_FILE", "indexes.csv")

# Nazwa kolumny w pliku wejściowym
INDEX_COLUMN = "index_name"

# Katalog na wszystkie wyniki
OUTPUT_DIRECTORY = get_env_str(
    "OUTPUT_DIRECTORY",
    "elastic_export",
)

if not os.path.isabs(INPUT_FILE):
    INPUT_FILE = os.path.join(
        SCRIPT_DIRECTORY,
        INPUT_FILE,
    )

if not os.path.isabs(OUTPUT_DIRECTORY):
    OUTPUT_DIRECTORY = os.path.join(
        SCRIPT_DIRECTORY,
        OUTPUT_DIRECTORY,
    )

# Raport zbiorczy
SUMMARY_FILE = get_env_str(
    "EXPORT_SUMMARY_FILE",
    "export_summary.csv",
)
EXPORT_DRY_RUN = get_env_bool("EXPORT_DRY_RUN", False)

# Rozmiar partii pobieranej z Elasticsearch
BATCH_SIZE = get_env_int("BATCH_SIZE", 1000)

# Czas utrzymywania kontekstu scroll
SCROLL_TIME = get_env_str("SCROLL_TIME", "10m")

# Timeout zapytań
REQUEST_TIMEOUT = get_env_int("REQUEST_TIMEOUT", 300)
ES_CLIENT_MAX_RETRIES = get_env_int("ES_CLIENT_MAX_RETRIES", 5)
ES_OPERATION_MAX_RETRIES = get_env_int("ES_OPERATION_MAX_RETRIES", 3)
ES_SCAN_MAX_RETRIES = get_env_int("ES_SCAN_MAX_RETRIES", 3)
ES_RETRY_BACKOFF_SECONDS = get_env_int("ES_RETRY_BACKOFF_SECONDS", 3)
ES_RETRY_BACKOFF_MAX_SECONDS = get_env_int(
    "ES_RETRY_BACKOFF_MAX_SECONDS",
    60,
)

# Sprawdzanie certyfikatu SSL
VERIFY_CERTS = get_env_bool(
    "SOURCE_ES_VERIFY_CERTS",
    get_env_bool("ES_VERIFY_CERTS", False),
)

# Separator w plikach CSV
CSV_DELIMITER = get_env_str("CSV_DELIMITER", ";")

# Kodowanie CSV zgodne z polskim Excelem
CSV_ENCODING = get_env_str("CSV_ENCODING", "utf-8-sig")

# Co ile dokumentów wymuszać zapis na dysku
FLUSH_EVERY = get_env_int("FLUSH_EVERY", 1000)

# Co ile dokumentów wyświetlać postęp
PROGRESS_EVERY = get_env_int("PROGRESS_EVERY", 10000)

# Operacja w pliku Bulk NDJSON:
#
# "index"  - utworzy dokument lub nadpisze dokument o tym samym _id
# "create" - zwróci błąd, jeśli dokument o tym _id już istnieje
#
BULK_OPERATION = get_env_str("BULK_OPERATION", "index")

# Czy zapisywać nazwę oryginalnego indeksu w pliku NDJSON
#
# True:
# plik można wysłać bezpośrednio do endpointu /_bulk
#
# False:
# akcja Bulk nie będzie zawierała _index i plik trzeba wysłać do:
# /nazwa-indeksu/_bulk
#
NDJSON_INCLUDE_INDEX_NAME = get_env_bool(
    "NDJSON_INCLUDE_INDEX_NAME",
    True,
)

# Czy zapisywać _type w metadanych Bulk NDJSON.
#
# Elasticsearch 6.8 przy imporcie Bulk wymaga typu dokumentu.
# Dla dokumentów eksportowanych z ES 6.8 pole _type powinno być
# dostępne automatycznie i zostanie zachowane.
#
NDJSON_INCLUDE_DOCUMENT_TYPE = get_env_bool(
    "NDJSON_INCLUDE_DOCUMENT_TYPE",
    True,
)

# Domyślny _type, jeżeli dokument nie zawiera pola _type.
#
# Dla indeksów przygotowanych pod migrację 6.8 najczęściej używane
# jest "_doc". W razie potrzeby zmień na inną nazwę.
#
NDJSON_DOCUMENT_TYPE_FALLBACK = get_env_str(
    "NDJSON_DOCUMENT_TYPE_FALLBACK",
    "_doc",
)


# ============================================================
# WYŁĄCZENIE OSTRZEŻEŃ SSL
# ============================================================

if not VERIFY_CERTS:
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# ============================================================
# FUNKCJE POMOCNICZE
# ============================================================

def sanitize_filename(value: str) -> str:
    """
    Tworzy bezpieczną nazwę pliku na podstawie nazwy indeksu.
    """

    value = re.sub(r'[<>:"/\\|?*]', "_", value)
    value = value.replace(" ", "_")
    value = value.strip("._")

    return value or "elastic_export"


def is_retryable_es_error(error: Exception) -> bool:
    if isinstance(
        error,
        (
            es_exceptions.ConnectionError,
            es_exceptions.ConnectionTimeout,
        ),
    ):
        return True

    if isinstance(error, es_exceptions.TransportError):
        status_code = getattr(error, "status_code", None)
        return status_code in {408, 429, 500, 502, 503, 504}

    return False


def get_retry_delay_seconds(attempt: int) -> int:
    delay = ES_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
    return max(
        1,
        min(delay, ES_RETRY_BACKOFF_MAX_SECONDS),
    )


def call_es_with_retries(
    operation_name: str,
    operation: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    for attempt in range(1, ES_OPERATION_MAX_RETRIES + 1):
        try:
            return operation(*args, **kwargs)

        except Exception as error:
            if (
                not is_retryable_es_error(error)
                or attempt >= ES_OPERATION_MAX_RETRIES
            ):
                raise

            delay = get_retry_delay_seconds(attempt)
            print(
                f"[RETRY] {operation_name} nie powiodło się "
                f"(próba {attempt}/{ES_OPERATION_MAX_RETRIES}): {error}"
            )
            print(
                f"[RETRY] Ponowienie za {delay} s..."
            )
            time.sleep(delay)


def save_json_file(
    output_path: str,
    data: Any,
) -> None:
    """
    Zapisuje dane do czytelnego pliku JSON.
    """

    with open(
        output_path,
        mode="w",
        encoding="utf-8",
    ) as output_file:

        json.dump(
            data,
            output_file,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

        output_file.flush()
        os.fsync(output_file.fileno())


def normalize_csv_value(value: Any) -> str:
    """
    Zamienia wartość na bezpieczny tekst do CSV.
    """

    if value is None:
        return ""

    if isinstance(value, bool):
        value = "true" if value else "false"

    elif isinstance(value, (dict, list)):
        value = json.dumps(
            value,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )

    else:
        value = str(value)

    value = value.replace("\x00", "")
    value = value.replace("\r\n", "\n")
    value = value.replace("\r", "\n")

    return value


def flatten_document(
    value: Any,
    parent_key: str = "",
    separator: str = ".",
) -> Dict[str, Any]:
    """
    Spłaszcza zagnieżdżony dokument do kolumn CSV.

    Przykład:

    {
        "personal_data": {
            "name": "Jan"
        }
    }

    Wynik:

    {
        "personal_data.name": "Jan"
    }

    Listy pozostają zapisane jako JSON w pojedynczej komórce.
    """

    flattened: Dict[str, Any] = {}

    if isinstance(value, dict):

        for key, child_value in value.items():

            current_key = (
                f"{parent_key}{separator}{key}"
                if parent_key
                else str(key)
            )

            if isinstance(child_value, dict):

                flattened.update(
                    flatten_document(
                        value=child_value,
                        parent_key=current_key,
                        separator=separator,
                    )
                )

            elif isinstance(child_value, list):

                flattened[current_key] = json.dumps(
                    child_value,
                    ensure_ascii=False,
                    default=str,
                    separators=(",", ":"),
                )

            else:
                flattened[current_key] = child_value

    elif parent_key:
        flattened[parent_key] = value

    return flattened


# ============================================================
# ODCZYT LISTY INDEKSÓW
# ============================================================

def read_index_names(input_path: str) -> List[str]:
    """
    Odczytuje nazwy indeksów z pliku CSV.

    Obsługiwane separatory:
    - średnik,
    - przecinek,
    - tabulator.

    Puste wiersze i duplikaty są pomijane.
    """

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Nie znaleziono pliku wejściowego: {input_path}"
        )

    index_names: List[str] = []
    seen: Set[str] = set()

    with open(
        input_path,
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as input_file:

        sample = input_file.read(4096)
        input_file.seek(0)

        try:
            dialect = csv.Sniffer().sniff(
                sample,
                delimiters=";,\t",
            )

            reader = csv.DictReader(
                input_file,
                dialect=dialect,
            )

        except csv.Error:
            reader = csv.DictReader(
                input_file,
                delimiter=";",
            )

        if not reader.fieldnames:
            raise ValueError(
                "Plik wejściowy nie zawiera nagłówka."
            )

        normalized_headers = {
            str(header).strip().lower(): header
            for header in reader.fieldnames
            if header is not None
        }

        expected_column = INDEX_COLUMN.strip().lower()

        if expected_column not in normalized_headers:
            available_columns = ", ".join(
                str(header)
                for header in reader.fieldnames
            )

            raise ValueError(
                f"Brak kolumny '{INDEX_COLUMN}' "
                f"w pliku {input_path}.\n"
                f"Dostępne kolumny: {available_columns}"
            )

        source_column = normalized_headers[expected_column]

        for row_number, row in enumerate(
            reader,
            start=2,
        ):
            raw_index_name = row.get(source_column)

            if raw_index_name is None:
                continue

            index_name = str(raw_index_name).strip()

            if not index_name:
                continue

            if index_name.startswith("#"):
                continue

            if index_name in seen:
                print(
                    f"Pominięto duplikat w wierszu "
                    f"{row_number}: {index_name}"
                )
                continue

            seen.add(index_name)
            index_names.append(index_name)

    if not index_names:
        raise ValueError(
            f"Plik {input_path} nie zawiera nazw indeksów."
        )

    return index_names


# ============================================================
# POŁĄCZENIE Z ELASTICSEARCH
# ============================================================

def create_elasticsearch_client() -> Elasticsearch:
    """
    Tworzy klienta Elasticsearch.
    """

    if not SOURCE_ES_LINK.strip():
        raise ValueError(
            "Brak SOURCE_ES_LINK w .env (lub ES_LINK)."
        )

    es = Elasticsearch(
        SOURCE_ES_LINK,
        request_timeout=REQUEST_TIMEOUT,
        verify_certs=VERIFY_CERTS,
        ssl_show_warn=False,
        retry_on_timeout=True,
        max_retries=ES_CLIENT_MAX_RETRIES,
    )

    info = call_es_with_retries(
        "es.info",
        es.info,
    )

    cluster_name = info.get(
        "cluster_name",
        "brak danych",
    )

    version = info.get(
        "version",
        {},
    ).get(
        "number",
        "brak danych",
    )

    print("Połączono z Elasticsearch.")
    print(f"Klaster: {cluster_name}")
    print(f"Wersja Elasticsearch: {version}")

    major_version_text = str(version).split(".", maxsplit=1)[0]

    try:
        major_version = int(major_version_text)

    except ValueError as error:
        raise ValueError(
            f"Nieprawidłowy format wersji Elasticsearch: {version}"
        ) from error

    if major_version != 6:
        raise ValueError(
            "Ten skrypt został przygotowany pod Elasticsearch 6.8. "
            f"Wykryto wersję: {version}"
        )

    return es


def index_exists(
    es: Elasticsearch,
    index_name: str,
) -> bool:
    """
    Sprawdza, czy indeks albo alias istnieje.
    """

    return bool(
        call_es_with_retries(
            "indices.exists",
            es.indices.exists,
            index=index_name
        )
    )


def get_document_count(
    es: Elasticsearch,
    index_name: str,
) -> int:
    """
    Pobiera liczbę dokumentów w indeksie.
    """

    response = call_es_with_retries(
        "count",
        es.count,
        index=index_name,
        body={
            "query": {
                "match_all": {}
            }
        },
    )

    return int(
        response.get("count", 0)
    )


def iterate_documents(
    es: Elasticsearch,
    index_name: str,
) -> Iterable[Dict[str, Any]]:
    """
    Pobiera wszystkie dokumenty przez scan/scroll.

    Oryginalny _source pozostaje niezmieniony.
    """

    query = {
        "query": {
            "match_all": {}
        },
        "_source": True,
    }

    return helpers.scan(
        client=es,
        index=index_name,
        query=query,
        size=BATCH_SIZE,
        scroll=SCROLL_TIME,
        preserve_order=False,
        raise_on_error=True,
        clear_scroll=True,
        request_timeout=REQUEST_TIMEOUT,
    )


# ============================================================
# MAPPING, SETTINGS I ALIASY
# ============================================================

def get_mapping(
    es: Elasticsearch,
    index_name: str,
) -> Dict[str, Any]:
    """
    Pobiera mapping indeksu.
    """

    return dict(
        call_es_with_retries(
            "indices.get_mapping",
            es.indices.get_mapping,
            index=index_name
        )
    )


def get_settings(
    es: Elasticsearch,
    index_name: str,
) -> Dict[str, Any]:
    """
    Pobiera pełne ustawienia indeksu.
    """

    return dict(
        call_es_with_retries(
            "indices.get_settings",
            es.indices.get_settings,
            index=index_name
        )
    )


def get_aliases(
    es: Elasticsearch,
    index_name: str,
) -> Dict[str, Any]:
    """
    Pobiera aliasy przypisane do indeksu.
    """

    try:
        return dict(
            call_es_with_retries(
                "indices.get_alias",
                es.indices.get_alias,
                index=index_name
            )
        )

    except es_exceptions.NotFoundError:
        return {}


def remove_path(
    dictionary: Dict[str, Any],
    path: List[str],
) -> None:
    """
    Usuwa wskazaną zagnieżdżoną właściwość ze słownika.
    """

    current: Any = dictionary

    for part in path[:-1]:

        if not isinstance(current, dict):
            return

        if part not in current:
            return

        current = current[part]

    if isinstance(current, dict):
        current.pop(path[-1], None)


def clean_index_settings(
    raw_index_settings: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Usuwa ustawienia generowane automatycznie przez Elasticsearch,
    których nie powinno się wysyłać przy tworzeniu nowego indeksu.

    Zachowywane są między innymi:
    - number_of_shards,
    - number_of_replicas,
    - analysis,
    - sort,
    - mapping limits,
    - refresh_interval,
    - codec.

    Usuwane są między innymi:
    - uuid,
    - creation_date,
    - provided_name,
    - version.created,
    - history_uuid.
    """

    settings_copy = json.loads(
        json.dumps(raw_index_settings)
    )

    removable_paths = [
        ["uuid"],
        ["creation_date"],
        ["creation_date_string"],
        ["provided_name"],
        ["history_uuid"],
        ["verified_before_close"],
        ["version"],
        ["resize"],
        ["routing", "allocation", "initial_recovery"],
    ]

    for path in removable_paths:
        remove_path(
            settings_copy,
            path,
        )

    return settings_copy


def build_create_index_body(
    index_name: str,
    mappings_response: Dict[str, Any],
    settings_response: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Buduje plik JSON gotowy do utworzenia indeksu docelowego.
    """

    index_mapping_data = mappings_response.get(
        index_name,
        {},
    )

    index_settings_data = settings_response.get(
        index_name,
        {},
    )

    mapping = index_mapping_data.get(
        "mappings",
        {},
    )

    raw_settings = index_settings_data.get(
        "settings",
        {},
    ).get(
        "index",
        {},
    )

    cleaned_settings = clean_index_settings(
        raw_settings
    )

    return {
        "settings": cleaned_settings,
        "mappings": mapping,
    }


# ============================================================
# ANALIZA KOLUMN CSV
# ============================================================

def collect_all_columns(
    es: Elasticsearch,
    index_name: str,
) -> List[str]:
    """
    Pierwszy przebieg po dokumentach.

    Zbiera wszystkie pola występujące w _source.
    """

    metadata_columns = [
        "_index",
        "_id",
        "_routing",
    ]

    for attempt in range(1, ES_SCAN_MAX_RETRIES + 1):
        source_columns: Set[str] = set()
        processed = 0

        try:
            for hit in iterate_documents(
                es=es,
                index_name=index_name,
            ):
                source = hit.get("_source") or {}

                flattened_source = flatten_document(
                    source
                )

                source_columns.update(
                    flattened_source.keys()
                )

                processed += 1

                if processed % PROGRESS_EVERY == 0:
                    print(
                        f"[{index_name}] Analiza pól: "
                        f"{processed:,} dokumentów, "
                        f"{len(source_columns):,} pól."
                    )

            sorted_source_columns = sorted(
                source_columns,
                key=lambda column: column.lower(),
            )

            return metadata_columns + sorted_source_columns

        except Exception as error:
            if (
                not is_retryable_es_error(error)
                or attempt >= ES_SCAN_MAX_RETRIES
            ):
                raise

            delay = get_retry_delay_seconds(attempt)
            print(
                f"[{index_name}] Błąd analizy pól "
                f"(próba {attempt}/{ES_SCAN_MAX_RETRIES}): {error}"
            )
            print(
                f"[{index_name}] Ponowienie pełnego przebiegu "
                f"za {delay} s..."
            )
            time.sleep(delay)

    return metadata_columns


# ============================================================
# ZAPIS CSV I NDJSON
# ============================================================

def build_bulk_action(
    hit: Dict[str, Any],
    index_name: str,
) -> Dict[str, Any]:
    """
    Buduje linię metadanych Elasticsearch Bulk API.

    Zachowywane są:
    - nazwa indeksu,
    - _type (dla ES 6.8),
    - _id,
    - routing.
    """

    action_metadata: Dict[str, Any] = {}

    if NDJSON_INCLUDE_INDEX_NAME:
        action_metadata["_index"] = index_name

    if NDJSON_INCLUDE_DOCUMENT_TYPE:
        document_type = hit.get("_type")

        if document_type is None:
            document_type = NDJSON_DOCUMENT_TYPE_FALLBACK

        if document_type:
            action_metadata["_type"] = document_type

    document_id = hit.get("_id")

    if document_id is not None:
        action_metadata["_id"] = document_id

    routing = hit.get("_routing")

    if routing is not None:
        action_metadata["routing"] = routing

    return {
        BULK_OPERATION: action_metadata
    }


def export_documents(
    es: Elasticsearch,
    index_name: str,
    csv_output_path: str,
    ndjson_output_path: str,
    columns: List[str],
) -> Dict[str, int]:
    """
    Drugi przebieg po indeksie.

    Jednocześnie zapisuje:

    1. płaski CSV,
    2. bezstratny plik Bulk NDJSON.
    """

    for attempt in range(1, ES_SCAN_MAX_RETRIES + 1):
        written_csv = 0
        written_ndjson = 0
        skipped_csv = 0
        skipped_ndjson = 0

        try:
            with open(
                csv_output_path,
                mode="w",
                encoding=CSV_ENCODING,
                newline="",
            ) as csv_file, open(
                ndjson_output_path,
                mode="w",
                encoding="utf-8",
                newline="\n",
            ) as ndjson_file:

                writer = csv.DictWriter(
                    csv_file,
                    fieldnames=columns,
                    delimiter=CSV_DELIMITER,
                    quotechar='"',
                    quoting=csv.QUOTE_ALL,
                    escapechar="\\",
                    doublequote=True,
                    extrasaction="ignore",
                    restval="",
                    lineterminator="\n",
                )

                writer.writeheader()

                csv_file.flush()
                ndjson_file.flush()

                os.fsync(csv_file.fileno())
                os.fsync(ndjson_file.fileno())

                processed = 0

                for hit in iterate_documents(
                    es=es,
                    index_name=index_name,
                ):
                    source = hit.get("_source")

                    if source is None:
                        source = {}

                    # --------------------------------------------
                    # ZAPIS CSV
                    # --------------------------------------------

                    try:
                        flattened_source = flatten_document(
                            source
                        )

                        csv_row: Dict[str, Any] = {
                            "_index": hit.get(
                                "_index",
                                index_name,
                            ),
                            "_id": hit.get("_id", ""),
                            "_routing": hit.get(
                                "_routing",
                                "",
                            ),
                        }

                        csv_row.update(flattened_source)

                        normalized_row = {
                            column: normalize_csv_value(
                                csv_row.get(column, "")
                            )
                            for column in columns
                        }

                        writer.writerow(normalized_row)

                        written_csv += 1

                    except Exception as csv_error:
                        skipped_csv += 1

                        print(
                            f"\n[{index_name}] "
                            f"Nie zapisano dokumentu do CSV."
                        )
                        print(
                            f"ID: {hit.get('_id', 'brak')}"
                        )
                        print(
                            f"Błąd CSV: {csv_error}"
                        )

                    # --------------------------------------------
                    # ZAPIS NDJSON
                    # --------------------------------------------

                    try:
                        action_line = build_bulk_action(
                            hit=hit,
                            index_name=index_name,
                        )

                        ndjson_file.write(
                            json.dumps(
                                action_line,
                                ensure_ascii=False,
                                separators=(",", ":"),
                                default=str,
                            )
                        )

                        ndjson_file.write("\n")

                        ndjson_file.write(
                            json.dumps(
                                source,
                                ensure_ascii=False,
                                separators=(",", ":"),
                                default=str,
                            )
                        )

                        ndjson_file.write("\n")

                        written_ndjson += 1

                    except Exception as ndjson_error:
                        skipped_ndjson += 1

                        print(
                            f"\n[{index_name}] "
                            f"Nie zapisano dokumentu do NDJSON."
                        )
                        print(
                            f"ID: {hit.get('_id', 'brak')}"
                        )
                        print(
                            f"Błąd NDJSON: {ndjson_error}"
                        )

                    processed += 1

                    if processed % FLUSH_EVERY == 0:
                        csv_file.flush()
                        ndjson_file.flush()

                        os.fsync(csv_file.fileno())
                        os.fsync(ndjson_file.fileno())

                    if processed % PROGRESS_EVERY == 0:
                        print(
                            f"[{index_name}] "
                            f"Przetworzono {processed:,} dokumentów."
                        )

                csv_file.flush()
                ndjson_file.flush()

                os.fsync(csv_file.fileno())
                os.fsync(ndjson_file.fileno())

            return {
                "csv_written": written_csv,
                "csv_skipped": skipped_csv,
                "ndjson_written": written_ndjson,
                "ndjson_skipped": skipped_ndjson,
            }

        except Exception as error:
            if (
                not is_retryable_es_error(error)
                or attempt >= ES_SCAN_MAX_RETRIES
            ):
                raise

            delay = get_retry_delay_seconds(attempt)
            print(
                f"[{index_name}] Błąd eksportu dokumentów "
                f"(próba {attempt}/{ES_SCAN_MAX_RETRIES}): {error}"
            )
            print(
                f"[{index_name}] Ponowienie pełnego przebiegu "
                f"za {delay} s..."
            )
            time.sleep(delay)

    return {
        "csv_written": 0,
        "csv_skipped": 0,
        "ndjson_written": 0,
        "ndjson_skipped": 0,
    }


# ============================================================
# EKSPORT POJEDYNCZEGO INDEKSU
# ============================================================

def export_single_index(
    es: Elasticsearch,
    index_name: str,
    position: int,
    total_indexes: int,
) -> Dict[str, Any]:
    """
    Eksportuje jeden indeks.
    """

    started = time.time()

    safe_index_name = sanitize_filename(
        index_name
    )

    index_output_directory = os.path.join(
        OUTPUT_DIRECTORY,
        safe_index_name,
    )

    if not EXPORT_DRY_RUN:
        os.makedirs(
            index_output_directory,
            exist_ok=True,
        )

    mapping_path = os.path.join(
        index_output_directory,
        f"{safe_index_name}_mapping.json",
    )

    settings_path = os.path.join(
        index_output_directory,
        f"{safe_index_name}_settings.json",
    )

    aliases_path = os.path.join(
        index_output_directory,
        f"{safe_index_name}_aliases.json",
    )

    create_index_path = os.path.join(
        index_output_directory,
        f"{safe_index_name}_create_index.json",
    )

    csv_path = os.path.join(
        index_output_directory,
        f"{safe_index_name}_documents.csv",
    )

    ndjson_path = os.path.join(
        index_output_directory,
        f"{safe_index_name}_documents.ndjson",
    )

    print("\n" + "=" * 80)
    print(
        f"INDEKS {position}/{total_indexes}: "
        f"{index_name}"
    )
    print("=" * 80)

    result: Dict[str, Any] = {
        "index_name": index_name,
        "mode": "DRY_RUN" if EXPORT_DRY_RUN else "LIVE",
        "status": "ERROR",
        "documents_expected": 0,
        "csv_exported": 0,
        "csv_skipped": 0,
        "ndjson_exported": 0,
        "ndjson_skipped": 0,
        "columns": 0,
        "mapping_file": mapping_path,
        "settings_file": settings_path,
        "aliases_file": aliases_path,
        "create_index_file": create_index_path,
        "csv_file": csv_path,
        "ndjson_file": ndjson_path,
        "time_seconds": 0,
        "error": "",
    }

    try:
        if not index_exists(
            es=es,
            index_name=index_name,
        ):
            raise ValueError(
                f"Indeks lub alias '{index_name}' nie istnieje."
            )

        document_count = get_document_count(
            es=es,
            index_name=index_name,
        )

        result["documents_expected"] = document_count

        print(
            f"Liczba dokumentów: {document_count:,}"
        )

        if EXPORT_DRY_RUN:
            result["status"] = "OK"
            print(
                f"[{index_name}] DRY RUN: pominięto "
                "pobieranie mapping/settings/aliases "
                "oraz zapis CSV/NDJSON."
            )
            return result

        # ----------------------------------------------------
        # KROK 1: MAPPING
        # ----------------------------------------------------

        print("KROK 1/6: pobieranie mappingu...")

        mapping = get_mapping(
            es=es,
            index_name=index_name,
        )

        save_json_file(
            output_path=mapping_path,
            data=mapping,
        )

        # ----------------------------------------------------
        # KROK 2: SETTINGS
        # ----------------------------------------------------

        print("KROK 2/6: pobieranie ustawień...")

        settings = get_settings(
            es=es,
            index_name=index_name,
        )

        save_json_file(
            output_path=settings_path,
            data=settings,
        )

        # ----------------------------------------------------
        # KROK 3: ALIASY
        # ----------------------------------------------------

        print("KROK 3/6: pobieranie aliasów...")

        aliases = get_aliases(
            es=es,
            index_name=index_name,
        )

        save_json_file(
            output_path=aliases_path,
            data=aliases,
        )

        # ----------------------------------------------------
        # KROK 4: CREATE INDEX
        # ----------------------------------------------------

        print(
            "KROK 4/6: przygotowanie pliku "
            "do utworzenia indeksu..."
        )

        create_index_body = build_create_index_body(
            index_name=index_name,
            mappings_response=mapping,
            settings_response=settings,
        )

        save_json_file(
            output_path=create_index_path,
            data=create_index_body,
        )

        # ----------------------------------------------------
        # KROK 5: KOLUMNY CSV
        # ----------------------------------------------------

        print(
            "KROK 5/6: analiza wszystkich pól..."
        )

        columns = collect_all_columns(
            es=es,
            index_name=index_name,
        )

        result["columns"] = len(columns)

        print(
            f"Znaleziono kolumn: {len(columns):,}"
        )

        # ----------------------------------------------------
        # KROK 6: CSV I NDJSON
        # ----------------------------------------------------

        print(
            "KROK 6/6: eksport CSV i NDJSON..."
        )

        export_result = export_documents(
            es=es,
            index_name=index_name,
            csv_output_path=csv_path,
            ndjson_output_path=ndjson_path,
            columns=columns,
        )

        result["csv_exported"] = (
            export_result["csv_written"]
        )

        result["csv_skipped"] = (
            export_result["csv_skipped"]
        )

        result["ndjson_exported"] = (
            export_result["ndjson_written"]
        )

        result["ndjson_skipped"] = (
            export_result["ndjson_skipped"]
        )

        if (
            export_result["ndjson_written"] == document_count
            and export_result["ndjson_skipped"] == 0
            and export_result["csv_skipped"] == 0
        ):
            result["status"] = "OK"

        else:
            result["status"] = "WARNING"

        print(
            f"CSV: {export_result['csv_written']:,} "
            f"dokumentów."
        )

        print(
            f"NDJSON: {export_result['ndjson_written']:,} "
            f"dokumentów."
        )

    except Exception as error:
        result["status"] = "ERROR"
        result["error"] = (
            f"{type(error).__name__}: {error}"
        )

        print(
            f"Błąd eksportu indeksu "
            f"'{index_name}': {error}",
            file=sys.stderr,
        )

    finally:
        elapsed = time.time() - started

        result["time_seconds"] = round(
            elapsed,
            2,
        )

        print(
            f"Czas dla indeksu: {elapsed:.1f} s"
        )

    return result


# ============================================================
# RAPORT ZBIORCZY
# ============================================================

def save_summary(
    results: List[Dict[str, Any]],
    output_path: str,
) -> None:
    """
    Zapisuje raport zbiorczy z eksportu.
    """

    columns = [
        "index_name",
        "mode",
        "status",
        "documents_expected",
        "csv_exported",
        "csv_skipped",
        "ndjson_exported",
        "ndjson_skipped",
        "columns",
        "mapping_file",
        "settings_file",
        "aliases_file",
        "create_index_file",
        "csv_file",
        "ndjson_file",
        "time_seconds",
        "error",
    ]

    with open(
        output_path,
        mode="w",
        encoding=CSV_ENCODING,
        newline="",
    ) as summary_file:

        writer = csv.DictWriter(
            summary_file,
            fieldnames=columns,
            delimiter=CSV_DELIMITER,
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            escapechar="\\",
            doublequote=True,
            extrasaction="ignore",
            restval="",
            lineterminator="\n",
        )

        writer.writeheader()

        for result in results:

            normalized_result = {
                column: normalize_csv_value(
                    result.get(column, "")
                )
                for column in columns
            }

            writer.writerow(
                normalized_result
            )

        summary_file.flush()
        os.fsync(summary_file.fileno())


# ============================================================
# PROGRAM GŁÓWNY
# ============================================================

def main() -> int:
    global_started = time.time()

    print("=" * 80)
    print("EKSPORT MIGRACYJNY INDEKSÓW ELASTICSEARCH")
    print("=" * 80)
    if EXPORT_DRY_RUN:
        print("TRYB: DRY RUN (bez zapisu danych indeksów do plików)")

    try:
        index_names = read_index_names(
            INPUT_FILE
        )

        print(
            f"Znaleziono indeksów: "
            f"{len(index_names):,}"
        )

        for number, index_name in enumerate(
            index_names,
            start=1,
        ):
            print(
                f"{number}. {index_name}"
            )

        os.makedirs(
            OUTPUT_DIRECTORY,
            exist_ok=True,
        )

        es = create_elasticsearch_client()

        results: List[Dict[str, Any]] = []

        total_indexes = len(index_names)

        for position, index_name in enumerate(
            index_names,
            start=1,
        ):
            result = export_single_index(
                es=es,
                index_name=index_name,
                position=position,
                total_indexes=total_indexes,
            )

            results.append(result)

        summary_path = os.path.join(
            OUTPUT_DIRECTORY,
            SUMMARY_FILE,
        )

        save_summary(
            results=results,
            output_path=summary_path,
        )

        elapsed = time.time() - global_started

        successful = sum(
            1
            for result in results
            if result["status"] == "OK"
        )

        warnings = sum(
            1
            for result in results
            if result["status"] == "WARNING"
        )

        errors = sum(
            1
            for result in results
            if result["status"] == "ERROR"
        )

        print("\n" + "=" * 80)
        print("EKSPORT ZAKOŃCZONY")
        print("=" * 80)

        print(
            f"Indeksy poprawne: {successful:,}"
        )

        print(
            f"Indeksy z ostrzeżeniem: {warnings:,}"
        )

        print(
            f"Indeksy z błędem: {errors:,}"
        )

        print(
            f"Łączny czas: {elapsed:.1f} s"
        )

        print(
            f"Raport:\n"
            f"{os.path.abspath(summary_path)}"
        )

        if errors > 0:
            return 2

        if warnings > 0:
            return 1

        return 0

    except KeyboardInterrupt:
        print(
            "\nProgram został przerwany.",
            file=sys.stderr,
        )

        print(
            "Dotychczas zapisane pliki pozostały na dysku."
        )

        return 130

    except es_exceptions.AuthenticationException as error:
        print(
            "\nBłąd logowania do Elasticsearch.",
            file=sys.stderr,
        )

        print(
            "Sprawdź SOURCE_ES_LINK (lub ES_LINK) w .env.",
            file=sys.stderr,
        )

        print(error, file=sys.stderr)

        return 3

    except es_exceptions.AuthorizationException as error:
        print(
            "\nBrak wymaganych uprawnień.",
            file=sys.stderr,
        )

        print(error, file=sys.stderr)

        return 4

    except es_exceptions.ElasticsearchException as error:
        print(
            "\nBłąd Elasticsearch.",
            file=sys.stderr,
        )

        print(error, file=sys.stderr)

        return 5

    except FileNotFoundError as error:
        print(
            f"\n{error}",
            file=sys.stderr,
        )

        return 6

    except ValueError as error:
        print(
            f"\nBłąd danych wejściowych: {error}",
            file=sys.stderr,
        )

        return 7

    except PermissionError as error:
        print(
            "\nBrak uprawnień do pliku.",
            file=sys.stderr,
        )

        print(error, file=sys.stderr)

        return 8

    except Exception as error:
        print(
            "\nNieoczekiwany błąd.",
            file=sys.stderr,
        )

        print(
            f"Typ błędu: {type(error).__name__}",
            file=sys.stderr,
        )

        print(
            f"Szczegóły: {error}",
            file=sys.stderr,
        )

        return 9


if __name__ == "__main__":
    raise SystemExit(main())