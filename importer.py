#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import wielu indeksów Elasticsearch 6.8 z plików wygenerowanych przez eksporter.

Skrypt:
1. czyta listę indeksów z indexes.csv,
2. odczytuje pliki z katalogu exportu,
3. tworzy indeks docelowy (opcjonalnie nadpisuje istniejący),
4. importuje dokumenty z NDJSON przez Bulk API,
5. opcjonalnie odtwarza aliasy,
6. zapisuje raport zbiorczy importu.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

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

load_env_file()
configure_console_output_encoding()

DEST_ES_LINK = get_env_str(
    "DEST_ES_LINK",
    get_env_str("ES_LINK", ""),
)

# Osobna lista indeksów do importu (niezależna od eksportera).
# Fallback do INPUT_FILE zachowuje zgodność wsteczną.
IMPORT_INPUT_FILE = get_env_str(
    "IMPORT_INPUT_FILE",
    get_env_str("INPUT_FILE", "indexes.csv"),
)
INDEX_COLUMN = get_env_str("INDEX_COLUMN", "index_name")
OUTPUT_DIRECTORY = get_env_str(
    "OUTPUT_DIRECTORY",
    "elastic_export",
)

IMPORT_SUMMARY_FILE = get_env_str(
    "IMPORT_SUMMARY_FILE",
    "import_summary.csv",
)
EXPORT_SUMMARY_FILE = get_env_str(
    "EXPORT_SUMMARY_FILE",
    "export_summary.csv",
)

REQUEST_TIMEOUT = get_env_int("REQUEST_TIMEOUT", 300)
ES_CLIENT_MAX_RETRIES = get_env_int("ES_CLIENT_MAX_RETRIES", 5)
ES_OPERATION_MAX_RETRIES = get_env_int("ES_OPERATION_MAX_RETRIES", 3)
ES_RETRY_BACKOFF_SECONDS = get_env_int("ES_RETRY_BACKOFF_SECONDS", 3)
ES_RETRY_BACKOFF_MAX_SECONDS = get_env_int(
    "ES_RETRY_BACKOFF_MAX_SECONDS",
    60,
)
IMPORT_BATCH_SIZE = get_env_int("IMPORT_BATCH_SIZE", 1000)
IMPORT_DRY_RUN = get_env_bool("IMPORT_DRY_RUN", False)
IMPORT_BULK_MAX_RETRIES = get_env_int("IMPORT_BULK_MAX_RETRIES", 3)
IMPORT_BULK_INITIAL_BACKOFF_SECONDS = get_env_int(
    "IMPORT_BULK_INITIAL_BACKOFF_SECONDS",
    2,
)
IMPORT_BULK_MAX_BACKOFF_SECONDS = get_env_int(
    "IMPORT_BULK_MAX_BACKOFF_SECONDS",
    120,
)

DEST_VERIFY_CERTS = get_env_bool(
    "DEST_ES_VERIFY_CERTS",
    get_env_bool("ES_VERIFY_CERTS", False),
)

CSV_DELIMITER = get_env_str("CSV_DELIMITER", ";")
CSV_ENCODING = get_env_str("CSV_ENCODING", "utf-8-sig")

IMPORT_OVERWRITE_EXISTING = get_env_bool(
    "IMPORT_OVERWRITE_EXISTING",
    False,
)

IMPORT_APPLY_ALIASES = get_env_bool(
    "IMPORT_APPLY_ALIASES",
    True,
)

IMPORT_REQUIRE_ES6 = get_env_bool(
    "IMPORT_REQUIRE_ES6",
    True,
)

IMPORT_DOCUMENT_TYPE_FALLBACK = get_env_str(
    "IMPORT_DOCUMENT_TYPE_FALLBACK",
    "_doc",
)
IMPORT_VALIDATE_FILES = get_env_bool(
    "IMPORT_VALIDATE_FILES",
    True,
)
IMPORT_VALIDATE_COUNTS = get_env_bool(
    "IMPORT_VALIDATE_COUNTS",
    True,
)
IMPORT_VALIDATE_AFTER_UPLOAD = get_env_bool(
    "IMPORT_VALIDATE_AFTER_UPLOAD",
    True,
)
IMPORT_POST_VALIDATE_STRICT = get_env_bool(
    "IMPORT_POST_VALIDATE_STRICT",
    True,
)


# ============================================================
# SSL
# ============================================================

if not DEST_VERIFY_CERTS:
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# ============================================================
# POMOCNICZE
# ============================================================

def sanitize_filename(value: str) -> str:
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


def read_json_file(path: str) -> Any:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Nie znaleziono pliku: {path}")

    with open(path, mode="r", encoding="utf-8") as file:
        return json.load(file)


def read_index_names(input_path: str) -> List[str]:
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
                str(header) for header in reader.fieldnames
            )
            raise ValueError(
                f"Brak kolumny '{INDEX_COLUMN}' w pliku {input_path}.\n"
                f"Dostępne kolumny: {available_columns}"
            )

        source_column = normalized_headers[expected_column]

        for row_number, row in enumerate(reader, start=2):
            raw_name = row.get(source_column)
            if raw_name is None:
                continue

            index_name = str(raw_name).strip()
            if not index_name or index_name.startswith("#"):
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


def create_elasticsearch_client() -> Elasticsearch:
    if not DEST_ES_LINK.strip():
        raise ValueError(
            "Brak DEST_ES_LINK w .env (lub ES_LINK)."
        )

    es = Elasticsearch(
        DEST_ES_LINK,
        request_timeout=REQUEST_TIMEOUT,
        verify_certs=DEST_VERIFY_CERTS,
        ssl_show_warn=False,
        retry_on_timeout=True,
        max_retries=ES_CLIENT_MAX_RETRIES,
    )

    info = call_es_with_retries(
        "es.info",
        es.info,
    )
    cluster_name = info.get("cluster_name", "brak danych")
    version = info.get("version", {}).get("number", "brak danych")

    print("Połączono z docelowym Elasticsearch.")
    print(f"Klaster DEST: {cluster_name}")
    print(f"Wersja Elasticsearch DEST: {version}")

    if IMPORT_REQUIRE_ES6:
        major_version_text = str(version).split(".", maxsplit=1)[0]

        try:
            major_version = int(major_version_text)
        except ValueError as error:
            raise ValueError(
                f"Nieprawidłowy format wersji Elasticsearch: {version}"
            ) from error

        if major_version != 6:
            raise ValueError(
                "Importer został przygotowany pod Elasticsearch 6.8. "
                f"Wykryto wersję DEST: {version}"
            )

    return es


def build_index_paths(index_name: str) -> Dict[str, str]:
    safe_name = sanitize_filename(index_name)
    base_dir = os.path.join(OUTPUT_DIRECTORY, safe_name)

    return {
        "base_dir": base_dir,
        "create_index": os.path.join(
            base_dir,
            f"{safe_name}_create_index.json",
        ),
        "aliases": os.path.join(
            base_dir,
            f"{safe_name}_aliases.json",
        ),
        "ndjson": os.path.join(
            base_dir,
            f"{safe_name}_documents.ndjson",
        ),
        "csv": os.path.join(
            base_dir,
            f"{safe_name}_documents.csv",
        ),
    }


def iterate_ndjson_actions(
    ndjson_path: str,
    fallback_index_name: str,
) -> Iterable[Dict[str, Any]]:
    with open(
        ndjson_path,
        mode="r",
        encoding="utf-8",
        newline="",
    ) as ndjson_file:
        line_number = 0

        while True:
            action_line = ndjson_file.readline()
            if not action_line:
                break

            source_line = ndjson_file.readline()
            line_number += 2

            if not source_line:
                raise ValueError(
                    f"Niepełna para linii NDJSON przy linii "
                    f"{line_number - 1} w pliku {ndjson_path}"
                )

            action_line = action_line.strip()
            source_line = source_line.strip()

            if not action_line:
                continue

            action_payload = json.loads(action_line)
            source_payload = json.loads(source_line)

            if not isinstance(action_payload, dict):
                raise ValueError(
                    f"Nieprawidłowa akcja NDJSON w {ndjson_path}"
                )

            if not action_payload:
                raise ValueError(
                    f"Pusta akcja NDJSON w {ndjson_path}"
                )

            operation, metadata = next(
                iter(action_payload.items())
            )

            if not isinstance(metadata, dict):
                raise ValueError(
                    f"Nieprawidłowe metadane NDJSON w {ndjson_path}"
                )

            action: Dict[str, Any] = {
                "_op_type": operation,
                "_source": source_payload,
            }

            action["_index"] = metadata.get(
                "_index",
                fallback_index_name,
            )

            if "_id" in metadata:
                action["_id"] = metadata["_id"]

            if "_type" in metadata:
                action["_type"] = metadata["_type"]
            else:
                action["_type"] = IMPORT_DOCUMENT_TYPE_FALLBACK

            if "routing" in metadata:
                action["_routing"] = metadata["routing"]
            elif "_routing" in metadata:
                action["_routing"] = metadata["_routing"]

            yield action


def count_csv_rows(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0

    with open(
        csv_path,
        mode="r",
        encoding=CSV_ENCODING,
        newline="",
    ) as csv_file:
        reader = csv.reader(
            csv_file,
            delimiter=CSV_DELIMITER,
        )
        # Pomijamy nagłówek.
        next(reader, None)
        return sum(1 for _ in reader)


def read_export_summary_counts(
    index_name: str,
) -> Dict[str, Optional[int]]:
    summary_path = os.path.join(
        OUTPUT_DIRECTORY,
        EXPORT_SUMMARY_FILE,
    )

    if not os.path.exists(summary_path):
        return {
            "documents_expected": None,
            "ndjson_exported": None,
            "csv_exported": None,
            "status": None,
        }

    with open(
        summary_path,
        mode="r",
        encoding=CSV_ENCODING,
        newline="",
    ) as summary_file:
        reader = csv.DictReader(
            summary_file,
            delimiter=CSV_DELIMITER,
        )

        for row in reader:
            if str(row.get("index_name", "")).strip() != index_name:
                continue

            def parse_int(column: str) -> Optional[int]:
                raw = str(row.get(column, "")).strip()
                if not raw:
                    return None
                try:
                    return int(raw)
                except ValueError:
                    return None

            return {
                "documents_expected": parse_int(
                    "documents_expected"
                ),
                "ndjson_exported": parse_int(
                    "ndjson_exported"
                ),
                "csv_exported": parse_int("csv_exported"),
                "status": str(row.get("status", "")).strip() or None,
            }

    return {
        "documents_expected": None,
        "ndjson_exported": None,
        "csv_exported": None,
        "status": None,
    }


def validate_index_files_consistency(
    index_name: str,
    paths: Dict[str, str],
) -> Dict[str, int]:
    required_files = [
        ("create_index", paths["create_index"]),
        ("ndjson", paths["ndjson"]),
    ]

    if IMPORT_APPLY_ALIASES:
        required_files.append(("aliases", paths["aliases"]))

    missing_files = [
        f"{name}: {path}"
        for name, path in required_files
        if not os.path.exists(path)
    ]

    if missing_files:
        raise FileNotFoundError(
            "Brak wymaganych plików importu:\n"
            + "\n".join(missing_files)
        )

    create_index_body = read_json_file(paths["create_index"])
    if not isinstance(create_index_body, dict):
        raise ValueError(
            f"[{index_name}] create_index.json nie jest obiektem JSON."
        )

    mappings = create_index_body.get("mappings")
    settings = create_index_body.get("settings")
    if mappings is None or not isinstance(mappings, dict):
        raise ValueError(
            f"[{index_name}] create_index.json nie zawiera poprawnego 'mappings'."
        )
    if settings is None or not isinstance(settings, dict):
        raise ValueError(
            f"[{index_name}] create_index.json nie zawiera poprawnego 'settings'."
        )

    if IMPORT_APPLY_ALIASES:
        aliases_payload = read_json_file(paths["aliases"])
        if not isinstance(aliases_payload, dict):
            raise ValueError(
                f"[{index_name}] aliases.json nie jest obiektem JSON."
            )

    ndjson_documents = count_ndjson_documents(
        index_name=index_name,
        ndjson_path=paths["ndjson"],
    )
    if ndjson_documents <= 0:
        raise ValueError(
            f"[{index_name}] NDJSON nie zawiera dokumentów do importu."
        )

    csv_documents = count_csv_rows(paths["csv"])

    if IMPORT_VALIDATE_COUNTS and os.path.exists(paths["csv"]):
        if csv_documents != ndjson_documents:
            raise ValueError(
                f"[{index_name}] Niespójność plików: "
                f"CSV={csv_documents}, NDJSON={ndjson_documents}."
            )

    summary_counts = read_export_summary_counts(index_name=index_name)
    expected_ndjson = summary_counts.get("ndjson_exported")
    expected_csv = summary_counts.get("csv_exported")

    if IMPORT_VALIDATE_COUNTS and expected_ndjson is not None:
        if expected_ndjson != ndjson_documents:
            raise ValueError(
                f"[{index_name}] Niespójność z export_summary.csv: "
                f"ndjson_exported={expected_ndjson}, "
                f"plik_ndjson={ndjson_documents}."
            )

    if (
        IMPORT_VALIDATE_COUNTS
        and expected_csv is not None
        and os.path.exists(paths["csv"])
        and expected_csv != csv_documents
    ):
        raise ValueError(
            f"[{index_name}] Niespójność z export_summary.csv: "
            f"csv_exported={expected_csv}, plik_csv={csv_documents}."
        )

    return {
        "ndjson_documents": ndjson_documents,
        "csv_documents": csv_documents,
    }


def get_destination_document_count(
    es: Elasticsearch,
    index_name: str,
) -> int:
    response = call_es_with_retries(
        "count(dest)",
        es.count,
        index=index_name,
        body={
            "query": {
                "match_all": {}
            }
        },
    )
    return int(response.get("count", 0))


def validate_post_import_consistency(
    index_name: str,
    index_action: str,
    documents_imported: int,
    documents_failed: int,
    before_count: Optional[int],
    after_count: int,
) -> Dict[str, str]:
    if documents_failed > 0:
        return {
            "status": "WARNING",
            "message": (
                f"Import zgłosił {documents_failed} błędów; "
                "walidacja po imporcie ograniczona."
            ),
        }

    if index_action == "created":
        if after_count == documents_imported:
            return {
                "status": "OK",
                "message": (
                    "Liczba dokumentów na DEST jest zgodna "
                    "z liczbą zaimportowanych dokumentów."
                ),
            }

        message = (
            f"Niespójność po imporcie: DEST={after_count}, "
            f"zaimportowano={documents_imported}."
        )
        return {
            "status": "ERROR" if IMPORT_POST_VALIDATE_STRICT else "WARNING",
            "message": message,
        }

    if index_action == "kept" and before_count is not None:
        if after_count < before_count:
            message = (
                f"Liczba dokumentów na DEST spadła "
                f"({before_count} -> {after_count})."
            )
            return {
                "status": (
                    "ERROR"
                    if IMPORT_POST_VALIDATE_STRICT
                    else "WARNING"
                ),
                "message": message,
            }

        return {
            "status": "OK",
            "message": (
                "Liczba dokumentów na DEST nie spadła "
                f"({before_count} -> {after_count})."
            ),
        }

    return {
        "status": "OK",
        "message": "Walidacja po imporcie zakończona.",
    }


def ensure_destination_index(
    es: Elasticsearch,
    index_name: str,
    create_index_body: Dict[str, Any],
) -> str:
    exists = bool(
        call_es_with_retries(
            "indices.exists",
            es.indices.exists,
            index=index_name,
        )
    )

    if exists and IMPORT_OVERWRITE_EXISTING:
        print(
            f"[{index_name}] Usuwanie istniejącego indeksu DEST..."
        )
        call_es_with_retries(
            "indices.delete",
            es.indices.delete,
            index=index_name,
        )
        exists = False

    if not exists:
        print(f"[{index_name}] Tworzenie indeksu DEST...")
        call_es_with_retries(
            "indices.create",
            es.indices.create,
            index=index_name,
            body=create_index_body,
        )
        return "created"

    print(
        f"[{index_name}] Indeks DEST już istnieje. "
        "Pominięto create."
    )
    return "kept"


def apply_aliases(
    es: Elasticsearch,
    index_name: str,
    aliases_payload: Dict[str, Any],
) -> int:
    source_aliases = aliases_payload.get(
        index_name,
        {},
    ).get("aliases", {})

    if not source_aliases:
        return 0

    actions: List[Dict[str, Any]] = []

    for alias_name, alias_options in source_aliases.items():
        add_payload: Dict[str, Any] = {
            "index": index_name,
            "alias": alias_name,
        }

        if isinstance(alias_options, dict):
            for key, value in alias_options.items():
                add_payload[key] = value

        actions.append({"add": add_payload})

    if not actions:
        return 0

    call_es_with_retries(
        "indices.update_aliases",
        es.indices.update_aliases,
        body={"actions": actions},
    )
    return len(actions)


def import_ndjson(
    es: Elasticsearch,
    index_name: str,
    ndjson_path: str,
) -> Tuple[int, int, int]:
    success_count = 0
    failed_count = 0
    processed_count = 0

    actions = iterate_ndjson_actions(
        ndjson_path=ndjson_path,
        fallback_index_name=index_name,
    )

    for ok, info in helpers.streaming_bulk(
        client=es,
        actions=actions,
        chunk_size=IMPORT_BATCH_SIZE,
        max_retries=IMPORT_BULK_MAX_RETRIES,
        initial_backoff=IMPORT_BULK_INITIAL_BACKOFF_SECONDS,
        max_backoff=IMPORT_BULK_MAX_BACKOFF_SECONDS,
        raise_on_error=False,
        raise_on_exception=False,
        request_timeout=REQUEST_TIMEOUT,
    ):
        processed_count += 1

        if ok:
            success_count += 1
        else:
            failed_count += 1

            operation = next(iter(info.keys()))
            details = info.get(operation, {})
            error = details.get("error", "brak szczegółów")
            status = details.get("status", "brak statusu")

            print(
                f"[{index_name}] Bulk error "
                f"(status {status}): {error}"
            )

        if processed_count % 10000 == 0:
            print(
                f"[{index_name}] "
                f"Przetworzono {processed_count:,} dokumentów."
            )

    return processed_count, success_count, failed_count


def count_ndjson_documents(
    index_name: str,
    ndjson_path: str,
) -> int:
    count = 0

    for _ in iterate_ndjson_actions(
        ndjson_path=ndjson_path,
        fallback_index_name=index_name,
    ):
        count += 1

        if count % 10000 == 0:
            print(
                f"[{index_name}] DRY RUN: "
                f"sprawdzono {count:,} dokumentów NDJSON."
            )

    return count


def import_single_index(
    es: Elasticsearch,
    index_name: str,
    position: int,
    total_indexes: int,
) -> Dict[str, Any]:
    started = time.time()

    print("\n" + "=" * 80)
    print(f"INDEKS {position}/{total_indexes}: {index_name}")
    print("=" * 80)

    paths = build_index_paths(index_name)

    result: Dict[str, Any] = {
        "index_name": index_name,
        "mode": "DRY_RUN" if IMPORT_DRY_RUN else "LIVE",
        "status": "ERROR",
        "index_action": "",
        "validation_status": "SKIPPED",
        "post_validation_status": "SKIPPED",
        "documents_processed": 0,
        "documents_imported": 0,
        "documents_failed": 0,
        "dest_count_before": "",
        "dest_count_after": "",
        "aliases_applied": 0,
        "create_index_file": paths["create_index"],
        "aliases_file": paths["aliases"],
        "ndjson_file": paths["ndjson"],
        "time_seconds": 0,
        "error": "",
    }

    try:
        if IMPORT_VALIDATE_FILES:
            validation_info = validate_index_files_consistency(
                index_name=index_name,
                paths=paths,
            )
            result["validation_status"] = "OK"
            result["documents_processed"] = validation_info[
                "ndjson_documents"
            ]
        else:
            validation_info = {
                "ndjson_documents": 0,
                "csv_documents": 0,
            }

        create_index_body = read_json_file(paths["create_index"])

        if IMPORT_DRY_RUN:
            destination_exists = bool(
                call_es_with_retries(
                    "indices.exists",
                    es.indices.exists,
                    index=index_name,
                )
            )

            if destination_exists and IMPORT_OVERWRITE_EXISTING:
                result["index_action"] = "would_delete_and_create"
            elif destination_exists:
                result["index_action"] = "would_keep_existing"
            else:
                result["index_action"] = "would_create"

            documents_processed = count_ndjson_documents(
                index_name=index_name,
                ndjson_path=paths["ndjson"],
            )

            if IMPORT_VALIDATE_FILES:
                documents_processed = validation_info[
                    "ndjson_documents"
                ]

            result["documents_processed"] = documents_processed
            result["documents_imported"] = 0
            result["documents_failed"] = 0

            if IMPORT_APPLY_ALIASES:
                aliases_payload = read_json_file(paths["aliases"])
                source_aliases = aliases_payload.get(
                    index_name,
                    {},
                ).get("aliases", {})
                result["aliases_applied"] = len(source_aliases)

            result["status"] = "OK"

            print(
                f"[{index_name}] DRY RUN: "
                f"sprawdzono {documents_processed:,} dokumentów."
            )
            print(
                f"[{index_name}] DRY RUN: akcja indeksu = "
                f"{result['index_action']}."
            )

        else:
            before_count: Optional[int] = None
            if IMPORT_VALIDATE_AFTER_UPLOAD:
                before_count = get_destination_document_count(
                    es=es,
                    index_name=index_name,
                )
                result["dest_count_before"] = before_count

            index_action = ensure_destination_index(
                es=es,
                index_name=index_name,
                create_index_body=create_index_body,
            )
            result["index_action"] = index_action

            (
                documents_processed,
                documents_imported,
                documents_failed,
            ) = import_ndjson(
                es=es,
                index_name=index_name,
                ndjson_path=paths["ndjson"],
            )

            result["documents_processed"] = documents_processed
            result["documents_imported"] = documents_imported
            result["documents_failed"] = documents_failed

            if IMPORT_APPLY_ALIASES:
                aliases_payload = read_json_file(paths["aliases"])

                aliases_applied = apply_aliases(
                    es=es,
                    index_name=index_name,
                    aliases_payload=aliases_payload,
                )
                result["aliases_applied"] = aliases_applied

            if IMPORT_VALIDATE_AFTER_UPLOAD:
                after_count = get_destination_document_count(
                    es=es,
                    index_name=index_name,
                )
                result["dest_count_after"] = after_count

                post_validation = validate_post_import_consistency(
                    index_name=index_name,
                    index_action=index_action,
                    documents_imported=documents_imported,
                    documents_failed=documents_failed,
                    before_count=before_count,
                    after_count=after_count,
                )

                result["post_validation_status"] = post_validation[
                    "status"
                ]
                print(
                    f"[{index_name}] Walidacja po imporcie: "
                    f"{post_validation['status']} - "
                    f"{post_validation['message']}"
                )

            if documents_failed == 0:
                result["status"] = "OK"
            elif documents_imported > 0:
                result["status"] = "WARNING"
            else:
                result["status"] = "ERROR"

            if result["post_validation_status"] == "WARNING":
                if result["status"] == "OK":
                    result["status"] = "WARNING"
            elif result["post_validation_status"] == "ERROR":
                result["status"] = "ERROR"

            print(
                f"[{index_name}] Zaimportowano: "
                f"{documents_imported:,}/{documents_processed:,}"
            )

    except Exception as error:
        result["status"] = "ERROR"
        result["error"] = (
            f"{type(error).__name__}: {error}"
        )
        print(
            f"Błąd importu indeksu '{index_name}': {error}",
            file=sys.stderr,
        )

    finally:
        elapsed = time.time() - started
        result["time_seconds"] = round(elapsed, 2)
        print(f"Czas dla indeksu: {elapsed:.1f} s")

    return result


def normalize_csv_value(value: Any) -> str:
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


def save_summary(
    results: List[Dict[str, Any]],
    output_path: str,
) -> None:
    columns = [
        "index_name",
        "mode",
        "status",
        "index_action",
        "validation_status",
        "post_validation_status",
        "documents_processed",
        "documents_imported",
        "documents_failed",
        "dest_count_before",
        "dest_count_after",
        "aliases_applied",
        "create_index_file",
        "aliases_file",
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
            writer.writerow(normalized_result)

        summary_file.flush()
        os.fsync(summary_file.fileno())


def main() -> int:
    global_started = time.time()

    print("=" * 80)
    print("IMPORT MIGRACYJNY INDEKSÓW ELASTICSEARCH 6.8")
    print("=" * 80)
    if IMPORT_DRY_RUN:
        print("TRYB: DRY RUN (bez zmian na klastrze DEST)")

    try:
        index_names = read_index_names(IMPORT_INPUT_FILE)

        print(
            f"Znaleziono indeksów do importu: "
            f"{len(index_names):,}"
        )

        for number, index_name in enumerate(index_names, start=1):
            print(f"{number}. {index_name}")

        es = create_elasticsearch_client()

        results: List[Dict[str, Any]] = []
        total_indexes = len(index_names)

        for position, index_name in enumerate(
            index_names,
            start=1,
        ):
            result = import_single_index(
                es=es,
                index_name=index_name,
                position=position,
                total_indexes=total_indexes,
            )
            results.append(result)

        summary_path = os.path.join(
            OUTPUT_DIRECTORY,
            IMPORT_SUMMARY_FILE,
        )

        save_summary(
            results=results,
            output_path=summary_path,
        )

        elapsed = time.time() - global_started

        successful = sum(
            1 for result in results if result["status"] == "OK"
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
        print("IMPORT ZAKOŃCZONY")
        print("=" * 80)
        print(f"Indeksy poprawne: {successful:,}")
        print(f"Indeksy z ostrzeżeniem: {warnings:,}")
        print(f"Indeksy z błędem: {errors:,}")
        print(f"Łączny czas: {elapsed:.1f} s")
        print(
            "Raport:\n"
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
        return 130

    except es_exceptions.AuthenticationException as error:
        print(
            "\nBłąd logowania do Elasticsearch DEST.",
            file=sys.stderr,
        )
        print(
            "Sprawdź DEST_ES_LINK (lub ES_LINK) w .env.",
            file=sys.stderr,
        )
        print(error, file=sys.stderr)
        return 3

    except es_exceptions.AuthorizationException as error:
        print(
            "\nBrak wymaganych uprawnień na DEST.",
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
        print(f"\n{error}", file=sys.stderr)
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
        print(f"Szczegóły: {error}", file=sys.stderr)
        return 9


if __name__ == "__main__":
    raise SystemExit(main())
