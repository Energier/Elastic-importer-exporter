#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prosty loader konfiguracji z pliku .env bez dodatkowych zależności.
"""

from __future__ import annotations

import os
import sys
from typing import Optional


def load_env_file(env_path: str = ".env") -> None:
    """
    Wczytuje zmienne środowiskowe z pliku .env.
    Nie nadpisuje wartości już ustawionych w środowisku.
    """

    if not os.path.exists(env_path):
        return

    with open(env_path, mode="r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if not key:
                continue

            # Usuwamy cudzysłowy wokół wartości.
            if (
                len(value) >= 2
                and value[0] == value[-1]
                and value[0] in {"'", '"'}
            ):
                value = value[1:-1]

            os.environ.setdefault(key, value)


def get_env_str(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key)

    if value is None:
        if default is None:
            raise ValueError(f"Brak zmiennej środowiskowej: {key}")
        return default

    return value


def get_env_int(key: str, default: int) -> int:
    value = os.getenv(key)

    if value is None or not value.strip():
        return default

    try:
        return int(value)
    except ValueError as error:
        raise ValueError(
            f"Nieprawidłowa wartość int dla {key}: {value}"
        ) from error


def get_env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)

    if value is None or not value.strip():
        return default

    normalized = value.strip().lower()

    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True

    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False

    raise ValueError(
        f"Nieprawidłowa wartość bool dla {key}: {value}"
    )


def configure_console_output_encoding() -> None:
    """
    Ustawia UTF-8 dla stdout/stderr, aby uniknąć błędów kodowania
    na różnych platformach (zwłaszcza Windows).
    """

    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue

        reconfigure = getattr(stream, "reconfigure", None)
        if not callable(reconfigure):
            continue

        try:
            reconfigure(
                encoding="utf-8",
                errors="replace",
            )
        except Exception:
            # Nie przerywamy programu, jeśli środowisko nie pozwala
            # na zmianę kodowania strumieni.
            pass
