"""Load project settings from environment / .env files."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _parse_env_line(line: str) -> tuple[str, str] | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):
        line = line[7:].strip()
    if "=" not in line:
        return None
    key, _, value = line.partition("=")
    key = key.strip()
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return key, value


def load_env_files(*paths: Path) -> None:
    """Populate os.environ from dotenv-style files without overriding shell exports."""
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            parsed = _parse_env_line(line)
            if parsed is None:
                continue
            key, value = parsed
            os.environ.setdefault(key, value)


load_env_files(PROJECT_ROOT / ".env", PROJECT_ROOT / ".env.local")


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "transactions")
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "transactions.dlq")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "transaction-validators")


settings = Settings()
