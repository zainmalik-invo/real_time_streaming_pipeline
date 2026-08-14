#!/usr/bin/env bash
# Reset lakehouse data + Spark checkpoints (keeps Kafka running).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
rm -rf data/lake checkpoints/*
mkdir -p data/lake checkpoints
echo "Cleared data/lake and checkpoints/"
