#!/usr/bin/env bash
# Bootstrap local Python 3.12 venv + deps, and print Java env hints.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3.12 >/dev/null 2>&1; then
  echo "Python 3.12 is required (PySpark does not support 3.14 yet)."
  echo "Install with: brew install python@3.12"
  exit 1
fi

if [[ ! -d .venv ]]; then
  python3.12 -m venv .venv
  echo "Created .venv with $(.venv/bin/python -V)"
fi

# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

# Prefer Java 17 for Spark 3.5 if installed; else OpenJDK from Homebrew.
JAVA_HOME=""
if [[ -d "$(brew --prefix openjdk@17 2>/dev/null)/libexec/openjdk.jdk/Contents/Home" ]]; then
  JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
elif [[ -d "$(brew --prefix openjdk)/libexec/openjdk.jdk/Contents/Home" ]]; then
  JAVA_HOME="$(brew --prefix openjdk)/libexec/openjdk.jdk/Contents/Home"
else
  echo "WARNING: Could not find Homebrew OpenJDK. Install with:"
  echo "  brew install openjdk@17"
fi

cat > .env.local <<EOF
export JAVA_HOME="$JAVA_HOME"
export PATH="\$JAVA_HOME/bin:\$PATH"
export PYSPARK_PYTHON="$ROOT/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$ROOT/.venv/bin/python"
export PYTHONUNBUFFERED=1
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="transactions"
export KAFKA_DLQ_TOPIC="transactions.dlq"
export KAFKA_CONSUMER_GROUP="transaction-validators"
EOF

echo
echo "Setup complete."
echo "Next:"
echo "  1) source .venv/bin/activate && source .env.local"
echo "  2) ./scripts/start_kafka.sh"
echo "  3) See README.md for run steps"
