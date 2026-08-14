#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running. Start Docker Desktop, then retry."
  exit 1
fi

docker compose up -d
echo "Waiting for Kafka topics..."
# kafka-init creates topics; wait until it exits successfully (or already created)
for i in $(seq 1 30); do
  if docker compose ps kafka-init 2>/dev/null | grep -q "Exited (0)"; then
    break
  fi
  # Also accept healthy kafka + existing topics
  if docker exec streaming-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q transactions; then
    break
  fi
  sleep 2
done

echo
docker exec streaming-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list || true
echo
echo "Kafka is ready at localhost:9092"
