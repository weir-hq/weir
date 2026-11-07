#!/usr/bin/env bash
# What an engineer would check with standard tooling. Compare with `make truth`.
set -euo pipefail
cd "$(dirname "$0")/.."
DC=${DC:-docker compose}

echo "== Kafka consumer lag =="
$DC exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null \
  | awk 'NF && !/^Consumer group/ {print $1, $2, $3, $6}' | column -t

echo
echo "== Celery queue length (Redis) =="
$DC exec -T redis redis-cli llen celery

echo
echo "== Errors in service logs (last 10 minutes) =="
$DC logs --since 10m producer enrich batcher worker aggregate 2>&1 | grep -ciE 'error|exception|retry' || true
