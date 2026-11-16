#!/usr/bin/env bash
# Creates FraudStream topics (idempotent). Partitions/retention mirror streaming/kafka/client.py TOPIC_SPECS.
set -euo pipefail
B="${BOOTSTRAP:-kafka:29092}"
P="${TOPIC_PREFIX:-}"
K=/opt/kafka/bin/kafka-topics.sh
create() { # name partitions retention_ms
  "$K" --bootstrap-server "$B" --create --if-not-exists --topic "${P}$1" --partitions "$2" \
       --replication-factor 1 --config retention.ms="$3" --config min.insync.replicas=1
}
create transactions.raw        6 604800000
create transactions.validated  6 259200000
create transactions.normal     6 259200000
create transactions.suspicious 6 604800000
create fraud.alerts            3 2592000000
create fraud.dlq               1 2592000000
create features.windows        3 86400000
"$K" --bootstrap-server "$B" --list
