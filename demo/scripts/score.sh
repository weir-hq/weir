#!/usr/bin/env bash
# Weir's answers next to the ground truth and the standard tools, for scoring a fault run.
# Run through `make score`, which prints `make truth` and `make baseline` first.
set -euo pipefail
cd "$(dirname "$0")"
WEIR=${WEIR_URL:-http://localhost:4100}
RANGE=${RANGE:-15m}
SEL='{pipeline="orders"}'

query() {
  echo
  echo "-- $1"
  curl -sS --get "$WEIR/api/query" --data-urlencode "q=$1" | python3 -I format_query.py
}

echo
echo "======== Weir (last $RANGE) ========"
query "graph($SEL[$RANGE])"
query "leak($SEL[$RANGE])"
query "drops($SEL[$RANGE]) by (stage, reason)"
query "decompose($SEL[$RANGE])"
query "age($SEL, 0.99)"
query "oldest($SEL)"
query "in_flight($SEL)"
query "transit($SEL[$RANGE])"
query "oldest_waiting($SEL)"
