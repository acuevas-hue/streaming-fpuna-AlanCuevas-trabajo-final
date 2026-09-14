#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidence
# Run in a fresh project namespace: never erase an existing user's stack or database.
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-fpuna-demo-$(date +%s)}"
echo "$COMPOSE_PROJECT_NAME" > evidence/compose-project.txt
collect() {
  docker compose logs --no-color > evidence/compose.log 2>&1 || true
  docker compose ps -a > evidence/services.txt 2>&1 || true
}
trap collect EXIT
docker compose up -d --build pipeline
docker compose run --rm producer produce demo | tee evidence/producer.log
docker compose run --rm verify | tee evidence/smoke.log
# Restart with durable PostgreSQL results. DirectRunner deliberately replays Kafka from earliest.
docker compose stop pipeline
docker compose rm -f pipeline
docker compose up -d pipeline
found=0
for ((i=0;i<120;i++)); do
  replay="$(docker compose logs --no-color pipeline 2>&1)"
  if grep -Eq 'RESULT merchant-A[|][0-9]+[|]3[|]600 ' <<< "$replay" &&
     grep -Eq 'RESULT merchant-A[|][0-9]+[|]1[|]900 ' <<< "$replay" &&
     grep -Eq 'RESULT merchant-B[|][0-9]+[|]1[|]700 ' <<< "$replay" &&
     grep -q 'INVALID ' <<< "$replay"; then found=1; break; fi
  sleep 1
done
if [[ "$found" != 1 ]]; then echo 'No evidence of replay after restart' >&2; exit 1; fi
docker compose run --rm verify | tee evidence/restart.log
docker compose exec -T postgres psql -U payments -d payments -c 'TABLE payment_windows;' -c 'TABLE invalid_events;' > evidence/results.txt
echo "Demo complete. Stop with: docker compose -p $COMPOSE_PROJECT_NAME down"
