#!/usr/bin/env bash
# Smoke test against a running stack (make up): creates a template, a single
# notification and a 300-item batch, then waits until everything is delivered.
set -euo pipefail

API="${API_URL:-http://localhost:8080}"
CORR="smoke-$(date +%s)"

echo "waiting for $API/readyz ..."
for _ in $(seq 1 60); do
  curl -fsS "$API/readyz" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "$API/readyz" | jq -c .

tpl=$(curl -fsS -X POST "$API/v1/templates" -H 'Content-Type: application/json' \
  -d "{\"name\":\"smoke-$CORR\",\"channel\":\"sms\",\"content\":\"Hi {{name}}, your code is {{code}}\"}" | jq -r .id)
echo "template: $tpl"

single=$(curl -fsS -X POST "$API/v1/notifications" -H 'Content-Type: application/json' -H "X-Correlation-ID: $CORR" \
  -d "{\"recipient\":\"+905551234567\",\"channel\":\"sms\",\"template_id\":\"$tpl\",\"variables\":{\"name\":\"Ada\",\"code\":\"4821\"},\"priority\":\"high\"}")
id=$(echo "$single" | jq -r .id)
echo "single notification: $id ($(echo "$single" | jq -r .content))"

items=$(jq -nc '[range(0;300) | {recipient: ("+90555" + (10000000 + .|tostring)), channel: (["sms","email","push"][. % 3]), content: ("flash sale #" + (.|tostring)), priority: (["high","normal","low"][. % 3])}
  | if .channel == "email" then .recipient = ("user" + (.content|split("#")[1]) + "@example.com")
    elif .channel == "push" then .recipient = ("device-token-" + (.content|split("#")[1]) + "-abcdefgh") else . end]')
batch=$(curl -fsS -X POST "$API/v1/notifications/batch" -H 'Content-Type: application/json' -H "X-Correlation-ID: $CORR" \
  -d "{\"notifications\": $items}" | jq -r .batch_id)
echo "batch: $batch (300 notifications)"

start=$(date +%s)
for _ in $(seq 1 120); do
  b=$(curl -fsS "$API/v1/batches/$batch")
  echo "  $(echo "$b" | jq -c .status_counts)"
  if [ "$(echo "$b" | jq -r .completed)" = "true" ]; then
    break
  fi
  sleep 1
done
echo "batch finished in $(( $(date +%s) - start ))s"
curl -fsS "$API/v1/notifications/$id" | jq '{id, status, attempts, provider_message_id}'
curl -fsS "$API/v1/metrics?window=5m" | jq '{queue_depth, channels: (.channels | map_values({sent, failed, success_rate, throughput_per_second, p95_ms: .latency_ms.p95}))}'

[ "$(curl -fsS "$API/v1/batches/$batch" | jq -r .completed)" = "true" ] || { echo "batch did not complete"; exit 1; }
