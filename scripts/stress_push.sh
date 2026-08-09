#!/bin/bash
# 每秒推送 1 条，3 个递增字段，共 1 万次
# Usage: ./scripts/stress_push.sh [agent_url]

AGENT="${1:-http://192.168.0.230:18080}"
DB="stress"
TABLE="counter"

echo "Target: $AGENT/write?db=$DB"
echo "Count: 10000, interval: 1s, fields: a,b,c (1→10000)"
echo "Start: $(date '+%H:%M:%S')"

START=$(date +%s)
for i in $(seq 1 10000); do
  TS=$(( ($(date +%s)) * 1000000000 ))
  CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$AGENT/write?db=$DB" \
    -d "$TABLE,src=script a=${i}i,b=${i}i,c=${i}i $TS" 2>/dev/null)
  if [ $((i % 1000)) -eq 0 ]; then
    ELAPSED=$(($(date +%s) - START))
    echo "[$(date '+%H:%M:%S')] $i/10000 (HTTP $CODE, ${ELAPSED}s elapsed)"
  fi
  sleep 1
done

ELAPSED=$(($(date +%s) - START))
echo "Done: $i/10000 in ${ELAPSED}s"
echo "Verify: curl -s -X POST http://192.168.0.109:18080/api/v1/query -d '{\"sql\":\"SELECT COUNT(*), MAX(a), MIN(a) FROM $DB.$TABLE\"}'"