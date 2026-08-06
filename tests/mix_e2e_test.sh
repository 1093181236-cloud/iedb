#!/bin/bash
# iedb mix mode integration test — agent + server in one process
# Usage: ./tests/mix_e2e_test.sh [--keep-tmp] [--verbose]
#
# Tests:
#   1. Health check
#   2. Write Line Protocol → 204
#   3. Buffer query + tag filter
#   4. Multiple tables/databases
#   5. Local Parquet snapshot
#   6. SQL query via DataFusion
#   7. Aggregate query (COUNT, AVG)
#   8. Metadata: databases, tables, detail
#   9. Schema evolution (new field detection)
#  10. Query truncation (max_rows enforcement)
#  11. Error handling: 400/404/405/413/422
#  12. Agent API unavailable (mix has no separate agent management)

set -e
KEEP_TMP=false
VERBOSE=false
for a in "$@"; do case "$a" in --keep-tmp) KEEP_TMP=true ;; --verbose) VERBOSE=true ;; esac; done

RED='\033[31m'; GREEN='\033[32m'; CYAN='\033[36m'; NC='\033[0m'
pass() { echo -e "${GREEN}PASS${NC} $*"; }
fail() { echo -e "${RED}FAIL${NC} $*"; exit 1; }
info() { echo -e "${CYAN}$*${NC}"; }

TMP=$(mktemp -d /tmp/iedb-mix-test.XXXXXX)
cleanup() {
  kill $MIX_PID 2>/dev/null || true; wait 2>/dev/null || true
  if ! $KEEP_TMP; then rm -rf "$TMP"; else echo "TMP=$TMP"; fi
}
trap cleanup EXIT

BIN="${IEDB_BIN:-./target/debug/iedb}"
if [ ! -x "$BIN" ]; then
  info "Building iedb..."
  (cd "$(dirname "$0")/.." && cargo build)
fi

mkdir -p "$TMP/data"

cat > "$TMP/iedb.toml" << EOF
[server]
host = "127.0.0.1"
port = 18080
max_body_bytes = 102400

[data]
dir = "$TMP/data"

[wal]
flush_interval_secs = 1
max_write_buffer_ops = 100000

[flush]
snapshot_interval = "2s"
backend = "local"
memory_limit = "512MB"

[query]
data_dir = "$TMP/data"
query_timeout_secs = 30
max_rows = 100
max_concurrent_queries = 4

[compaction]
enabled = false

[metadata]
db_path = "$TMP/data/iedb.db"
EOF

info "Starting mix mode..." && "$BIN" --mode mix --config "$TMP/iedb.toml" &
MIX_PID=$!; sleep 2

S=0; TOTAL=0
check_eq() { TOTAL=$((TOTAL+1)); local v="$1"; local e="$2"; local m="$3"
  if [ "$v" = "$e" ]; then S=$((S+1)); pass "$m"; else fail "$m (expected '$e', got '$v')"; fi; }
check_ge() { TOTAL=$((TOTAL+1)); local v="$1"; local e="$2"; local m="$3"
  if [ "$v" -ge "$e" ] 2>/dev/null; then S=$((S+1)); pass "$m"; else fail "$m (expected >=$e, got $v)"; fi; }
check_ok() { TOTAL=$((TOTAL+1)); local v="$1"; local m="$2"
  if [ "$v" = "ok" ] || [ "$v" = "True" ] || [ "$v" = "200" ]; then S=$((S+1)); pass "$m"; else fail "$m (got: $v)"; fi; }

# ── 1. Health ──
info "1. Health"
check_eq "$(curl -sf 'http://127.0.0.1:18080/health')" "ok" "/health"
check_eq "$(curl -sf 'http://127.0.0.1:18081/health')" "ok" "agent port /health"

# ── 2. Write ──
info "2. Write"
OLD_TS=$(( ($(date +%s) - 60) * 1000000000 ))
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:18081/write?db=mydb" \
  -d "cpu,host=srv01 cpu=75.5,mem=62.3 $OLD_TS
cpu,host=srv02 cpu=30.0,mem=40.1 $((OLD_TS+1000000000))
cpu,host=srv01 cpu=50.0,mem=80.0 $((OLD_TS+2000000000))")" "204" "write 204"

# ── 3. Buffer query ──
info "3. Buffer query"
check_eq "$(curl -sf 'http://127.0.0.1:18081/query?db=mydb&table=cpu' | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "3" "buffer: 3 rows"
check_eq "$(curl -sf 'http://127.0.0.1:18081/query?db=mydb&table=cpu&tag=host=srv01' | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "2" "buffer tag filter srv01"

# ── 4. Multiple tables ──
info "4. Multiple tables"
curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=mydb" -d "mem,host=srv01 used=40.0,free=60.0 $OLD_TS"
curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=otherdb" -d "temp,location=room1 value=22.5 $OLD_TS"
pass "writes to multiple tables"

# ── 5. Local snapshot ──
info "5. Local snapshot"
sleep 12
check_ge "$(find "$TMP/data" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')" "1" "local parquet files"

# ── 6. SQL query ──
info "6. SQL query"
SQL=$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" -H "Content-Type: application/json" -d '{"sql":"SELECT * FROM mydb.cpu ORDER BY time"}')
check_eq "$(echo "$SQL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "3" "SQL: 3 rows"

# ── 7. Aggregate ──
info "7. Aggregate"
AGG=$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" -H "Content-Type: application/json" -d '{"sql":"SELECT host, COUNT(*) AS cnt, AVG(cpu) AS avg FROM mydb.cpu GROUP BY host ORDER BY host"}')
check_eq "$(echo "$AGG" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "2" "SQL: GROUP BY 2 groups"

# ── 8. Metadata ──
info "8. Metadata"
check_ge "$(curl -sf 'http://127.0.0.1:18080/api/v1/metadata/databases' | python3 -c "import sys,json; print(len(json.load(sys.stdin)['databases']))")" "1" "metadata: databases"
check_ge "$(curl -sf 'http://127.0.0.1:18080/api/v1/metadata/tables?db=mydb' | python3 -c "import sys,json; print(len(json.load(sys.stdin)['tables']))")" "2" "metadata: mydb tables"
check_ok "$(curl -sf 'http://127.0.0.1:18080/api/v1/metadata/table?db=mydb&table=cpu' | python3 -c "import sys,json; print(len(json.load(sys.stdin)['fields'])>=2)")" "metadata: cpu fields"

# ── 9. Schema evolution ──
info "9. Schema evolution"
NEW_TS=$(( ($(date +%s) - 30) * 1000000000 ))
curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=mydb" -d "cpu,host=srv03 cpu=10.0,mem=20.0,temp=80.0 $NEW_TS"
sleep 12
check_ok "$(curl -sf 'http://127.0.0.1:18080/api/v1/metadata/table?db=mydb&table=cpu' | python3 -c "import sys,json; d=json.load(sys.stdin); print(any(f['name']=='temp' for f in d['fields']))")" "schema: temp field"

# ── 10. Truncation ──
info "10. Truncation"
for i in $(seq 0 150); do
  TS=$(( ($(date +%s) - 3600 + $i) * 1000000000 ))
  curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=bigdb" -d "big,idx=x val=$i $TS" || true
done
sleep 10
check_ok "$(curl -sf -X POST 'http://127.0.0.1:18080/api/v1/query' -H 'Content-Type: application/json' -d '{"sql":"SELECT * FROM bigdb.big"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['truncated'])")" "truncation"

# ── 11. Error handling ──
info "11. Error responses"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X GET 'http://127.0.0.1:18080/api/v1/query')" "405" "405: GET query"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:18080/api/v1/query' -H 'Content-Type: application/json' -d '{"sql":"BROKEN"}')" "422" "422: bad SQL"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:18080/api/v1/query' -H 'Content-Type: application/json' -d '{}')" "400" "400: empty SQL"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:18081/write?db=test' -d "$(python3 -c "print('x'*200000)")")" "413" "413: oversized body"

# ── 12. Mix has no agent management API ──
info "12. No agent management in mix"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:18080/api/v1/agents')" "404" "404: /agents not in mix"

# ── 13. SQL WHERE/time filter ──
info "13. SQL filters"
F=$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM mydb.cpu WHERE host='"'"'srv01'"'"' ORDER BY time"}')
check_eq "$(echo "$F" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "2" "SQL WHERE host=srv01"

# ── 14. Write query params ──
info "14. Write with default db"
curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write" \
  -d "nodef,src=x val=1.0 $OLD_TS"
sleep 12
# Should be queryable as "default" database
ND=$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/databases")
check_ok "$(echo "$ND" | python3 -c "import sys,json; dbs=[d['name'] for d in json.load(sys.stdin)['databases']]; print('default' in dbs)")" "default db from paramless write"

# ── 15. Ingestion query params ──
info "15. Ingest with query params"
SAMPLE=$(find "$TMP/data" -name "*.parquet" 2>/dev/null | head -1)
if [ -n "$SAMPLE" ]; then
  check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    'http://127.0.0.1:18080/api/v1/ingest/parquet?db=ingestdb&measurement=direct' \
    -H 'Content-Type: application/octet-stream' \
    -H 'x-agent-id: mix-test' \
    --data-binary "@$SAMPLE")" "200" "ingest via query params"
fi

echo ""
echo "========================================="
echo -e "  ${GREEN}Mix pipeline: $S/$TOTAL passed${NC}"
if [ "$S" = "$TOTAL" ]; then echo "  ALL PASSED"; else echo -e "  ${RED}$((TOTAL-S)) FAILURES${NC}"; exit 1; fi
echo "========================================="
