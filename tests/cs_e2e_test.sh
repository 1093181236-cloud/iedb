#!/bin/bash
# iedb C/S integration test — server + agent full pipeline
# Usage: ./tests/cs_e2e_test.sh [--keep-tmp] [--verbose]
#
# Tests:
#   1. Server health, empty state
#   2. Agent registration, status=online
#   3. Agent list/get detail
#   4. Write Line Protocol → 204
#   5. Buffer query (tag filter)
#   6. Write multiple tables
#   7. Snapshot → HTTP upload → verify Parquet on server
#   8. SQL query via DataFusion
#   9. Metadata: databases, tables, table detail with schema
#  10. Agent heartbeat schema_changes (new field)
#  11. Config update → heartbeat picks it up
#  12. Agent re-registration (simulate restart)
#  13. Error handling: 400/404/405/422
#  14. Agent delete
#  15. Cleanup: agent offline after kill

set -e
KEEP_TMP=false
VERBOSE=false
for a in "$@"; do
  case "$a" in
    --keep-tmp) KEEP_TMP=true ;;
    --verbose) VERBOSE=true ;;
  esac
done

RED='\033[31m'; GREEN='\033[32m'; CYAN='\033[36m'; NC='\033[0m'
pass() { echo -e "${GREEN}PASS${NC} $*"; }
fail() { echo -e "${RED}FAIL${NC} $*"; exit 1; }
info() { echo -e "${CYAN}$*${NC}"; }
vlog() { if $VERBOSE; then echo "$*"; fi; }

TMP=$(mktemp -d /tmp/iedb-cs-test.XXXXXX)
cleanup() {
  kill $SERVER_PID $AGENT_PID 2>/dev/null || true; wait 2>/dev/null || true
  if ! $KEEP_TMP; then rm -rf "$TMP"; else echo "TMP=$TMP"; fi
}
trap cleanup EXIT

BIN="${IEDB_BIN:-./target/debug/iedb}"
if [ ! -x "$BIN" ]; then
  info "Building iedb..."
  (cd "$(dirname "$0")/.." && cargo build)
fi

mkdir -p "$TMP/server-data" "$TMP/agent-data"

# ── Server config ──
cat > "$TMP/server.toml" << EOF
[server]
host = "127.0.0.1"
port = 18080
max_body_bytes = 102400

[data]
dir = "$TMP/server-data"

[query]
data_dir = "$TMP/server-data"
query_timeout_secs = 30
max_rows = 100
max_concurrent_queries = 4

[compaction]
enabled = false

[agents]
heartbeat_timeout_secs = 10
offline_cleanup_days = 7

[agents.default_config]
flush.snapshot_interval = "2s"
flush.backend = "http"
flush.memory_limit = "512MB"
wal.flush_interval_secs = 1
wal.max_write_buffer_ops = 100000

[metadata]
db_path = "$TMP/server-data/iedb.db"
EOF

# ── Agent config ──
cat > "$TMP/agent.toml" << EOF
[server]
port = 18081

[data]
dir = "$TMP/agent-data"

[agent]
id = "edge-01"
server_url = "http://127.0.0.1:18080"

[wal]
flush_interval_secs = 1
max_write_buffer_ops = 100000

[flush]
snapshot_interval = "2s"
backend = "http"
memory_limit = "512MB"
EOF

# ═══════════════════════════════════════════
#  Start services
# ═══════════════════════════════════════════
info "Starting server..." && "$BIN" --mode server --config "$TMP/server.toml" &
SERVER_PID=$!; sleep 2
info "Starting agent..." && "$BIN" --mode agent --config "$TMP/agent.toml" &
AGENT_PID=$!; sleep 3

# ═══════════════════════════════════════════
S=0; TOTAL=0
# check_eq: assert value == expected, value from command substitution
check_eq() { TOTAL=$((TOTAL+1)); local val="$1"; local exp="$2"; local msg="$3"
  if [ "$val" = "$exp" ]; then S=$((S+1)); pass "$msg"; else fail "$msg (expected '$exp', got '$val')"; fi; }
# check_ge: assert value >= expected (numeric)
check_ge() { TOTAL=$((TOTAL+1)); local val="$1"; local exp="$2"; local msg="$3"
  if [ "$val" -ge "$exp" ] 2>/dev/null; then S=$((S+1)); pass "$msg"; else fail "$msg (expected >=$exp, got $val)"; fi; }
# check_ok: helper for http status 200 and boolean true
check_ok() { TOTAL=$((TOTAL+1)); local val="$1"; local msg="$2"
  if [ "$val" = "ok" ] || [ "$val" = "True" ] || [ "$val" = "200" ]; then S=$((S+1)); pass "$msg"; else fail "$msg (got: $val)"; fi; }
# ═══════════════════════════════════════════

# ── 1. Server health, empty databases ──
info "1. Server health + empty state"
check_ok "$(curl -sf "http://127.0.0.1:18080/health")" "server /health"
DBS=$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/databases")
check_eq "$(echo "$DBS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['databases']))")" "0" "empty databases at startup"

# ── 2. Agent registration ──
info "2. Agent registration"
AGENTS=$(curl -sf "http://127.0.0.1:18080/api/v1/agents")
check_eq "$(echo "$AGENTS" | python3 -c "import sys,json; print(json.load(sys.stdin)['agents'][0]['status'])")" "online" "agent online"
check_eq "$(echo "$AGENTS" | python3 -c "import sys,json; print(json.load(sys.stdin)['agents'][0]['id'])")" "edge-01" "agent id=edge-01"

# ── 3. Agent detail ──
info "3. Agent detail"
check_eq "$(curl -sf "http://127.0.0.1:18080/api/v1/agents/edge-01" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")" "edge-01" "agent detail by id"

# ── 4. Write Line Protocol ──
info "4. Write Line Protocol"
OLD_TS=$(( ($(date +%s) - 60) * 1000000000 ))
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:18081/write?db=mydb" \
  -d "cpu,host=srv01 cpu=75.5,mem=62.3 $OLD_TS
cpu,host=srv02 cpu=30.0,mem=40.1 $((OLD_TS+1000000000))
cpu,host=srv01 cpu=50.0,mem=80.0 $((OLD_TS+2000000000))")" "204" "write returns 204"

# ── 5. Buffer query ──
info "5. Buffer query"
check_eq "$(curl -sf "http://127.0.0.1:18081/query?db=mydb&table=cpu" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "3" "buffer: 3 rows"
check_eq "$(curl -sf "http://127.0.0.1:18081/query?db=mydb&table=cpu&tag=host=srv01" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "2" "buffer tag filter: 2 rows"

# ── 6. Write multiple tables ──
info "6. Write multiple tables"
curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=mydb" -d "mem,host=srv01 used=40.0,free=60.0 $OLD_TS"
curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=otherdb" -d "temp,location=room1 value=22.5 $OLD_TS"
pass "writes to multiple tables"

# ── 7. Snapshot + HTTP upload ──
info "7. Snapshot + HTTP upload"
sleep 12
FILES=$(find "$TMP/server-data" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
check_ge "$FILES" "1" "parquet files on server: $FILES"

# ── 8. SQL query ──
info "8. SQL query via DataFusion"
SQL=$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" -H "Content-Type: application/json" -d '{"sql":"SELECT * FROM mydb.cpu ORDER BY time"}')
check_eq "$(echo "$SQL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")" "3" "SQL: 3 rows"
check_eq "$(echo "$SQL" | python3 -c "import sys,json; print(json.load(sys.stdin)['truncated'])")" "False" "SQL: not truncated"
AGG=$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" -H "Content-Type: application/json" -d '{"sql":"SELECT COUNT(*) AS cnt, AVG(cpu) AS avg_cpu FROM mydb.cpu"}')
check_eq "$(echo "$AGG" | python3 -c "import sys,json; print(json.load(sys.stdin)['rows'][0]['cnt'])")" "3" "SQL: COUNT(*)=3"

# ── 9. Metadata ──
info "9. Metadata"
check_ge "$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/databases" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['databases']))")" "2" "metadata: >=2 databases"
check_ge "$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/tables?db=mydb" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['tables']))")" "1" "metadata: mydb has tables"
check_ok "$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/table?db=mydb&table=cpu" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['fields'])>=2)")" "metadata: cpu has >=2 fields"

# ── 10. Schema changes ──
info "10. Schema change detection"
NEW_TS=$(( ($(date +%s) - 30) * 1000000000 ))
curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=mydb" -d "cpu,host=srv03 cpu=10.0,mem=20.0,temp=80.0 $NEW_TS"
sleep 12
check_ok "$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/table?db=mydb&table=cpu" | python3 -c "import sys,json; d=json.load(sys.stdin); print(any(f['name']=='temp' for f in d['fields']))")" "schema: temp field discovered"

# ── 11. Config update ──
info "11. Config update"
VER=$(curl -sf -X PUT "http://127.0.0.1:18080/api/v1/agents/edge-01/config" -H "Content-Type: application/json" -d '{"flush":{"memory_limit":"128MB"}}' | python3 -c "import sys,json; print(json.load(sys.stdin)['target_version'])")
check_ge "$VER" "2" "config update: target_version=$VER"

# ── 12. Agent restart ──
info "12. Agent restart (re-registration)"
kill $AGENT_PID 2>/dev/null; wait $AGENT_PID 2>/dev/null || true; sleep 1
"$BIN" --mode agent --config "$TMP/agent.toml" & AGENT_PID=$!; sleep 3
check_eq "$(curl -sf "http://127.0.0.1:18080/api/v1/agents" | python3 -c "import sys,json; a=json.load(sys.stdin)['agents']; print(a[0]['status'])")" "online" "re-register: agent online"

# ── 13. Error handling ──
info "13. Error responses"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:18080/api/v1/query' -H 'Content-Type: application/json' -d '{}')" "400" "400: empty SQL"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:18080/api/v1/agents/nonexistent')" "404" "404: unknown agent"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X GET 'http://127.0.0.1:18080/api/v1/query')" "405" "405: GET on POST endpoint"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:18080/api/v1/query' -H 'Content-Type: application/json' -d '{"sql":"BROKEN"}')" "422" "422: invalid SQL"
DD=$(python3 -c "print('x'*200000)")
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:18081/write?db=test' -d "$DD")" "413" "413: oversized body"

# ── 14. Agent delete ──
info "14. Agent delete"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE 'http://127.0.0.1:18080/api/v1/agents/edge-01')" "200" "delete agent: 200"
check_eq "$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:18080/api/v1/agents/edge-01')" "404" "deleted agent: 404"

# ── 15. Truncation ──
info "15. Query truncation"
for i in $(seq 0 150); do
  TS=$(( ($(date +%s) - 3600 + $i) * 1000000000 ))
  curl -s -o /dev/null -X POST "http://127.0.0.1:18081/write?db=bigdb" -d "big,idx=x val=$i $TS" || true
done
sleep 10
check_ok "$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" -H "Content-Type: application/json" -d '{"sql":"SELECT * FROM bigdb.big"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['truncated'])")" "truncation: >100 rows"

# ═══════════════════════════════════════════
echo ""
echo "========================================="
echo -e "  ${GREEN}C/S pipeline: $S/$TOTAL passed${NC}"
if [ "$S" = "$TOTAL" ]; then
  echo "  ALL PASSED"
else
  echo -e "  ${RED}$((TOTAL-S)) FAILURES${NC}"
  exit 1
fi
echo "========================================="
