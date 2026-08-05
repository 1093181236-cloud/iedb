#!/bin/bash
# iedb CS integration test — server + agent full pipeline
# Usage: ./tests/cs_e2e_test.sh [--keep-tmp]

set -e
KEEP_TMP=false
if [ "$1" = "--keep-tmp" ]; then KEEP_TMP=true; fi

TMP=$(mktemp -d /tmp/iedb-cs-test.XXXXXX)
trap "if ! \$KEEP_TMP; then rm -rf $TMP; fi; kill 0" EXIT

BIN="${IEDB_BIN:-./target/debug/iedb}"
if [ ! -x "$BIN" ]; then
  echo "Building iedb..."
  (cd "$(dirname "$0")/.." && cargo build)
fi

mkdir -p "$TMP/server-data" "$TMP/agent-data"

# ── Server config ──
cat > "$TMP/server.toml" << EOF
[server]
host = "127.0.0.1"
port = 18080
max_body_bytes = 10485760

[data]
dir = "$TMP/server-data"

[query]
data_dir = "$TMP/server-data"
query_timeout_secs = 30
max_rows = 10000
max_concurrent_queries = 4

[compaction]
enabled = false

[agents]
heartbeat_timeout_secs = 30
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

echo "=== Start Server ==="
"$BIN" --mode server --config "$TMP/server.toml" &
SERVER_PID=$!
sleep 2

echo "=== Start Agent ==="
"$BIN" --mode agent --config "$TMP/agent.toml" &
AGENT_PID=$!
sleep 3

# ── Test 1: Agent registration ──
echo ""
echo "=== Test 1: Agent Registration ==="
AGENTS=$(curl -sf "http://127.0.0.1:18080/api/v1/agents")
echo "$AGENTS" | python3 -m json.tool
STATUS=$(echo "$AGENTS" | python3 -c "import sys,json; print(json.load(sys.stdin)['agents'][0]['status'])")
if [ "$STATUS" != "online" ]; then echo "FAIL: agent not online"; exit 1; fi
echo "PASS"

# ── Test 2: Write Line Protocol ──
echo ""
echo "=== Test 2: Write Line Protocol ==="
OLD_TS=$(( ($(date +%s) - 60) * 1000000000 ))
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:18081/write?db=mydb" \
  -d "cpu,host=srv01 cpu=75.5,mem=62.3 $OLD_TS
cpu,host=srv02 cpu=30.0,mem=40.1 $((OLD_TS + 1000000000))")
if [ "$CODE" != "204" ]; then echo "FAIL: expected 204, got $CODE"; exit 1; fi
echo "PASS (HTTP $CODE)"

# ── Test 3: Buffer query ──
echo ""
echo "=== Test 3: Buffer Query ==="
BUF=$(curl -sf "http://127.0.0.1:18081/query?db=mydb&table=cpu")
ROWS=$(echo "$BUF" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")
if [ "$ROWS" != "2" ]; then echo "FAIL: expected 2 rows, got $ROWS"; exit 1; fi
echo "PASS ($ROWS rows)"

# ── Wait for snapshot + upload ──
sleep 10

# ── Test 4: Server SQL query ──
echo ""
echo "=== Test 4: SQL Query via DataFusion ==="
SQL=$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM mydb.cpu ORDER BY time"}')
echo "$SQL" | python3 -m json.tool | head -15
ROWS=$(echo "$SQL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")
TRUNC=$(echo "$SQL" | python3 -c "import sys,json; print(json.load(sys.stdin)['truncated'])")
if [ "$ROWS" != "2" ]; then echo "FAIL: expected 2 rows, got $ROWS"; exit 1; fi
if [ "$TRUNC" != "False" ]; then echo "FAIL: unexpected truncation"; exit 1; fi
echo "PASS ($ROWS rows, not truncated)"

# ── Test 5: Metadata ──
echo ""
echo "=== Test 5: Metadata ==="
DBS=$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/databases")
echo "$DBS" | python3 -m json.tool
DB_COUNT=$(echo "$DBS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['databases']))")
if [ "$DB_COUNT" != "1" ]; then echo "FAIL: expected 1 database"; exit 1; fi

TABLE=$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/tables?db=mydb")
TABLE_COUNT=$(echo "$TABLE" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['tables']))")
if [ "$TABLE_COUNT" != "1" ]; then echo "FAIL: expected 1 table"; exit 1; fi

DETAIL=$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/table?db=mydb&table=cpu")
FIELD_COUNT=$(echo "$DETAIL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['fields']))")
if [ "$FIELD_COUNT" -lt 2 ]; then echo "FAIL: expected at least 2 fields"; exit 1; fi
echo "PASS (1 DB, 1 table, $FIELD_COUNT fields)"

# ── Test 6: Config update ──
echo ""
echo "=== Test 6: Config Update ==="
curl -sf -X PUT "http://127.0.0.1:18080/api/v1/agents/edge-01/config" \
  -H "Content-Type: application/json" \
  -d '{"flush":{"memory_limit":"256MB"}}' | python3 -m json.tool
AGENT_INFO=$(curl -sf "http://127.0.0.1:18080/api/v1/agents/edge-01")
TARGET_VER=$(echo "$AGENT_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin)['target_config_version'])")
if [ "$TARGET_VER" -lt 2 ]; then echo "FAIL: config update not propagated"; exit 1; fi
echo "PASS (target_config_version=$TARGET_VER)"

echo ""
echo "========================================="
echo "  All tests passed: C/S pipeline OK"
echo "========================================="
