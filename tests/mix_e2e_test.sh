#!/bin/bash
# iedb mix mode integration test — agent + server in one process
# Usage: ./tests/mix_e2e_test.sh [--keep-tmp]

set -e
KEEP_TMP=false
if [ "$1" = "--keep-tmp" ]; then KEEP_TMP=true; fi

TMP=$(mktemp -d /tmp/iedb-mix-test.XXXXXX)
trap "if ! \$KEEP_TMP; then rm -rf $TMP; fi; kill 0" EXIT

BIN="${IEDB_BIN:-./target/debug/iedb}"
if [ ! -x "$BIN" ]; then
  echo "Building iedb..."
  (cd "$(dirname "$0")/.." && cargo build)
fi

mkdir -p "$TMP/data"

cat > "$TMP/iedb.toml" << EOF
[server]
host = "127.0.0.1"
port = 18080
max_body_bytes = 10485760

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
max_rows = 10000
max_concurrent_queries = 4

[compaction]
enabled = false

[metadata]
db_path = "$TMP/data/iedb.db"
EOF

echo "=== Start Mix Mode ==="
"$BIN" --mode mix --config "$TMP/iedb.toml" &
MIX_PID=$!
sleep 2

# ── Test 1: Health ──
echo ""
echo "=== Test 1: Health ==="
HEALTH=$(curl -sf "http://127.0.0.1:18080/health")
if [ "$HEALTH" != "ok" ]; then echo "FAIL"; exit 1; fi
echo "PASS ($HEALTH)"

# ── Test 2: Write ──
echo ""
echo "=== Test 2: Write Line Protocol ==="
OLD_TS=$(( ($(date +%s) - 60) * 1000000000 ))
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:18081/write?db=testdb" \
  -d "cpu,host=srv01 cpu=75.5,mem=62.3 $OLD_TS
cpu,host=srv02 cpu=30.0,mem=40.1 $((OLD_TS + 1000000000))")
if [ "$CODE" != "204" ]; then echo "FAIL: expected 204, got $CODE"; exit 1; fi
echo "PASS (HTTP $CODE)"

# ── Test 3: Buffer query ──
echo ""
echo "=== Test 3: Buffer Query ==="
BUF=$(curl -sf "http://127.0.0.1:18081/query?db=testdb&table=cpu")
ROWS=$(echo "$BUF" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")
if [ "$ROWS" != "2" ]; then echo "FAIL: expected 2 rows, got $ROWS"; exit 1; fi
echo "PASS ($ROWS rows)"

# ── Wait for snapshot ──
sleep 10

# ── Test 4: SQL query ──
echo ""
echo "=== Test 4: SQL Query ==="
SQL=$(curl -sf -X POST "http://127.0.0.1:18080/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM testdb.cpu ORDER BY time"}')
echo "$SQL" | python3 -m json.tool | head -15
ROWS=$(echo "$SQL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['rows']))")
if [ "$ROWS" != "2" ]; then echo "FAIL: expected 2 rows, got $ROWS"; exit 1; fi
echo "PASS ($ROWS rows)"

# ── Test 5: Metadata ──
echo ""
echo "=== Test 5: Metadata ==="
DBS=$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/databases")
DB_COUNT=$(echo "$DBS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['databases']))")
if [ "$DB_COUNT" != "1" ]; then echo "FAIL: expected 1 database"; exit 1; fi
TABLES=$(curl -sf "http://127.0.0.1:18080/api/v1/metadata/tables?db=testdb")
TC=$(echo "$TABLES" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['tables']))")
if [ "$TC" != "1" ]; then echo "FAIL: expected 1 table"; exit 1; fi
echo "PASS (1 DB, 1 table)"

echo ""
echo "========================================="
echo "  All tests passed: Mix pipeline OK"
echo "========================================="
