// SQLite connection + schema initialization for the iedb server.
use rusqlite::Connection;
use std::path::Path;

pub struct Db {
    conn: tokio::sync::Mutex<Connection>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        Self::init_schema(&conn)?;
        Ok(Db { conn: tokio::sync::Mutex::new(conn) })
    }

    fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS agents (
                id          TEXT PRIMARY KEY,
                hostname    TEXT NOT NULL,
                arch        TEXT,
                version     TEXT,
                config_json TEXT,
                config_version INTEGER DEFAULT 1,
                target_config_version INTEGER DEFAULT 1,
                registered_at INTEGER NOT NULL,
                last_seen_at INTEGER,
                listen_addr TEXT
            );

            CREATE TABLE IF NOT EXISTS databases (
                name TEXT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS tables (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                db_name     TEXT NOT NULL,
                table_name  TEXT NOT NULL,
                time_min    INTEGER,
                time_max    INTEGER,
                total_rows  INTEGER DEFAULT 0,
                updated_at  INTEGER,
                UNIQUE(db_name, table_name)
            );

            CREATE TABLE IF NOT EXISTS fields (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id   INTEGER NOT NULL REFERENCES tables(id),
                name       TEXT NOT NULL,
                value_type TEXT NOT NULL,
                is_tag     INTEGER DEFAULT 0,
                UNIQUE(table_id, name, is_tag)
            );

            CREATE TABLE IF NOT EXISTS agent_tables (
                agent_id   TEXT NOT NULL REFERENCES agents(id),
                table_id   INTEGER NOT NULL REFERENCES tables(id),
                PRIMARY KEY (agent_id, table_id)
            );

            -- Files absorbed by compaction: a lost-response retry of an
            -- already-merged upload must not re-create the file (the rows
            -- live in the compacted output). Entries pruned after 7 days.
            CREATE TABLE IF NOT EXISTS compaction_tombstones (
                file_name  TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            );
        ")?;

        // Migration: add listen_addr column if it doesn't exist (for DBs created before this feature)
        let has_col: bool = conn
            .prepare("SELECT listen_addr FROM agents LIMIT 0")
            .is_ok();
        if !has_col {
            conn.execute_batch("ALTER TABLE agents ADD COLUMN listen_addr TEXT;")?;
        }

        Ok(())
    }

    pub fn conn(&self) -> &tokio::sync::Mutex<Connection> {
        &self.conn
    }
}
