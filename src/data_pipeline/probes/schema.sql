CREATE TABLE IF NOT EXISTS probe_results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL,           -- passed/failed/warning
    summary     TEXT NOT NULL,
    stats_json  TEXT NOT NULL,
    run_at      TEXT NOT NULL            -- ISO8601 字符串，由调用方传入
);

CREATE INDEX IF NOT EXISTS idx_probe_results_name_run_at
    ON probe_results(name, run_at);
