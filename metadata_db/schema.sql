-- =============================================================================
-- Data Lineage & Observability Dashboard — Metadata Store Schema
-- Run this against the DB named in .env as DB_NAME (e.g. olist_lineage_metadata)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- pipeline_runs
-- One row per execution of a pipeline job. This is the "heartbeat" table --
-- everything else (lineage edges, DQ results) ties back to a specific run_id
-- so you can always answer "as of which run was this true?"
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name        VARCHAR(100)   NOT NULL,      -- e.g. 'load_customers'
    started_at      DATETIME       NOT NULL,
    finished_at     DATETIME       NULL,
    status          ENUM('running', 'success', 'failed') NOT NULL DEFAULT 'running',
    rows_read       BIGINT         NULL,
    rows_written    BIGINT         NULL,
    duration_seconds DECIMAL(10,2) NULL,
    error_message   TEXT           NULL,
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_job_name (job_name),
    INDEX idx_started_at (started_at)
);

-- -----------------------------------------------------------------------------
-- lineage_edges
-- One row per source-table -> target-table relationship, auto-extracted by
-- the AST parser (Day 2) from the PySpark job scripts.
-- source_type distinguishes edges found automatically vs. added by hand.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lineage_edges (
    edge_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name        VARCHAR(100)   NOT NULL,      -- which script produced this edge
    source_table    VARCHAR(150)   NOT NULL,
    target_table    VARCHAR(150)   NOT NULL,
    source_type     ENUM('auto_parsed', 'manual_override') NOT NULL DEFAULT 'auto_parsed',
    detected_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_edge (job_name, source_table, target_table),
    INDEX idx_source_table (source_table),
    INDEX idx_target_table (target_table)
);

-- -----------------------------------------------------------------------------
-- lineage_overrides
-- Manual entries for source->target mappings the AST parser could NOT
-- resolve automatically (e.g. dynamically built table names). This is the
-- "safety net" table -- every row here represents a documented parser gap,
-- not a silent one.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lineage_overrides (
    override_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name        VARCHAR(100)   NOT NULL,
    source_table    VARCHAR(150)   NOT NULL,
    target_table    VARCHAR(150)   NOT NULL,
    reason          TEXT           NOT NULL,      -- why the parser couldn't find this automatically
    added_by        VARCHAR(100)   NOT NULL DEFAULT 'manual',
    added_at        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- dq_check_results
-- One row per data quality check execution, tied to a specific run_id and
-- table. This is what makes data quality auditable -- every pass/fail has a
-- timestamp, a run, and a table attached to it.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_check_results (
    check_result_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id          BIGINT         NOT NULL,
    table_name      VARCHAR(150)   NOT NULL,
    check_type      ENUM('null_check', 'duplicate_check', 'referential_integrity',
                         'schema_drift', 'freshness') NOT NULL,
    column_name     VARCHAR(150)   NULL,          -- NULL for table-level checks
    status          ENUM('pass', 'fail', 'warn') NOT NULL,
    detail          TEXT           NULL,           -- e.g. 'null_rate=2.3%, threshold=1.0%'
    checked_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    INDEX idx_table_name (table_name),
    INDEX idx_status (status),
    INDEX idx_checked_at (checked_at)
);
