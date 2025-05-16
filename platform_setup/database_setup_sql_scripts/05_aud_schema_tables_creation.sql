\c support_insights

-- Create tables in aud schema
CREATE TABLE IF NOT EXISTS aud.dag_runs (
    dag_run_id           SERIAL       PRIMARY KEY,
    airflow_dag_run_id   TEXT         NOT NULL UNIQUE,
    source_id            INT          NOT NULL,
    dag_run_status       TEXT         NOT NULL,
    run_start_date       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_end_date         TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    batch_count          INT,
    insert_count         INT,
    update_count         INT,
    duplicate_count      INT,
    valid_count          INT,
    validity_percentage  DECIMAL(5,2), -- Adjusted precision for clarity
    run_duration         INTERVAL,
    CONSTRAINT fk_dag_runs_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_dag_runs_end_date
    CHECK                  (run_end_date >= run_start_date) 
);

CREATE TABLE IF NOT EXISTS aud.data_error_history (
    data_error_id         SERIAL       PRIMARY KEY,
    dag_run_id            INT          NOT NULL,
    source_id             INT          NOT NULL,
    error_type            TEXT         NOT NULL,
    error_description     TEXT         NOT NULL,
    source_record_id      INT          NOT NULL,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_data_error_history_dag_run_id
    FOREIGN KEY            (dag_run_id) REFERENCES aud.dag_runs(dag_run_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_data_error_history_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_data_error_history_end_date
    CHECK                  (end_date >= start_date) 
);

-- Indexes for aud.dag_runs
CREATE INDEX IF NOT EXISTS idx_dag_runs_source_id ON aud.dag_runs(source_id);
CREATE INDEX IF NOT EXISTS idx_dag_runs_status ON aud.dag_runs(dag_run_status);
CREATE INDEX IF NOT EXISTS idx_dag_runs_temporal ON aud.dag_runs(run_start_date, run_end_date);
CREATE INDEX IF NOT EXISTS idx_dag_runs_validity_percentage ON aud.dag_runs(validity_percentage);

-- Indexes for aud.data_error_history
CREATE INDEX IF NOT EXISTS idx_data_error_history_dag_run_id ON aud.data_error_history(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_data_error_history_source_id ON aud.data_error_history(source_id);
CREATE INDEX IF NOT EXISTS idx_data_error_history_error_type ON aud.data_error_history(error_type);
CREATE INDEX IF NOT EXISTS idx_data_error_history_is_active ON aud.data_error_history(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_data_error_history_temporal ON aud.data_error_history(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_data_error_history_source_record_id ON aud.data_error_history(source_record_id);