\c support_insights

-- Create tables in prs schema
CREATE TABLE IF NOT EXISTS prs.client_alpha_cs_data (
    prs_record_id          SERIAL       PRIMARY KEY,
    source_id              INT          NOT NULL,
    dag_run_id             INT          NOT NULL,
    source_system_identifier TEXT        NOT NULL,
    source_record_id       INT,
    interaction_id         INT,
    support_category       TEXT,
    agent_pseudo_name      TEXT,
    contact_date           TIMESTAMP,
    interaction_status     TEXT,
    interaction_type       TEXT,
    type_of_customer       TEXT,
    interaction_duration   INT,
    total_time             INT,
    status_of_customer_incident TEXT,
    resolved_in_first_contact BOOLEAN, -- Fixed to BOOLEAN
    solution_type          TEXT,
    rating                 INTEGER, -- Fixed to INTEGER
    hash_key               TEXT         NOT NULL,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_client_alpha_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_client_alpha_dag_run_id
    FOREIGN KEY            (dag_run_id) REFERENCES aud.dag_runs(dag_run_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_client_alpha_end_date
    CHECK                  (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS prs.client_beta_cs_data (
    prs_record_id          SERIAL       PRIMARY KEY,
    source_id              INT          NOT NULL,
    dag_run_id             INT          NOT NULL,
    source_system_identifier TEXT        NOT NULL,
    support_identifier     INT,
    contact_regarding      TEXT,
    agent_code             TEXT,
    date_of_interaction    TIMESTAMP,
    status_of_interaction  TEXT,
    type_of_interaction    TEXT,
    customer_type          TEXT,
    contact_duration       INTERVAL,
    after_contact_work_time INTERVAL,
    incident_status        TEXT,
    first_contact_solve    BOOLEAN,
    type_of_resolution     TEXT,
    support_rating         INTEGER,
    time_stamp             TIMESTAMP,
    hash_key               TEXT         NOT NULL,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_client_beta_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_client_beta_dag_run_id
    FOREIGN KEY            (dag_run_id) REFERENCES aud.dag_runs(dag_run_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_client_beta_end_date
    CHECK                  (end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS prs.client_gamma_cs_data (
    prs_record_id          SERIAL       PRIMARY KEY,
    source_id              INT          NOT NULL,
    dag_run_id             INT          NOT NULL,
    source_system_identifier TEXT        NOT NULL,
    ticket_identifier      INT,
    support_category       TEXT,
    agent_name             TEXT,
    date_of_call           TIMESTAMP,
    call_status            TEXT,
    call_type              TEXT,
    type_of_customer       TEXT,
    duration               INT,
    work_time              INT,
    ticket_status          TEXT,
    resolved_in_first_contact BOOLEAN,
    resolution_category    TEXT,
    rating                 TEXT,
    hash_key               TEXT         NOT NULL,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_client_gamma_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_client_gamma_dag_run_id
    FOREIGN KEY            (dag_run_id) REFERENCES aud.dag_runs(dag_run_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_client_gamma_end_date
    CHECK                  (end_date >= start_date)
);

-- Indexes for prs.client_alpha_cs_data
CREATE INDEX IF NOT EXISTS idx_client_alpha_source_id ON prs.client_alpha_cs_data(source_id);
CREATE INDEX IF NOT EXISTS idx_client_alpha_dag_run_id ON prs.client_alpha_cs_data(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_client_alpha_source_system_identifier ON prs.client_alpha_cs_data(source_system_identifier);
CREATE INDEX IF NOT EXISTS idx_client_alpha_temporal ON prs.client_alpha_cs_data(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_client_alpha_is_active ON prs.client_alpha_cs_data(is_active) WHERE is_active = TRUE;

-- Indexes for prs.client_beta_cs_data
CREATE INDEX IF NOT EXISTS idx_client_beta_source_id ON prs.client_beta_cs_data(source_id);
CREATE INDEX IF NOT EXISTS idx_client_beta_dag_run_id ON prs.client_beta_cs_data(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_client_beta_source_system_identifier ON prs.client_beta_cs_data(source_system_identifier);
CREATE INDEX IF NOT EXISTS idx_client_beta_temporal ON prs.client_beta_cs_data(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_client_beta_is_active ON prs.client_beta_cs_data(is_active) WHERE is_active = TRUE;

-- Indexes for prs.client_gamma_cs_data
CREATE INDEX IF NOT EXISTS idx_client_gamma_source_id ON prs.client_gamma_cs_data(source_id);
CREATE INDEX IF NOT EXISTS idx_client_gamma_dag_run_id ON prs.client_gamma_cs_data(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_client_gamma_source_system_identifier ON prs.client_gamma_cs_data(source_system_identifier);
CREATE INDEX IF NOT EXISTS idx_client_gamma_temporal ON prs.client_gamma_cs_data(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_client_gamma_is_active ON prs.client_gamma_cs_data(is_active) WHERE is_active = TRUE;