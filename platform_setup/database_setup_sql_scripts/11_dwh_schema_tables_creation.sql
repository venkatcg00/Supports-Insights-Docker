\c support_insights

-- Create tables in dwh schema
CREATE TABLE IF NOT EXISTS dwh.ds_sources (
    dwh_source_id         SERIAL       PRIMARY KEY,
    source_id             INT          NOT NULL,
    source_name           TEXT         NOT NULL,
    source_file_type      TEXT         NOT NULL,
    dataload_strategy     TEXT         NOT NULL,
    last_loaded_record_id INT,
    description           TEXT,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_ds_sources_source_id
    FOREIGN KEY           (source_id) REFERENCES ds.sources(source_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_ds_sources_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.ds_customer_types (
    dwh_customer_type_id  SERIAL       PRIMARY KEY,
    customer_type_id      INT          NOT NULL,
    customer_type_name    TEXT         NOT NULL,
    source_id             INT          NOT NULL,
    description           TEXT,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_ds_customer_types_customer_type_id
    FOREIGN KEY           (customer_type_id) REFERENCES ds.customer_types(customer_type_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT fk_ds_customer_types_source_id
    FOREIGN KEY           (source_id) REFERENCES ds.sources(source_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_ds_customer_types_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.ds_support_areas (
    dwh_support_area_id   SERIAL       PRIMARY KEY,
    support_area_id       INT          NOT NULL,
    support_area_name     TEXT         NOT NULL,
    source_id             INT          NOT NULL,
    description           TEXT,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_ds_support_areas_support_area_id
    FOREIGN KEY           (support_area_id) REFERENCES ds.support_areas(support_area_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT fk_ds_support_areas_source_id
    FOREIGN KEY           (source_id) REFERENCES ds.sources(source_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_ds_support_areas_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.ds_agents (
    dwh_agent_id          SERIAL       PRIMARY KEY,
    agent_id              INT          NOT NULL UNIQUE, -- Fixed duplicate PRIMARY KEY
    first_name            TEXT         NOT NULL,
    middle_name           TEXT,
    last_name             TEXT         NOT NULL,
    pseudo_code           TEXT         NOT NULL,
    source_id             INT          NOT NULL,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_ds_agents_agent_id
    FOREIGN KEY           (agent_id) REFERENCES ds.agents(agent_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT fk_ds_agents_source_id
    FOREIGN KEY           (source_id) REFERENCES ds.sources(source_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_ds_agents_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.info_object_types (
    dwh_object_type_id    SERIAL       PRIMARY KEY,
    object_type_id        INT          NOT NULL,
    object_type_code      TEXT         NOT NULL,
    description           TEXT,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_info_object_types_object_type_id
    FOREIGN KEY           (object_type_id) REFERENCES info.object_types(object_type_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_info_object_types_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.info_object_names (
    dwh_object_name_id    SERIAL       PRIMARY KEY,
    object_name_id        INT          NOT NULL,
    object_name           TEXT         NOT NULL,
    object_type_id        INT          NOT NULL,
    object_load_strategy_id INT       NOT NULL,
    description           TEXT,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_info_object_names_object_name_id
    FOREIGN KEY           (object_name_id) REFERENCES info.object_names(object_name_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT fk_info_object_names_object_type_id
    FOREIGN KEY           (object_type_id) REFERENCES info.object_types(object_type_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT fk_info_object_names_object_load_strategy_id
    FOREIGN KEY           (object_load_strategy_id) REFERENCES info.object_load_strategies(object_load_strategy_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_info_object_names_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.info_object_load_strategies (
    dwh_object_load_strategy_id SERIAL PRIMARY KEY,
    object_load_strategy_id INT      NOT NULL,
    object_load_strategy_code TEXT   NOT NULL,
    description           TEXT,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_info_object_load_strategies_object_load_strategy_id
    FOREIGN KEY           (object_load_strategy_id) REFERENCES info.object_load_strategies(object_load_strategy_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_info_object_load_strategies_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.info_data_dictionary (
    dwh_data_dictionary_id SERIAL      PRIMARY KEY,
    data_dictionary_id    INT          NOT NULL,
    source_id             INT          NOT NULL,
    field_name            TEXT         NOT NULL,
    data_type             TEXT         NOT NULL,
    values_allowed        TEXT         NOT NULL,
    description           TEXT,
    example               TEXT,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date              TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    dml_operation         TEXT         NOT NULL,
    CONSTRAINT fk_info_data_dictionary_data_dictionary_id
    FOREIGN KEY           (data_dictionary_id) REFERENCES info.data_dictionary(data_dictionary_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT fk_info_data_dictionary_source_id
    FOREIGN KEY           (source_id) REFERENCES ds.sources(source_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_info_data_dictionary_end_date
    CHECK                 (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS dwh.aud_dag_runs (
    dwh_dag_run_id        SERIAL       PRIMARY KEY,
    dag_run_id            INT          NOT NULL,
    airflow_dag_run_id    TEXT         NOT NULL UNIQUE,
    source_id             INT          NOT NULL,
    dag_run_status        TEXT         NOT NULL,
    run_start_date        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_end_date          TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    batch_count           INT,
    insert_count          INT,
    update_count          INT,
    duplicate_count       INT,
    valid_count           INT,
    validity_percentage   DECIMAL(5,2),
    run_duration          INTERVAL,
    dml_operation         TEXT         NOT NULL,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_aud_dag_runs_dag_run_id
    FOREIGN KEY           (dag_run_id) REFERENCES aud.dag_runs(dag_run_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT fk_aud_dag_runs_source_id
    FOREIGN KEY           (source_id) REFERENCES ds.sources(source_id)
                          ON DELETE RESTRICT
                          ON UPDATE CASCADE
                          ,
    CONSTRAINT chk_aud_dag_runs_end_date
    CHECK                 (run_end_date >= run_start_date) 
);

-- Indexes for dwh.ds_sources
CREATE INDEX IF NOT EXISTS idx_dwh_ds_sources_source_id ON dwh.ds_sources(source_id);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_sources_temporal ON dwh.ds_sources(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_sources_is_active ON dwh.ds_sources(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.ds_customer_types
CREATE INDEX IF NOT EXISTS idx_dwh_ds_customer_types_customer_type_id ON dwh.ds_customer_types(customer_type_id);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_customer_types_source_id ON dwh.ds_customer_types(source_id);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_customer_types_temporal ON dwh.ds_customer_types(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_customer_types_is_active ON dwh.ds_customer_types(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.ds_support_areas
CREATE INDEX IF NOT EXISTS idx_dwh_ds_support_areas_support_area_id ON dwh.ds_support_areas(support_area_id);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_support_areas_source_id ON dwh.ds_support_areas(source_id);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_support_areas_temporal ON dwh.ds_support_areas(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_support_areas_is_active ON dwh.ds_support_areas(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.ds_agents
CREATE INDEX IF NOT EXISTS idx_dwh_ds_agents_agent_id ON dwh.ds_agents(agent_id);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_agents_source_id ON dwh.ds_agents(source_id);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_agents_temporal ON dwh.ds_agents(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_ds_agents_is_active ON dwh.ds_agents(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.info_object_types
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_types_object_type_id ON dwh.info_object_types(object_type_id);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_types_temporal ON dwh.info_object_types(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_types_is_active ON dwh.info_object_types(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.info_object_names
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_names_object_name_id ON dwh.info_object_names(object_name_id);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_names_object_type_id ON dwh.info_object_names(object_type_id);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_names_object_load_strategy_id ON dwh.info_object_names(object_load_strategy_id);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_names_temporal ON dwh.info_object_names(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_names_is_active ON dwh.info_object_names(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.info_object_load_strategies
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_load_strategies_object_load_strategy_id ON dwh.info_object_load_strategies(object_load_strategy_id);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_load_strategies_temporal ON dwh.info_object_load_strategies(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_info_object_load_strategies_is_active ON dwh.info_object_load_strategies(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.info_data_dictionary
CREATE INDEX IF NOT EXISTS idx_dwh_info_data_dictionary_data_dictionary_id ON dwh.info_data_dictionary(data_dictionary_id);
CREATE INDEX IF NOT EXISTS idx_dwh_info_data_dictionary_source_id ON dwh.info_data_dictionary(source_id);
CREATE INDEX IF NOT EXISTS idx_dwh_info_data_dictionary_temporal ON dwh.info_data_dictionary(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_info_data_dictionary_is_active ON dwh.info_data_dictionary(is_active) WHERE is_active = TRUE;

-- Indexes for dwh.aud_dag_runs
CREATE INDEX IF NOT EXISTS idx_dwh_aud_dag_runs_dag_run_id ON dwh.aud_dag_runs(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_dwh_aud_dag_runs_source_id ON dwh.aud_dag_runs(source_id);
CREATE INDEX IF NOT EXISTS idx_dwh_aud_dag_runs_temporal ON dwh.aud_dag_runs(run_start_date, run_end_date);
CREATE INDEX IF NOT EXISTS idx_dwh_aud_dag_runs_status ON dwh.aud_dag_runs(dag_run_status);
CREATE INDEX IF NOT EXISTS idx_dwh_aud_dag_runs_is_active ON dwh.aud_dag_runs(is_active) WHERE is_active = TRUE;