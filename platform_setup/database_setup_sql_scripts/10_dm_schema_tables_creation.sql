\c support_insights

-- Create partitioned table in dm schema
CREATE TABLE IF NOT EXISTS dm.customer_support_fact (
    fact_id                SERIAL,
    source_id              INT          NOT NULL,
    source_record_id       INT          NOT NULL,
    source_system_identifier TEXT        NOT NULL,
    agent_id               INT,
    interaction_date       TIMESTAMP,
    support_area_id        INT,
    interaction_status     TEXT,
    interaction_type       TEXT,
    customer_type_id       INT,
    handle_time            INT,
    work_time              INT,
    first_contact_resolution BOOLEAN,
    query_status           TEXT,
    solution_type          TEXT,
    customer_rating        INTEGER,
    dag_run_id             INT          NOT NULL,
    is_valid               BOOLEAN      NOT NULL,
    is_active              BOOLEAN      NOT NULL,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_customer_support_fact_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_customer_support_fact_agent_id
    FOREIGN KEY            (agent_id) REFERENCES ds.agents(agent_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_customer_support_fact_support_area_id
    FOREIGN KEY            (support_area_id) REFERENCES ds.support_areas(support_area_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_customer_support_fact_customer_type_id
    FOREIGN KEY            (customer_type_id) REFERENCES ds.customer_types(customer_type_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_customer_support_fact_dag_run_id
    FOREIGN KEY            (dag_run_id) REFERENCES aud.dag_runs(dag_run_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_customer_support_fact_end_date
    CHECK                  (end_date >= start_date) 
) PARTITION BY LIST (source_id);

-- Create partitions for specific source_id values
CREATE TABLE IF NOT EXISTS dm.customer_support_fact_alpha
    PARTITION OF dm.customer_support_fact
    FOR VALUES IN (1); -- client_alpha

CREATE TABLE IF NOT EXISTS dm.customer_support_fact_beta
    PARTITION OF dm.customer_support_fact
    FOR VALUES IN (2); -- client_beta

CREATE TABLE IF NOT EXISTS dm.customer_support_fact_gamma
    PARTITION OF dm.customer_support_fact
    FOR VALUES IN (3); -- client_gamma

-- Create default partition for source_id = 0 and other unexpected values
CREATE TABLE IF NOT EXISTS dm.customer_support_fact_default
    PARTITION OF dm.customer_support_fact
    DEFAULT; -- Handles source_id = 0, 4, NULL, etc.

-- Indexes for dm.customer_support_fact_alpha
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_fact_id ON dm.customer_support_fact_alpha(fact_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_source_id ON dm.customer_support_fact_alpha(source_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_dag_run_id ON dm.customer_support_fact_alpha(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_agent_id ON dm.customer_support_fact_alpha(agent_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_support_area_id ON dm.customer_support_fact_alpha(support_area_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_customer_type_id ON dm.customer_support_fact_alpha(customer_type_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_source_system_identifier ON dm.customer_support_fact_alpha(source_system_identifier);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_source_record_id ON dm.customer_support_fact_alpha(source_record_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_interaction_date ON dm.customer_support_fact_alpha(interaction_date);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_interaction_status ON dm.customer_support_fact_alpha(interaction_status);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_is_active ON dm.customer_support_fact_alpha(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_alpha_temporal ON dm.customer_support_fact_alpha(start_date, end_date);

-- Indexes for dm.customer_support_fact_beta
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_fact_id ON dm.customer_support_fact_beta(fact_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_source_id ON dm.customer_support_fact_beta(source_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_dag_run_id ON dm.customer_support_fact_beta(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_agent_id ON dm.customer_support_fact_beta(agent_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_support_area_id ON dm.customer_support_fact_beta(support_area_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_customer_type_id ON dm.customer_support_fact_beta(customer_type_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_source_system_identifier ON dm.customer_support_fact_beta(source_system_identifier);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_source_record_id ON dm.customer_support_fact_beta(source_record_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_interaction_date ON dm.customer_support_fact_beta(interaction_date);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_interaction_status ON dm.customer_support_fact_beta(interaction_status);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_is_active ON dm.customer_support_fact_beta(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_beta_temporal ON dm.customer_support_fact_beta(start_date, end_date);

-- Indexes for dm.customer_support_fact_gamma
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_fact_id ON dm.customer_support_fact_gamma(fact_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_source_id ON dm.customer_support_fact_gamma(source_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_dag_run_id ON dm.customer_support_fact_gamma(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_agent_id ON dm.customer_support_fact_gamma(agent_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_support_area_id ON dm.customer_support_fact_gamma(support_area_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_customer_type_id ON dm.customer_support_fact_gamma(customer_type_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_source_system_identifier ON dm.customer_support_fact_gamma(source_system_identifier);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_source_record_id ON dm.customer_support_fact_gamma(source_record_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_interaction_date ON dm.customer_support_fact_gamma(interaction_date);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_interaction_status ON dm.customer_support_fact_gamma(interaction_status);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_is_active ON dm.customer_support_fact_gamma(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_gamma_temporal ON dm.customer_support_fact_gamma(start_date, end_date);

-- Indexes for dm.customer_support_fact_default
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_fact_id ON dm.customer_support_fact_default(fact_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_source_id ON dm.customer_support_fact_default(source_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_dag_run_id ON dm.customer_support_fact_default(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_agent_id ON dm.customer_support_fact_default(agent_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_support_area_id ON dm.customer_support_fact_default(support_area_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_customer_type_id ON dm.customer_support_fact_default(customer_type_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_source_system_identifier ON dm.customer_support_fact_default(source_system_identifier);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_source_record_id ON dm.customer_support_fact_default(source_record_id);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_interaction_date ON dm.customer_support_fact_default(interaction_date);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_interaction_status ON dm.customer_support_fact_default(interaction_status);
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_is_active ON dm.customer_support_fact_default(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_customer_support_fact_default_temporal ON dm.customer_support_fact_default(start_date, end_date);