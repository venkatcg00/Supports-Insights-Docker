\c support_insights

-- Create tables in ds schema
CREATE TABLE IF NOT EXISTS ds.sources (
    source_id              SERIAL       PRIMARY KEY,
    source_name            TEXT         NOT NULL UNIQUE,
    source_file_type       TEXT         NOT NULL,
    dataload_strategy      TEXT         NOT NULL,
    last_loaded_record_id  INT,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT chk_sources_end_date
    CHECK                  (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS ds.customer_types (
    customer_type_id       SERIAL       PRIMARY KEY,
    customer_type_name     TEXT         NOT NULL,
    source_id              INT          NOT NULL,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_customer_types_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_customer_types_end_date
    CHECK                  (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS ds.support_areas (
    support_area_id        SERIAL       PRIMARY KEY,
    support_area_name      TEXT         NOT NULL,
    source_id              INT          NOT NULL,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_support_areas_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_support_areas_end_date
    CHECK                  (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS ds.agents (
    agent_id               SERIAL       PRIMARY KEY,
    first_name             TEXT         NOT NULL,
    middle_name            TEXT,
    last_name              TEXT         NOT NULL,
    pseudo_code            TEXT         NOT NULL UNIQUE,
    source_id              INT          NOT NULL,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_agents_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_agents_end_date
    CHECK                  (end_date >= start_date) 
);

-- Indexes for ds.sources
CREATE INDEX IF NOT EXISTS idx_sources_is_active ON ds.sources(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_sources_temporal ON ds.sources(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_sources_last_loaded ON ds.sources(last_loaded_record_id);

-- Indexes for ds.customer_types
CREATE INDEX IF NOT EXISTS idx_customer_types_source_id ON ds.customer_types(source_id);
CREATE INDEX IF NOT EXISTS idx_customer_types_name ON ds.customer_types(customer_type_name);
CREATE INDEX IF NOT EXISTS idx_customer_types_is_active ON ds.customer_types(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_customer_types_temporal ON ds.customer_types(start_date, end_date);

-- Indexes for ds.support_areas
CREATE INDEX IF NOT EXISTS idx_support_areas_source_id ON ds.support_areas(source_id);
CREATE INDEX IF NOT EXISTS idx_support_areas_name ON ds.support_areas(support_area_name);
CREATE INDEX IF NOT EXISTS idx_support_areas_is_active ON ds.support_areas(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_support_areas_temporal ON ds.support_areas(start_date, end_date);

-- Indexes for ds.agents
CREATE INDEX IF NOT EXISTS idx_agents_source_id ON ds.agents(source_id);
CREATE INDEX IF NOT EXISTS idx_agents_name ON ds.agents(first_name, last_name);
CREATE INDEX IF NOT EXISTS idx_agents_is_active ON ds.agents(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_agents_temporal ON ds.agents(start_date, end_date);