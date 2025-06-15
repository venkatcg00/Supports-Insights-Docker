\c support_insights

-- Create tables in ds schema
CREATE TABLE IF NOT EXISTS ds.sources (
    source_id              SERIAL       PRIMARY KEY,
    source_name            TEXT         NOT NULL UNIQUE,
    source_file_type       TEXT         NOT NULL,
    data_nature            TEXT         NOT NULL,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS ds.customer_types (
    customer_type_id       SERIAL       PRIMARY KEY,
    customer_type_name     TEXT         NOT NULL,
    source_id              INT          NOT NULL,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_customer_types_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS ds.support_areas (
    support_area_id        SERIAL       PRIMARY KEY,
    support_area_name      TEXT         NOT NULL,
    source_id              INT          NOT NULL,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_support_areas_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS ds.agents (
    agent_id               SERIAL       PRIMARY KEY,
    first_name             TEXT         NOT NULL,
    middle_name            TEXT,
    last_name              TEXT         NOT NULL,
    pseudo_code            TEXT         NOT NULL UNIQUE,
    source_id              INT          NOT NULL,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_agents_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
);

-- Indexes for ds.sources
CREATE INDEX IF NOT EXISTS idx_sources_is_active ON ds.sources(is_active) WHERE is_active = TRUE;

-- Indexes for ds.customer_types
CREATE INDEX IF NOT EXISTS idx_customer_types_source_id ON ds.customer_types(source_id);
CREATE INDEX IF NOT EXISTS idx_customer_types_name ON ds.customer_types(customer_type_name);
CREATE INDEX IF NOT EXISTS idx_customer_types_is_active ON ds.customer_types(is_active) WHERE is_active = TRUE;

-- Indexes for ds.support_areas
CREATE INDEX IF NOT EXISTS idx_support_areas_source_id ON ds.support_areas(source_id);
CREATE INDEX IF NOT EXISTS idx_support_areas_name ON ds.support_areas(support_area_name);
CREATE INDEX IF NOT EXISTS idx_support_areas_is_active ON ds.support_areas(is_active) WHERE is_active = TRUE;

-- Indexes for ds.agents
CREATE INDEX IF NOT EXISTS idx_agents_source_id ON ds.agents(source_id);
CREATE INDEX IF NOT EXISTS idx_agents_name ON ds.agents(first_name, last_name);
CREATE INDEX IF NOT EXISTS idx_agents_is_active ON ds.agents(is_active) WHERE is_active = TRUE;