\c support_insights

-- Create tables in info schema
CREATE TABLE IF NOT EXISTS info.object_types (
    object_type_id         SERIAL       PRIMARY KEY,
    object_type_code       TEXT         NOT NULL UNIQUE,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT chk_object_types_end_date
    CHECK                  (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS info.object_load_strategies (
    object_load_strategy_id SERIAL       PRIMARY KEY,
    object_load_strategy_code TEXT       NOT NULL UNIQUE,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT chk_object_load_strategies_end_date
    CHECK                  (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS info.object_names (
    object_name_id         SERIAL       PRIMARY KEY,
    object_name            TEXT         NOT NULL UNIQUE,
    object_type_id         INT          NOT NULL,
    object_load_strategy_id INT          NOT NULL,
    description            TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_object_names_object_type_id
    FOREIGN KEY            (object_type_id) REFERENCES info.object_types(object_type_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT fk_object_names_object_load_strategy_id
    FOREIGN KEY            (object_load_strategy_id) REFERENCES info.object_load_strategies(object_load_strategy_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_object_names_end_date
    CHECK                  (end_date >= start_date) 
);

CREATE TABLE IF NOT EXISTS info.data_dictionary (
    data_dictionary_id     SERIAL       PRIMARY KEY,
    source_id              INT          NOT NULL,
    field_name             TEXT         NOT NULL,
    data_type              TEXT         NOT NULL,
    values_allowed         TEXT         NOT NULL,
    description            TEXT,
    example                TEXT,
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    start_date             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date               TIMESTAMP    NOT NULL DEFAULT '9999-12-31 23:59:59',
    CONSTRAINT fk_data_dictionary_source_id
    FOREIGN KEY            (source_id) REFERENCES ds.sources(source_id)
                           ON DELETE RESTRICT
                           ON UPDATE CASCADE
                           ,
    CONSTRAINT chk_data_dictionary_end_date
    CHECK                  (end_date >= start_date) 
);