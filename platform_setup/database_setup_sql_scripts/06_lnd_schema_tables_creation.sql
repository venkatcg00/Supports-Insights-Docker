\c support_insights

-- Create tables in lnd schema for truncate and load
CREATE TABLE IF NOT EXISTS lnd.client_alpha_cs_data (
    source_record_id       TEXT,
    interaction_id         TEXT,
    support_category       TEXT,
    agent_pseudo_name      TEXT,
    contact_date           TEXT,
    interaction_status     TEXT,
    interaction_type       TEXT,
    type_of_customer       TEXT,
    interaction_duration   TEXT,
    total_time             TEXT,
    status_of_customer_incident TEXT,
    resolved_in_first_contact TEXT,
    solution_type          TEXT,
    rating                 TEXT
);

CREATE TABLE IF NOT EXISTS lnd.client_beta_cs_data (
    support_identifier     TEXT,
    contact_regarding      TEXT,
    agent_code             TEXT,
    date_of_interaction    TEXT,
    status_of_interaction  TEXT,
    type_of_interaction    TEXT,
    customer_type          TEXT,
    contact_duration       TEXT,
    after_contact_work_time TEXT,
    incident_status        TEXT,
    first_contact_solve    TEXT,
    type_of_resolution     TEXT,
    support_rating         TEXT,
    time_stamp             TEXT
);

CREATE TABLE IF NOT EXISTS lnd.client_gamma_cs_data (
    ticket_identifier      TEXT,
    support_category       TEXT,
    agent_name             TEXT,
    date_of_call           TEXT,
    call_status            TEXT,
    call_type              TEXT,
    type_of_customer       TEXT,
    duration               TEXT,
    work_time              TEXT,
    ticket_status          TEXT,
    resolved_in_first_contact TEXT,
    resolution_category    TEXT,
    rating                 TEXT
);