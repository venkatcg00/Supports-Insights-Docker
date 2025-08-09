-- Create tables
CREATE TABLE IF NOT EXISTS sources (
    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL UNIQUE,
    source_file_type TEXT NOT NULL,
    dataload_strategy TEXT NOT NULL,
    last_loaded_record_id INTEGER,
    description TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    start_date TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
    end_date TEXT NOT NULL DEFAULT '9999-12-31 23:59:59'
);

CREATE TABLE IF NOT EXISTS customer_types (
    customer_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_type_name TEXT NOT NULL,
    source_id INTEGER NOT NULL,
    description TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    start_date TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
    end_date TEXT NOT NULL DEFAULT '9999-12-31 23:59:59',
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS support_areas (
    support_area_id INTEGER PRIMARY KEY AUTOINCREMENT,
    support_area_name TEXT NOT NULL,
    source_id INTEGER NOT NULL,
    description TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    start_date TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
    end_date TEXT NOT NULL DEFAULT '9999-12-31 23:59:59',
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS agents (
    agent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    middle_name TEXT,
    last_name TEXT NOT NULL,
    pseudo_code TEXT NOT NULL UNIQUE,
    source_id INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    start_date TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
    end_date TEXT NOT NULL DEFAULT '9999-12-31 23:59:59',
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS run_history (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_name TEXT NOT NULL,
    input_value INTEGER NOT NULL,
    records_generated INTEGER NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    duration REAL,
    status TEXT NOT NULL,
    error_message TEXT,
    log_file TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    script_name TEXT PRIMARY KEY,
    script_status TEXT NOT NULL,
    processing_count INTEGER NOT NULL,
    new_records INTEGER NOT NULL,
    update_records INTEGER NOT NULL,
    null_records INTEGER NOT NULL,
    max_serial_number INTEGER NOT NULL,
    max_record_id INTEGER NOT NULL,
    change_timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now'))
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_script_name ON run_history (script_name);
CREATE INDEX IF NOT EXISTS idx_sources_is_active ON sources (is_active) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_sources_temporal ON sources (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_sources_last_loaded ON sources (last_loaded_record_id);
CREATE INDEX IF NOT EXISTS idx_customer_types_source_id ON customer_types (source_id);
CREATE INDEX IF NOT EXISTS idx_customer_types_name ON customer_types (customer_type_name);
CREATE INDEX IF NOT EXISTS idx_customer_types_is_active ON customer_types (is_active) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_customer_types_temporal ON customer_types (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_support_areas_source_id ON support_areas (source_id);
CREATE INDEX IF NOT EXISTS idx_support_areas_name ON support_areas (support_area_name);
CREATE INDEX IF NOT EXISTS idx_support_areas_is_active ON support_areas (is_active) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_support_areas_temporal ON support_areas (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_agents_source_id ON agents (source_id);
CREATE INDEX IF NOT EXISTS idx_agents_name ON agents (first_name, last_name);
CREATE INDEX IF NOT EXISTS idx_agents_is_active ON agents (is_active) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_agents_temporal ON agents (start_date, end_date);

-- Insert data into sources
INSERT INTO sources (source_name, source_file_type, dataload_strategy, description)
VALUES
    ('CLIENT_ALPHA', 'JSON', 'INCREMENTAL', 'CUSTOMER SUPPORT DATA FROM CLIENT_ALPHA SOURCE'),
    ('CLIENT_BETA', 'XML', 'INCREMENTAL', 'CUSTOMER SUPPORT DATA FROM CLIENT_BETA SOURCE'),
    ('CLIENT_GAMMA', 'CSV', 'INCREMENTAL', 'CUSTOMER SUPPORT DATA FROM CLIENT_GAMMA SOURCE');

-- Insert data into support_areas (CLIENT_ALPHA)
INSERT INTO support_areas (support_area_name, source_id, description)
VALUES
    ('ORDER ISSUES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('PRODUCT INQUIRIES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('SHIPPING & DELIVERY', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('ACCOUNT & SECURITY', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('PAYMENTS & BILLING', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('RETURNS & REFUNDS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('TECHNICAL SUPPORT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('PROMOTIONS & DISCOUNTS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('PRIME MEMBERSHIP', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA'),
    ('MARKETPLACE & THIRD-PARTY SELLERS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'SUPPORT AREA FROM CLIENT_ALPHA');

-- Insert data into support_areas (CLIENT_BETA)
INSERT INTO support_areas (support_area_name, source_id, description)
VALUES
    ('RIDE ISSUES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('DRIVER/PASSENGER CONCERNS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('PAYMENTS & BILLING', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('SAFETY & SECURITY', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('ACCOUNT & PROFILE MANAGEMENT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('PROMOTIONS & DISCOUNT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('DRIVER SUPPORT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('TECHNICAL SUPPORT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('CLIENT_BETA EATS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA'),
    ('ACCESIBILITY SUPPORT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'SUPPORT AREA FROM CLIENT_BETA');

-- Insert data into support_areas (CLIENT_GAMMA)
INSERT INTO support_areas (support_area_name, source_id, description)
VALUES
    ('ACCOUNT MANAGEMENT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('TECHNICAL SUPPORT', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('SUPPORT OUTAGES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('NEW CUSTOMER ONBOARDING', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('MOBILE & WIRELESS SERVICES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('HOME SERVICES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('PROMOTIONS & DISCOUNTS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('SECURITY & PRIVACY', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('BUSINESS SERVICES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA'),
    ('LOYALTY & RETENTIONS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'SUPPORT AREA FROM CLIENT_GAMMA');

-- Insert data into agents (CLIENT_ALPHA)
INSERT INTO agents (first_name, middle_name, last_name, pseudo_code, source_id)
VALUES
    ('JOHN', 'A.', 'DOE', 'JDOE', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('JANE', 'B.', 'SMITH', 'JSMITH', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('ALICE', 'C.', 'JOHNSON', 'AJOHNSON', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('BOB', 'D.', 'WILLIAMS', 'BWILLIAMS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('CHARLIE', 'E.', 'BROWN', 'CBROWN', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('DAVID', 'F.', 'JONES', 'DJONES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('EVE', 'G.', 'GARCIA', 'EGARCIA', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('FRANK', 'H.', 'MARTINEZ', 'FMARTINEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('GRACE', 'I.', 'HERNANDEZ', 'GHERNANDEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA')),
    ('HANK', 'J.', 'LOPEZ', 'HLOPEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'));

-- Insert data into agents (CLIENT_BETA)
INSERT INTO agents (first_name, middle_name, last_name, pseudo_code, source_id)
VALUES
    ('IVY', 'K.', 'GONZALEZ', 'IGONZALEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('JACK', 'L.', 'WILSON', 'JWILSON', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('KARA', 'M.', 'ANDERSON', 'KANDERSON', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('LEO', 'N.', 'THOMAS', 'LTHOMAS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('MIA', 'O.', 'TAYLOR', 'MTAYLOR', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('NATE', 'P.', 'MOORE', 'NMOORE', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('OLIVIA', 'Q.', 'JACKSON', 'OJACKSON', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('PAUL', 'R.', 'WHITE', 'PWHITE', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('QUINN', 'S.', 'HARRIS', 'QHARRIS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA')),
    ('RYAN', 'T.', 'MARTIN', 'RMARTIN', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'));

-- Insert data into agents (CLIENT_GAMMA)
INSERT INTO agents (first_name, middle_name, last_name, pseudo_code, source_id)
VALUES
    ('SARA', 'U.', 'THOMPSON', 'STHOMPSON', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('TOM', 'V.', 'GARCIA', 'TGARCIA', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('UMA', 'W.', 'MARTINEZ', 'UMARTINEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('VICTOR', 'X.', 'HERNANDEZ', 'VHERNANDEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('WENDY', 'Y.', 'LOPEZ', 'WLOPEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('XANDER', 'Z.', 'GONZALEZ', 'XGONZALEZ', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('YARA', 'A.', 'WILSON', 'YWILSON', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('ZANE', 'B.', 'ANDERSON', 'ZANDERSON', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('ABBY', 'C.', 'THOMAS', 'ATHOMAS', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA')),
    ('EDGAR', 'D.', 'JONES', 'EJONES', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'));

-- Insert data into customer_types (CLIENT_ALPHA)
INSERT INTO customer_types (customer_type_name, source_id, description)
VALUES
    ('FIRST-TIME SHOPPER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('FREQUENT BUYER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('PRIME MEMBER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('HIGH-SPENDER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('BUDGET SHOPPER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('BUSINESS BUYER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('TECH-SAVVY BUYER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('GIFT GIVER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('RETURNS & REFUND SEEKER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE'),
    ('COMPLAINT-FOCUSED CUSTOMER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_ALPHA'), 'CUSTOMER TYPE FROM CLIENT_ALPHA SOURCE');

-- Insert data into customer_types (CLIENT_BETA)
INSERT INTO customer_types (customer_type_name, source_id, description)
VALUES
    ('FIRST-TIME RIDER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('FREQUENT RIDER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('COMMUTER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('TRAVELER/TOURIST', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('CLIENT_BETA EATS CUSTOMER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('CORPORATE RIDER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('DRIVER-PARTNER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('PROMO-SEEKER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('SAFETY-CONSCIOUS RIDER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE'),
    ('ACCESSIBILITY-FOCUSED CUSTOMER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_BETA'), 'CUSTOMER TYPE FROM CLIENT_BETA SOURCE');

-- Insert data into customer_types (CLIENT_GAMMA)
INSERT INTO customer_types (customer_type_name, source_id, description)
VALUES
    ('NEW CUSTOMER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('LONG-TERM CUSTOMER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('TECH-SAVVY USER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('SENIOR CITIZEN', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('BUSINESS OWNER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('FAMILY PLAN MANAGER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('FREQUENT TRAVELER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('BUDGET-CONSCIOUS CUSTOMER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('HEAVY DATA USER', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE'),
    ('DEVICE ENTHUSIAST', (SELECT source_id FROM sources WHERE source_name = 'CLIENT_GAMMA'), 'CUSTOMER TYPE FROM CLIENT_GAMMA SOURCE');

COMMIT;