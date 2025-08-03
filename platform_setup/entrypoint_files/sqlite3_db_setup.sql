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

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id TEXT PRIMARY KEY,
    vin TEXT UNIQUE,
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    variant TEXT,
    fuel_type TEXT NOT NULL CHECK(fuel_type IN ('Petrol', 'Diesel', 'Electric', 'Hybrid')),
    vehicle_type TEXT,
    license_plate TEXT,
    country TEXT,
    initial_odometer_km REAL,
    registration_date TEXT
);

CREATE TABLE IF NOT EXISTS cities (
    city TEXT PRIMARY KEY,
    country TEXT,
    latitude REAL,
    longitude REAL,
    typical_weather TEXT
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

-- Insert data into vehicles
INSERT INTO vehicles VALUES
('027cbc5f-cf5e-4620-a0a1-dc6f0a6464b7', 'DFC97DC8-AAD7-45B', 'Tesla', 'Model Y', 'Model Y 338', 'Electric', 'Sedan', 'ES-N3639', 'EU', 7442.0, '2017-12-10'),
('b8f9914d-ceed-4c3a-a408-6dd16ff297c4', 'C30845BE-2B2A-46B', 'Peugeot', 'e-208', 'e-208 266', 'Electric', 'Hatchback', 'NL-Q9191', 'EU', 47761.7, '2016-07-15'),
('063a139b-c335-49b1-a061-556f8b93254f', '8E2CDE32-50CE-4F5', 'Skoda', 'Enyaq', 'Enyaq 982', 'Electric', 'Van', 'DE-X1230', 'EU', 140031.0, '2023-06-10'),
('a8296f18-67a0-4f3a-9287-b3fb1cf617ee', 'A661E187-DA88-419', 'Peugeot', 'e-208', 'e-208 447', 'Electric', 'SUV', 'NL-P2949', 'EU', 54187.4, '2017-03-02'),
('5295a011-572f-4694-a21b-4a3c123fa674', 'B2DDDE54-3BCE-427', 'Ford', 'Focus', 'Focus 111', 'Petrol', 'Sedan', 'NL-A8206', 'EU', 24334.9, '2019-04-07'),
('23f49d52-8b2a-4d39-a228-a0128ff0869c', 'CC55A47E-2937-45D', 'Hyundai', 'i30', 'i30 356', 'Petrol', 'Sedan', 'ES-I7892', 'EU', 80713.0, '2021-07-05'),
('163cfaf3-37bf-4daa-af8c-b0f2f5c0f5f7', 'E4F41AF5-9648-493', 'Peugeot', 'e-208', 'e-208 558', 'Electric', 'SUV', 'IT-C4404', 'EU', 135789.5, '2015-08-13'),
('26446d2e-5794-43e7-bceb-248cd001ca10', 'FB6EBE3E-46FC-42A', 'Mercedes-Benz', 'EQA', 'EQA 357', 'Electric', 'Sedan', 'DE-B4648', 'EU', 57750.9, '2017-11-12'),
('f27e1557-9c0c-41aa-9caa-66a70bf07ba1', '2D813E4F-5D5B-484', 'Tesla', 'Model Y', 'Model Y 841', 'Electric', 'SUV', 'IT-I2354', 'EU', 13212.6, '2017-06-22'),
('a5d30c92-1f50-4ba5-b90f-7b8f81fc32c7', '293E862B-96D5-4A1', 'Mercedes-Benz', 'EQA', 'EQA 346', 'Electric', 'SUV', 'ES-J4222', 'EU', 131545.4, '2020-01-21'),
('454a3182-66e6-4dd8-aa27-05ac02e761fd', '170A9970-59D0-45A', 'Tesla', 'Model Y', 'Model Y 738', 'Electric', 'SUV', 'ES-I6426', 'EU', 62140.2, '2020-12-22'),
('1c83bf1d-416c-472d-8d60-18881f2a88d4', '7888998B-D75E-43B', 'Ford', 'Kuga', 'Kuga 886', 'Hybrid', 'SUV', 'DE-J6481', 'EU', 81155.6, '2018-07-24'),
('60aa0b21-6206-4c35-85a6-0abdae9614be', '0F50A633-00CD-4B6', 'Tesla', 'Model 3', 'Model 3 228', 'Electric', 'Sedan', 'DE-K7066', 'EU', 52013.0, '2019-02-05'),
('52bac9a0-0323-4992-9d7d-53ea024b42fc', '56B8EBEE-21B5-46F', 'Tesla', 'Model 3', 'Model 3 134', 'Electric', 'Van', 'DE-T9739', 'EU', 145733.0, '2015-09-01'),
('85af86e0-fb0e-4eb9-b269-cb4b684cb49b', 'A8776FE1-1E7F-405', 'Fiat', '500', '500 446', 'Diesel', 'Van', 'IT-D8041', 'EU', 1510.0, '2019-10-21'),
('08631634-27d7-4a22-bb41-29bd3b239cc0', 'DAF748AB-374B-491', 'Skoda', 'Octavia', 'Octavia 216', 'Diesel', 'Hatchback', 'DE-H3113', 'EU', 30506.9, '2016-03-28'),
('c41654e1-d46d-4729-a2fc-c63f0a25185c', '6E04D5DF-DD54-46B', 'BMW', 'X5', 'X5 738', 'Hybrid', 'Sedan', 'DE-C6863', 'EU', 100501.4, '2023-08-15'),
('51a6d4c5-999c-4b16-bf6b-f1f547530a3c', 'D8BDDC94-3EDD-4C4', 'Skoda', 'Enyaq', 'Enyaq 343', 'Electric', 'Van', 'ES-I9361', 'EU', 50752.4, '2018-05-09'),
('60f5ece3-24e3-43e4-8cb0-99b8df764e54', '63D6D792-25C6-450', 'Mercedes-Benz', 'GLC', 'GLC 954', 'Hybrid', 'SUV', 'NL-P5473', 'EU', 104084.3, '2016-07-04'),
('41e970df-d68f-47cb-a493-1acbd3938b39', '4F230C06-344D-4CD', 'Mercedes-Benz', 'GLC', 'GLC 242', 'Hybrid', 'SUV', 'DE-X9152', 'EU', 25060.1, '2021-06-19'),
('24352482-6375-482a-be83-71ec6e87de4a', 'B3D4C4FB-6BD5-4E1', 'Fiat', 'Panda', 'Panda 592', 'Hybrid', 'Hatchback', 'NL-I4782', 'EU', 127047.6, '2020-12-09'),
('50d5ef9b-4784-4820-a26c-c09190f38f2f', 'ECC0A912-D5F0-4BB', 'Mercedes-Benz', 'GLC', 'GLC 942', 'Petrol', 'SUV', 'NL-O6630', 'EU', 100912.3, '2015-02-15'),
('33d29c10-fb39-487f-b37a-0afcd36d0907', '722FBD54-C56C-4BE', 'BMW', 'iX', 'iX 701', 'Hybrid', 'SUV', 'ES-O3474', 'EU', 126695.2, '2023-07-16'),
('1195ba8f-027a-47b6-b637-dd6965903115', '1E3EFFFF-2AE3-4DE', 'BMW', 'X5', 'X5 567', 'Hybrid', 'Van', 'IT-V8065', 'EU', 128301.6, '2019-02-03'),
('a8236ad4-b1dd-427d-85fc-e830cf0c56a7', '9E5A75B7-64EB-47C', 'Fiat', 'Panda', 'Panda 200', 'Diesel', 'Sedan', 'FR-K2215', 'EU', 117412.7, '2015-09-20'),
('4e30cb45-b322-486f-a096-aad394a641b6', '10C31BF6-8B08-4D5', 'Peugeot', '2008', '2008 643', 'Hybrid', 'Sedan', 'ES-J5181', 'EU', 81509.5, '2020-03-11'),
('7b9377f1-764a-434c-94f3-d6ca0b06b958', 'B56B94B5-3FCD-405', 'Volkswagen', 'ID.3', 'ID.3 696', 'Electric', 'Hatchback', 'IT-P3094', 'EU', 54242.1, '2018-04-26'),
('15a7cc0a-e809-4cfd-870f-f6e06ef4b61e', '24F32948-37C7-43C', 'Volkswagen', 'ID.3', 'ID.3 173', 'Electric', 'Van', 'DE-R1026', 'EU', 134222.3, '2019-07-26'),
('d4eb81f7-9633-4248-a30b-e1dc44659722', '808F6513-B444-42B', 'Mercedes-Benz', 'EQA', 'EQA 113', 'Electric', 'Hatchback', 'ES-V5726', 'EU', 142077.0, '2023-12-02'),
('6f67e48d-f4ec-4748-87f3-3ecfac1a03f4', '8B2E67E1-46A2-473', 'Renault', 'ZOE', 'ZOE 801', 'Electric', 'SUV', 'DE-B3765', 'EU', 130177.7, '2023-06-20'),
('2d41e5ab-3ba3-46bb-983c-88eff9429f7f', '1E3085DE-426E-4A0', 'BMW', '320d', '320d 363', 'Hybrid', 'Van', 'NL-E9961', 'EU', 35068.2, '2020-05-14'),
('409b7503-b3f7-41c5-9e7e-8adf00af2960', '8832CDFC-B9EC-45F', 'Volkswagen', 'Golf', 'Golf 894', 'Hybrid', 'Sedan', 'DE-Q7768', 'EU', 118009.4, '2023-02-26'),
('2022be24-9bcb-47b9-813b-03888d8b8e1d', '21E90F11-29AA-496', 'BMW', '320d', '320d 884', 'Diesel', 'Van', 'DE-U9426', 'EU', 88555.0, '2019-10-12'),
('8b0fcb22-08c1-4f76-b5e1-3bbdf88a4dd4', 'B01F86A4-5BA1-44C', 'Tesla', 'Model Y', 'Model Y 829', 'Electric', 'Van', 'DE-I4780', 'EU', 106784.0, '2018-10-05'),
('0a12683c-40de-4f81-882a-92ec6c049aff', '6527E525-D3A6-40D', 'Fiat', '500', '500 960', 'Diesel', 'Hatchback', 'DE-Y4582', 'EU', 117335.6, '2016-01-12'),
('365ff78b-a308-464b-923e-bc61fe308625', '29499F20-B2FC-4B8', 'Tesla', 'Model 3', 'Model 3 641', 'Electric', 'Sedan', 'DE-K6723', 'EU', 34906.8, '2023-08-14'),
('e071ad75-cbfe-4230-bf2b-cf3955abd66d', '86A28CCF-B4F9-4CB', 'Renault', 'Clio', 'Clio 631', 'Petrol', 'Van', 'DE-V3127', 'EU', 56480.5, '2021-01-20'),
('d1ff6307-4583-4ced-9d9c-36eb82002fea', 'BA5B02E1-441D-481', 'Volkswagen', 'ID.3', 'ID.3 392', 'Electric', 'Hatchback', 'IT-S9619', 'EU', 121244.9, '2018-01-25'),
('3e30cdc1-43c5-494f-90ec-a0e996fe7e43', '0565A227-D7F2-455', 'Volkswagen', 'ID.3', 'ID.3 199', 'Electric', 'Hatchback', 'IT-T8784', 'EU', 60369.6, '2023-07-09'),
('bdc71b3b-74f8-45b2-8adf-07cedd598bad', '31AA79DD-389C-46A', 'Hyundai', 'i30', 'i30 651', 'Diesel', 'SUV', 'IT-U1371', 'EU', 148394.5, '2021-11-13'),
('b7094e45-e26a-428e-a62b-55327c919668', 'AEA860D5-2BA7-4BF', 'Fiat', 'Panda', 'Panda 575', 'Hybrid', 'Sedan', 'FR-S9170', 'EU', 96350.9, '2017-06-04'),
('9f470396-9d95-4009-abde-3493ee2b5fb8', 'AAE4EFC3-3508-493', 'Tesla', 'Model 3', 'Model 3 933', 'Electric', 'Van', 'DE-E7699', 'EU', 41467.8, '2021-10-03'),
('b77da262-c363-4c56-bae2-6b85c3ac50ce', '2F43B9F6-7F70-4C5', 'Fiat', 'Panda', 'Panda 674', 'Petrol', 'Hatchback', 'FR-H4305', 'EU', 80423.7, '2017-09-25'),
('17b7f89d-1fc1-4f7a-b013-79c4a52dd948', 'FD8FA91D-730A-49F', 'Volkswagen', 'Tiguan', 'Tiguan 287', 'Petrol', 'Sedan', 'FR-B9346', 'EU', 32532.7, '2022-04-24'),
('0ccef514-ddd6-4288-95c2-db4445d0c067', '99740C55-0EB6-4D2', 'Tesla', 'Model Y', 'Model Y 464', 'Electric', 'Hatchback', 'DE-N6785', 'EU', 134598.6, '2020-04-24'),
('2558ed4e-d187-4f85-8afa-fe2658075164', 'F1F2FA0C-094E-44B', 'Fiat', '500', '500 809', 'Petrol', 'SUV', 'FR-M4100', 'EU', 91412.8, '2017-08-10'),
('0d23234f-8058-41ca-8182-bb8aeedcc4a8', 'AEDAA88B-5516-4A9', 'Peugeot', 'e-208', 'e-208 394', 'Electric', 'SUV', 'IT-K1986', 'EU', 135768.1, '2023-06-24'),
('a0a42034-12b2-4fb9-9737-7cd5417d10c9', '53292BEB-6D77-47D', 'Volkswagen', 'Passat', 'Passat 330', 'Petrol', 'SUV', 'DE-P3214', 'EU', 55094.2, '2017-10-03'),
('1c5a5032-8ab6-41b6-a06a-204475f4c85b', 'A8A9DB79-696A-43B', 'Hyundai', 'Kona Electric', 'Kona Electric 399', 'Electric', 'Hatchback', 'FR-Y9585', 'EU', 125358.7, '2019-12-26'),
('700d7c19-f2c8-4680-81ea-26439af3a971', 'FCF90745-167E-44B', 'Tesla', 'Model Y', 'Model Y 439', 'Electric', 'SUV', 'DE-N4827', 'EU', 66495.3, '2022-11-11');

-- Insert data into cities
INSERT INTO cities VALUES
('Berlin', 'Germany', 52.52, 13.405, '[''Clear'', ''Rain'', ''Fog'', ''Snow'']'),
('Paris', 'France', 48.8566, 2.3522, '[''Clear'', ''Rain'', ''Storm'', ''Fog'']'),
('Amsterdam', 'Netherlands', 52.3676, 4.9041, '[''Rain'', ''Fog'', ''Clear'']'),
('Rome', 'Italy', 41.9028, 12.4964, '[''Clear'', ''Rain'', ''Storm'']'),
('Madrid', 'Spain', 40.4168, -3.7038, '[''Clear'', ''Rain'', ''Dust'']'),
('Vienna', 'Austria', 48.2082, 16.3738, '[''Clear'', ''Rain'', ''Snow'']'),
('Copenhagen', 'Denmark', 55.6761, 12.5683, '[''Clear'', ''Rain'', ''Fog'']'),
('Stockholm', 'Sweden', 59.3293, 18.0686, '[''Clear'', ''Snow'', ''Fog'']'),
('Brussels', 'Belgium', 50.8503, 4.3517, '[''Rain'', ''Fog'', ''Clear'']'),
('Lisbon', 'Portugal', 38.7169, -9.1399, '[''Clear'', ''Rain'', ''Fog'']');

PRAGMA foreign_keys = ON;
-- Landing pages
CREATE TABLE IF NOT EXISTS landing_pages (
    page_type     TEXT PRIMARY KEY,
    url_path      TEXT NOT NULL,
    weight        INTEGER NOT NULL DEFAULT 1
);

INSERT OR IGNORE INTO landing_pages (page_type, url_path, weight) VALUES
('Homepage','/',30),
('Search','/search',25),
('CarDetail','/cars/car-details',20),
('Pricing','/pricing',10),
('FAQ','/faq',5),
('Blog','/blog/plan-benefits',3),
('Landing','/lp/electric-deals',7);

-- Channels
CREATE TABLE IF NOT EXISTS channels (
    channel        TEXT PRIMARY KEY,
    default_medium TEXT,
    default_source TEXT,
    weight         INTEGER DEFAULT 1
);

INSERT OR IGNORE INTO channels (channel, default_medium, default_source, weight) VALUES
('Direct',   NULL,      NULL,       25),
('Organic',  'search',  'google',   30),
('Paid',     'cpc',     'google',   20),
('Social',   'social',  'facebook', 10),
('Referral', 'referral','deal-blog', 5),
('Email',    'email',   'newsletter',10);

-- Funnel timings
CREATE TABLE IF NOT EXISTS funnel_timings (
    stage          TEXT PRIMARY KEY,
    min_delay_sec  INTEGER NOT NULL,
    max_delay_sec  INTEGER NOT NULL
);

INSERT OR IGNORE INTO funnel_timings(stage, min_delay_sec, max_delay_sec) VALUES
('view_car',        5,  60),
('plan_select',    20, 120),
('checkout_start', 30, 150),
('credit_submit',  60, 180),
('subscribe',      90, 240);

-- Devices
CREATE TABLE IF NOT EXISTS devices (
    device_id   TEXT PRIMARY KEY,
    device_type TEXT NOT NULL CHECK(device_type IN ('Mobile','Desktop','Tablet')),
    os          TEXT NOT NULL,
    browser     TEXT NOT NULL,
    weight      INTEGER NOT NULL DEFAULT 1
);

INSERT OR IGNORE INTO devices (device_id, device_type, os, browser, weight) VALUES
('d-m-ios-safari',    'Mobile','iOS','Safari',28),
('d-m-android-chrome','Mobile','Android','Chrome',32),
('d-d-win-chrome',    'Desktop','Windows','Chrome',15),
('d-d-mac-chrome',    'Desktop','macOS','Chrome',7),
('d-d-mac-safari',    'Desktop','macOS','Safari',6),
('d-d-win-edge',      'Desktop','Windows','Edge',7),
('d-t-android-chrome','Tablet','Android','Chrome',3),
('d-t-ipados-safari', 'Tablet','iOS','Safari',2);

-- Price buckets
CREATE TABLE IF NOT EXISTS price_buckets (
    price_bucket TEXT PRIMARY KEY,
    min_price_eur REAL,
    max_price_eur REAL
);

INSERT OR IGNORE INTO price_buckets(price_bucket, min_price_eur, max_price_eur) VALUES
('LT_399',    0,   399),
('R400_599',  400, 599),
('GE_600',    600, NULL);

-- A/B variants (optional)
CREATE TABLE IF NOT EXISTS ab_variants (
    experiment TEXT,
    variant    TEXT,
    weight     INTEGER DEFAULT 1,
    PRIMARY KEY (experiment, variant)
);

INSERT OR IGNORE INTO ab_variants VALUES
('plan_ui_test','control', 50),
('plan_ui_test','variantA',50);

-- Car catalog (seeded with >=100 rows)
CREATE TABLE IF NOT EXISTS car_catalog (
    car_id              TEXT PRIMARY KEY,
    brand               TEXT NOT NULL,
    model               TEXT NOT NULL,
    body_type           TEXT NOT NULL,
    powertrain          TEXT NOT NULL CHECK(powertrain IN ('EV','Hybrid','Petrol','Diesel')),
    monthly_price_eur   REAL NOT NULL,
    price_bucket        TEXT NOT NULL REFERENCES price_buckets(price_bucket),
    city                TEXT NOT NULL REFERENCES cities(city),
    availability_pct    REAL NOT NULL CHECK(availability_pct BETWEEN 0 AND 100)
);

CREATE INDEX IF NOT EXISTS idx_car_catalog_brand_model ON car_catalog(brand, model);
CREATE INDEX IF NOT EXISTS idx_car_catalog_city ON car_catalog(city);


INSERT OR IGNORE INTO car_catalog(car_id, brand, model, body_type, powertrain, monthly_price_eur, price_bucket, city, availability_pct) VALUES
('08886f29-e1ac-40ff-8e0d-18d8c94c104b', 'Kia', 'Sportage', 'SUV', 'Hybrid', 771.0, 'GE_600', 'Madrid', 69.7),
('b2348764-038d-4b81-94c0-583faee4f88d', 'Audi', 'A3', 'Hatchback', 'Petrol', 367.0, 'LT_399', 'Brussels', 94.2),
('e17b2362-6c1b-46c0-9f07-b4df61e54353', 'Volvo', 'S60', 'Sedan', 'Petrol', 458.0, 'R400_599', 'Copenhagen', 59.7),
('a35d6ad7-3703-413f-a4b5-8eae93fdff39', 'Opel', 'Corsa', 'Hatchback', 'Petrol', 392.0, 'LT_399', 'Amsterdam', 86.5),
('c1731889-7c4b-4307-ae7a-98165fca5a06', 'Opel', 'Grandland', 'SUV', 'Petrol', 522.0, 'R400_599', 'Paris', 41.5),
('560c9010-f780-4071-bda3-f3b014c1c68c', 'Hyundai', 'Kona', 'SUV', 'EV', 861.0, 'GE_600', 'Amsterdam', 40.5),
('83ae5311-9ab7-4419-9702-76cd1bccbe15', 'Citroën', 'C3', 'Hatchback', 'Diesel', 325.0, 'LT_399', 'Stockholm', 47.6),
('3ee0bb6d-32ea-40b5-8dee-262e8fb7ca62', 'BMW', '3 Series', 'Sedan', 'Diesel', 432.0, 'R400_599', 'Copenhagen', 53.0),
('512f78ea-3bd3-495b-ad09-51c3a52d49f1', 'Toyota', 'RAV4', 'SUV', 'EV', 774.0, 'GE_600', 'Madrid', 59.0),
('dd1faba1-b466-475c-9f4f-b07417705cbd', 'Volkswagen', 'ID.3', 'Hatchback', 'Petrol', 371.0, 'LT_399', 'Madrid', 94.0),
('65634f6b-7e22-41d0-b150-f6a75652397d', 'Opel', 'Insignia', 'Sedan', 'Hybrid', 481.0, 'R400_599', 'Lisbon', 68.6),
('ce46fc69-d456-41b9-bb25-d68008ee63db', 'Mercedes-Benz', 'EQA', 'SUV', 'Diesel', 569.0, 'R400_599', 'Berlin', 72.9),
('19c4ff60-754b-438f-a355-0358f9ba2f9d', 'Volvo', 'V60', 'Wagon', 'Diesel', 548.0, 'R400_599', 'Paris', 60.3),
('4fd681f2-2d6f-426a-89e2-c6faae4886c7', 'BMW', '3 Series', 'Sedan', 'Hybrid', 598.0, 'R400_599', 'Vienna', 58.5),
('8ef1629b-7073-4dba-a2a8-99d5dcb13500', 'Volvo', 'C40', 'SUV', 'EV', 745.0, 'GE_600', 'Stockholm', 79.2),
('bd77f982-7e41-4f38-b2be-d11ee98e7da6', 'Citroën', 'C5 Aircross', 'SUV', 'EV', 671.0, 'GE_600', 'Stockholm', 66.2),
('c8173981-f495-426e-ae9e-522206e32194', 'Renault', 'ZOE', 'Hatchback', 'Hybrid', 484.0, 'R400_599', 'Stockholm', 58.6),
('3364c104-215a-4093-8caa-1bc5113e4431', 'Polestar', '2', 'Sedan', 'EV', 656.0, 'GE_600', 'Copenhagen', 54.4),
('219dc112-9e8b-4883-bdf6-07ec550c72b6', 'Renault', 'Captur', 'SUV', 'Diesel', 526.0, 'R400_599', 'Berlin', 93.3),
('3b99c6c6-0a02-4d56-bfd0-0446f26e0800', 'Dacia', 'Sandero', 'Hatchback', 'Petrol', 336.0, 'LT_399', 'Stockholm', 60.0),
('5a6c4b01-acbf-41c6-9320-df606bd206cd', 'Škoda', 'Octavia', 'Wagon', 'Hybrid', 542.0, 'R400_599', 'Berlin', 88.4),
('06e67251-a6f0-4222-b3fa-82e7736e3b4e', 'Citroën', 'C4', 'Hatchback', 'EV', 548.0, 'R400_599', 'Copenhagen', 74.0),
('ef621b34-fb7f-46bb-a302-5ba785c9c9c0', 'Volkswagen', 'Golf', 'Hatchback', 'EV', 456.0, 'R400_599', 'Rome', 59.2),
('5061bb8f-78e0-4d27-8aec-0ec0eedeb6bc', 'Volvo', 'S60', 'Sedan', 'EV', 697.0, 'GE_600', 'Lisbon', 34.7),
('622d7790-425f-49c4-b5bc-4959d9acfdf6', 'Volkswagen', 'ID.4', 'SUV', 'Petrol', 438.0, 'R400_599', 'Lisbon', 87.0),
('1228dd51-199d-4ccf-be9f-94cb33e0008e', 'Škoda', 'Scala', 'Hatchback', 'Diesel', 338.0, 'LT_399', 'Brussels', 53.8),
('cc8ce673-01c2-4993-aa5b-55235774a407', 'Hyundai', 'Kona', 'SUV', 'EV', 758.0, 'GE_600', 'Brussels', 71.6),
('5f50c404-f575-463c-92bd-87bcc6a7b02e', 'Mercedes-Benz', 'CLA', 'Sedan', 'Diesel', 421.0, 'R400_599', 'Lisbon', 69.4),
('db1b5dc5-a3da-465c-9ad8-3b84e67c5d56', 'Audi', 'A4', 'Sedan', 'EV', 538.0, 'R400_599', 'Amsterdam', 75.9),
('391818f2-f4f2-4109-92e8-054934749663', 'Kia', 'Ceed', 'Hatchback', 'Petrol', 353.0, 'LT_399', 'Amsterdam', 42.9),
('c4b9ebb6-5125-4724-8b4b-78b671b51e09', 'Ford', 'Focus', 'Hatchback', 'EV', 490.0, 'R400_599', 'Paris', 50.1),
('d9dacfc7-2ece-4abd-a14f-c7aa43c80446', 'Peugeot', '308', 'Hatchback', 'EV', 418.0, 'R400_599', 'Brussels', 60.0),
('13378126-f8df-4aef-95fa-78d23b86332e', 'Renault', 'Captur', 'SUV', 'Petrol', 524.0, 'R400_599', 'Brussels', 68.8),
('728ed4ab-d144-4ef8-90b6-4b5386013213', 'Peugeot', '208', 'Hatchback', 'Petrol', 435.0, 'R400_599', 'Amsterdam', 60.6),
('a0ac02ee-cd53-4eff-bf98-177bf4c8dcd5', 'Toyota', 'C-HR', 'SUV', 'EV', 900.0, 'GE_600', 'Madrid', 32.4),
('f87058cb-7fff-42a6-8d2a-f09565e97c55', 'Volvo', 'S60', 'Sedan', 'Diesel', 512.0, 'R400_599', 'Vienna', 35.1),
('a8df529b-d41e-4afe-9566-f66fd24f7af7', 'Ford', 'Fiesta', 'Hatchback', 'Petrol', 410.0, 'R400_599', 'Amsterdam', 32.5),
('8cd4b3ca-9020-49fd-b0cf-7b25f2c217b4', 'Mercedes-Benz', 'A-Class', 'Hatchback', 'EV', 486.0, 'R400_599', 'Brussels', 30.6),
('bb4414c4-aa87-4e06-a3d8-8e9940f5756f', 'Fiat', '500X', 'SUV', 'Petrol', 752.0, 'GE_600', 'Rome', 56.9),
('17a4a995-aa8d-4a70-94ec-1c907a6aae9f', 'Ford', 'Mondeo', 'Sedan', 'Petrol', 408.0, 'R400_599', 'Copenhagen', 36.0),
('002fc8e6-ca76-41db-a0ea-5f9682298a63', 'Volkswagen', 'Passat', 'Wagon', 'Diesel', 480.0, 'R400_599', 'Madrid', 84.7),
('bb656c2b-3462-42e7-8189-68c7be1ff243', 'Hyundai', 'i20', 'Hatchback', 'EV', 475.0, 'R400_599', 'Lisbon', 48.6),
('b2b2b3e3-8f72-44ca-8eea-41d0ad0f5edc', 'Kia', 'EV6', 'SUV', 'Hybrid', 803.0, 'GE_600', 'Copenhagen', 88.2),
('55df6bb4-ad36-4028-a0e9-23b81397e267', 'Fiat', '500X', 'SUV', 'Petrol', 685.0, 'GE_600', 'Amsterdam', 34.3),
('a9b51013-ebf0-48e1-bc4a-ee4173e33431', 'Škoda', 'Fabia', 'Hatchback', 'Hybrid', 454.0, 'R400_599', 'Vienna', 70.7),
('601598de-edf6-4a42-b20e-b12d7ef2027b', 'Opel', 'Astra', 'Hatchback', 'Petrol', 301.0, 'LT_399', 'Stockholm', 52.2),
('ec5d6113-4f39-44eb-8d94-d9c67af2cbef', 'Renault', 'ZOE', 'Hatchback', 'EV', 394.0, 'LT_399', 'Berlin', 39.9),
('4cbe0a73-20e3-4cea-a963-23ee29afe818', 'BMW', 'X3', 'SUV', 'Diesel', 535.0, 'R400_599', 'Rome', 54.0),
('745b1509-36e9-4e1d-93f9-f1d6cdffb439', 'BMW', 'i3', 'Hatchback', 'Hybrid', 457.0, 'R400_599', 'Paris', 46.2),
('eb10c4de-a17f-4022-9b6b-e81383c92bcf', 'Audi', 'Q4 e-tron', 'SUV', 'Diesel', 714.0, 'GE_600', 'Copenhagen', 38.8),
('c336e809-94de-4b7d-b738-9742cf027e11', 'Cupra', 'Formentor', 'SUV', 'Diesel', 723.0, 'GE_600', 'Lisbon', 75.7),
('2c72c57d-319d-4fba-b7d9-21e42990bbd5', 'Citroën', 'C3', 'Hatchback', 'Hybrid', 443.0, 'R400_599', 'Vienna', 66.8),
('0e63317d-098f-4620-b4aa-fd07835f29d3', 'Renault', 'Clio', 'Hatchback', 'Petrol', 454.0, 'R400_599', 'Paris', 75.3),
('16c48479-1a65-4522-9ccb-813f61f9847c', 'Dacia', 'Duster', 'SUV', 'Petrol', 477.0, 'R400_599', 'Paris', 76.0),
('0dca5b28-ed5f-4965-a750-548bc3fec071', 'Toyota', 'Yaris', 'Hatchback', 'EV', 553.0, 'R400_599', 'Brussels', 91.1),
('ba07ba87-9f05-450c-a8a3-3c3101db229c', 'Dacia', 'Duster', 'SUV', 'Petrol', 632.0, 'GE_600', 'Berlin', 64.1),
('9cef8dc2-efe5-426a-aa6d-d4e994ebe6c8', 'Opel', 'Grandland', 'SUV', 'EV', 814.0, 'GE_600', 'Vienna', 77.3),
('52eb4e01-bb0b-4b85-aaa5-07ba3b8f2c74', 'Dacia', 'Jogger', 'MPV', 'Diesel', 525.0, 'R400_599', 'Berlin', 36.1),
('90b1bab2-b49b-4c34-9809-0be87acb830e', 'Mercedes-Benz', 'C-Class', 'Sedan', 'Diesel', 518.0, 'R400_599', 'Brussels', 87.5),
('63a69a3c-b9cf-437e-837f-3607a0d94fce', 'Peugeot', '308', 'Hatchback', 'Petrol', 454.0, 'R400_599', 'Brussels', 32.4),
('15939f2c-5948-4626-8f11-511633618220', 'Mercedes-Benz', 'A-Class', 'Hatchback', 'EV', 483.0, 'R400_599', 'Copenhagen', 83.9),
('cd7cd343-c98b-43df-9acc-bc76d6826e50', 'Ford', 'Focus', 'Hatchback', 'Petrol', 398.0, 'LT_399', 'Rome', 44.7),
('0903a11d-7d19-4ce4-90ea-8ee765538993', 'Mini', 'Countryman', 'SUV', 'Petrol', 804.0, 'GE_600', 'Lisbon', 68.9),
('8a29136c-9837-4f17-a135-a33747e4a96b', 'Kia', 'Rio', 'Hatchback', 'Diesel', 409.0, 'R400_599', 'Vienna', 79.7),
('662998b6-7ce9-40ca-b7da-c7cafee5275e', 'Peugeot', 'e-208', 'Hatchback', 'Petrol', 321.0, 'LT_399', 'Brussels', 92.7),
('9ec5f905-be7d-47c6-9ad7-ea9fa6cb0767', 'Tesla', 'Model 3', 'Sedan', 'Hybrid', 420.0, 'R400_599', 'Madrid', 91.4),
('abbbbf2a-2e71-4c27-8d81-cd959ea59def', 'Fiat', 'Panda', 'Hatchback', 'Diesel', 373.0, 'LT_399', 'Stockholm', 36.0),
('0cfe06c7-5899-4354-a0eb-a0083ec7f94c', 'Renault', 'Megane', 'Hatchback', 'Hybrid', 417.0, 'R400_599', 'Lisbon', 75.6),
('8ddce02b-283c-4153-b879-67ddc430f721', 'Cupra', 'Born', 'Hatchback', 'EV', 481.0, 'R400_599', 'Madrid', 43.2),
('7859fa99-1ce6-4224-a9b0-f901fbab44d1', 'Mini', 'Cooper', 'Hatchback', 'Petrol', 404.0, 'R400_599', 'Rome', 46.1),
('7bc79927-181c-42bb-8f51-56cc0f635a7d', 'Hyundai', 'i30', 'Hatchback', 'Petrol', 423.0, 'R400_599', 'Amsterdam', 81.6),
('052f6209-3e76-4cd7-84b0-5daf1e117771', 'Peugeot', '2008', 'SUV', 'EV', 731.0, 'GE_600', 'Madrid', 63.1),
('596d3be4-5e9f-4046-b86d-7bae6f47c32b', 'Dacia', 'Sandero', 'Hatchback', 'Hybrid', 465.0, 'R400_599', 'Berlin', 66.6),
('2dff8446-64fc-477e-9c3c-be64a5ae8695', 'Mercedes-Benz', 'A-Class', 'Hatchback', 'EV', 534.0, 'R400_599', 'Rome', 76.6),
('4f8eb9f0-5c2f-4b26-b1ba-6f49400a7acc', 'BMW', '1 Series', 'Hatchback', 'Hybrid', 374.0, 'LT_399', 'Madrid', 64.3),
('1f3418ee-637d-48d2-8fc5-f15e6b531e30', 'Citroën', 'C5 Aircross', 'SUV', 'EV', 636.0, 'GE_600', 'Copenhagen', 50.6),
('9e98823b-12c4-490e-a470-33e75248c881', 'Hyundai', 'Tucson', 'SUV', 'Hybrid', 834.0, 'GE_600', 'Madrid', 72.3),
('204f44bb-a5de-403b-b152-8877f1b5daca', 'Volkswagen', 'Tiguan', 'SUV', 'Petrol', 475.0, 'R400_599', 'Brussels', 86.0),
('00d7253e-b704-45df-aab5-a53771b51362', 'Tesla', 'Model 3', 'Sedan', 'Petrol', 507.0, 'R400_599', 'Lisbon', 54.7),
('cc7f21d6-461a-4643-bb06-7e9661edb2c7', 'Cupra', 'Born', 'Hatchback', 'Diesel', 452.0, 'R400_599', 'Copenhagen', 81.1),
('7f84d77b-af86-4438-aad5-5c628ed109b4', 'Volvo', 'XC40', 'SUV', 'Hybrid', 813.0, 'GE_600', 'Madrid', 50.9),
('64d95d76-5f38-4bb6-8670-be95cad88c59', 'BMW', 'i3', 'Hatchback', 'Petrol', 374.0, 'LT_399', 'Stockholm', 43.9),
('621b84b1-c96f-4f33-b7aa-6d26e89ef3dd', 'Toyota', 'C-HR', 'SUV', 'Petrol', 711.0, 'GE_600', 'Amsterdam', 84.0),
('673abea4-5b50-4fa2-98ac-3959b173e0c1', 'Škoda', 'Enyaq', 'SUV', 'Hybrid', 797.0, 'GE_600', 'Madrid', 94.5),
('00e969c4-9b1f-4cad-8434-a13911eaba95', 'Audi', 'A3', 'Hatchback', 'Petrol', 326.0, 'LT_399', 'Berlin', 33.0),
('2685d8a4-dfdd-4ef8-a1e3-b51c03799a41', 'Citroën', 'C4', 'Hatchback', 'Petrol', 377.0, 'LT_399', 'Vienna', 33.5),
('e4057c95-fd2d-429f-bd6b-03cfd22a039c', 'Škoda', 'Enyaq', 'SUV', 'Diesel', 596.0, 'R400_599', 'Copenhagen', 74.9),
('4a6ffba2-02db-4970-875a-0cab9dca2ab7', 'Fiat', 'Doblo', 'MPV', 'Hybrid', 684.0, 'GE_600', 'Stockholm', 34.4),
('9fee6d70-bdbc-4eeb-80ac-d354683cff17', 'Audi', 'A4', 'Sedan', 'Petrol', 439.0, 'R400_599', 'Rome', 47.8),
('27a502fd-8b34-40ec-b3a1-e20d3b49c944', 'Opel', 'Corsa', 'Hatchback', 'Petrol', 432.0, 'R400_599', 'Berlin', 69.6),
('61019c3d-88ea-4393-9b37-0af1a67879b8', 'Toyota', 'Yaris', 'Hatchback', 'Petrol', 375.0, 'LT_399', 'Lisbon', 59.7),
('9ec65131-2519-443b-99a4-3bfb0a3854ed', 'Ford', 'Puma', 'SUV', 'EV', 750.0, 'GE_600', 'Vienna', 87.0),
('dd7143ee-8c3b-4f19-8fa0-2f8df6256149', 'Mini', 'Cooper', 'Hatchback', 'Hybrid', 395.0, 'LT_399', 'Berlin', 45.6),
('f44dc83e-4d8d-4bb9-871f-2956720bed89', 'Hyundai', 'Tucson', 'SUV', 'Petrol', 688.0, 'GE_600', 'Vienna', 69.4),
('271d685f-6598-4602-b679-4491358319c6', 'Renault', 'Megane', 'Hatchback', 'Hybrid', 485.0, 'R400_599', 'Stockholm', 66.0),
('2416c099-b2fb-44bf-90f6-114c6671db72', 'Polestar', '2', 'Sedan', 'Hybrid', 621.0, 'GE_600', 'Copenhagen', 41.7),
('d4ce7b77-3d54-4ee8-aebf-ccbb78ba6ca9', 'Peugeot', '2008', 'SUV', 'Diesel', 635.0, 'GE_600', 'Berlin', 65.9),
('36126d4e-18a7-49f6-96df-1fa60944bf62', 'Mercedes-Benz', 'A-Class', 'Hatchback', 'EV', 524.0, 'R400_599', 'Amsterdam', 32.7),
('7d825e0e-a732-420c-9227-79a443a4dc8b', 'BMW', '1 Series', 'Hatchback', 'EV', 480.0, 'R400_599', 'Paris', 35.6),
('ac731e6d-a056-402a-8dc8-93fc2844b355', 'Audi', 'A6', 'Wagon', 'Hybrid', 645.0, 'GE_600', 'Madrid', 79.8),
('6d40a9d8-89f2-4ab5-b89b-5a6c40056d1e', 'Cupra', 'Born', 'Hatchback', 'Petrol', 318.0, 'LT_399', 'Brussels', 91.7),
('dd97f5c3-0bc2-4ffc-b3f9-6aa0c242f27e', 'Mercedes-Benz', 'CLA', 'Sedan', 'Petrol', 488.0, 'R400_599', 'Paris', 76.8),
('cbcd594e-23ae-42af-b056-c874ea378735', 'Audi', 'A3', 'Hatchback', 'Diesel', 389.0, 'LT_399', 'Copenhagen', 62.1),
('828ee8ef-596f-4b2e-a73e-3c46ddbdc9a8', 'BMW', 'iX1', 'SUV', 'Diesel', 549.0, 'R400_599', 'Brussels', 73.0),
('22b6363b-b0e2-4a87-bb52-f3700b2bbb53', 'Renault', 'Captur', 'SUV', 'Hybrid', 726.0, 'GE_600', 'Rome', 50.5),
('2c08c8db-ebb7-432d-83df-137cb6c1903d', 'Kia', 'Niro', 'SUV', 'Diesel', 529.0, 'R400_599', 'Rome', 76.9),
('75107654-5569-4127-8d07-d52455e9af4f', 'Volkswagen', 'Passat', 'Wagon', 'Petrol', 720.0, 'GE_600', 'Lisbon', 51.1),
('6bfc5bef-ced7-4775-966a-ca5d37b05283', 'Renault', 'ZOE', 'Hatchback', 'Petrol', 450.0, 'R400_599', 'Lisbon', 44.7),
('9677d97c-c2d8-494f-93e1-5137d447bf75', 'Volkswagen', 'ID.4', 'SUV', 'Hybrid', 810.0, 'GE_600', 'Brussels', 65.0),
('43717c0f-b8f3-43f8-965d-f85e1847d82d', 'Dacia', 'Jogger', 'MPV', 'Hybrid', 617.0, 'GE_600', 'Copenhagen', 35.1),
('1193af26-6c21-4069-ad53-86d1fe30c1ad', 'Opel', 'Mokka', 'SUV', 'Hybrid', 497.0, 'R400_599', 'Paris', 86.0),
('d9c62ec3-7893-4127-8cf4-6281f6891821', 'Toyota', 'Corolla', 'Sedan', 'Hybrid', 588.0, 'R400_599', 'Brussels', 81.1),
('b2a3bfc4-f2e1-4c47-b911-6147e28d7277', 'Cupra', 'Formentor', 'SUV', 'EV', 748.0, 'GE_600', 'Lisbon', 69.2),
('9839b4fe-ed72-4c0e-b18f-64e6ef716e2b', 'Dacia', 'Duster', 'SUV', 'Hybrid', 763.0, 'GE_600', 'Copenhagen', 82.4),
('21140028-96d2-4c50-b01e-a63e6746bfe0', 'Toyota', 'Prius', 'Hatchback', 'EV', 487.0, 'R400_599', 'Lisbon', 74.1),
('076d3328-584b-4c69-9ad8-d244cfb8050c', 'Ford', 'Fiesta', 'Hatchback', 'Petrol', 440.0, 'R400_599', 'Vienna', 74.1),
('7aa441aa-95ae-4ba1-bee3-b49fd553ffd0', 'Volvo', 'XC40', 'SUV', 'Petrol', 436.0, 'R400_599', 'Amsterdam', 30.3),
('60f70669-0bf2-4347-a721-479fc1608a2c', 'Volkswagen', 'Golf', 'Hatchback', 'EV', 535.0, 'R400_599', 'Lisbon', 57.4),
('277f0783-f6c0-40eb-8d88-aa6f07c3b922', 'Audi', 'A4', 'Sedan', 'Hybrid', 570.0, 'R400_599', 'Stockholm', 88.3),
('3955d5dc-cbb2-4619-a82d-f390211d0578', 'Audi', 'Q3', 'SUV', 'Petrol', 612.0, 'GE_600', 'Copenhagen', 77.1),
('823fe931-937f-4554-9740-ebfea63cb81f', 'Kia', 'Niro', 'SUV', 'Hybrid', 741.0, 'GE_600', 'Berlin', 67.2),
('8bb34087-ac64-45dc-952c-52aa6b49433f', 'Audi', 'Q4 e-tron', 'SUV', 'EV', 942.0, 'GE_600', 'Berlin', 79.0),
('b790d3b8-28cf-43e1-ba1b-a4cd3ddaf0d8', 'Tesla', 'Model Y', 'SUV', 'EV', 871.0, 'GE_600', 'Brussels', 31.7),
('e45a6e73-05d3-43ac-b902-d88c4911e68a', 'Fiat', 'Tipo', 'Sedan', 'Hybrid', 592.0, 'R400_599', 'Amsterdam', 94.2),
('704c50a5-d0be-4524-a69b-acba5468e9e3', 'Peugeot', '208', 'Hatchback', 'EV', 481.0, 'R400_599', 'Copenhagen', 91.3),
('3483cf05-a4e4-4fab-ab47-6fa146e79f20', 'Škoda', 'Fabia', 'Hatchback', 'Diesel', 336.0, 'LT_399', 'Rome', 78.5),
('115dda03-b58e-40c8-9bfb-c1d6ccc12cb9', 'Volkswagen', 'Golf', 'Hatchback', 'Hybrid', 384.0, 'LT_399', 'Copenhagen', 35.1),
('6db14187-b4fd-4323-9c68-10563df52c84', 'Škoda', 'Karoq', 'SUV', 'Hybrid', 801.0, 'GE_600', 'Lisbon', 46.3),
('1cbbbf67-37ad-4a88-b5fb-cc84cff36c25', 'Kia', 'Ceed', 'Hatchback', 'EV', 567.0, 'R400_599', 'Brussels', 83.8),
('a9ca65a0-8762-40e8-83f9-ae4838be82c1', 'Cupra', 'Formentor', 'SUV', 'Hybrid', 594.0, 'R400_599', 'Vienna', 63.3),
('4c0c5b23-7c03-446b-b881-194e1d8f30be', 'Audi', 'A3', 'Hatchback', 'EV', 512.0, 'R400_599', 'Amsterdam', 82.3),
('9b812121-0732-4e46-a18b-c66d79ddc571', 'Škoda', 'Karoq', 'SUV', 'Hybrid', 647.0, 'GE_600', 'Vienna', 64.5),
('dcd106d4-05f9-47a0-9ceb-896f431c3066', 'Audi', 'A6', 'Wagon', 'Hybrid', 678.0, 'GE_600', 'Berlin', 90.4),
('ada07695-7a5a-42ae-8164-cfa9bbee5f7e', 'Kia', 'Niro', 'SUV', 'Petrol', 597.0, 'R400_599', 'Lisbon', 89.8),
('7c8288a8-a39f-4756-a43c-3f06c581797d', 'Fiat', '500', 'Hatchback', 'Hybrid', 433.0, 'R400_599', 'Copenhagen', 72.6),
('14f6f908-2ace-4533-b82e-bf3b8f806738', 'Kia', 'Ceed', 'Hatchback', 'Petrol', 445.0, 'R400_599', 'Lisbon', 50.0),
('aee7489e-cbc9-4351-b74d-2cd5fdbd0543', 'Mercedes-Benz', 'GLC', 'SUV', 'Diesel', 498.0, 'R400_599', 'Brussels', 74.6),
('f11bb15c-e0ce-41f7-805a-a4985d35aeb6', 'Mercedes-Benz', 'CLA', 'Sedan', 'Hybrid', 608.0, 'GE_600', 'Paris', 48.4),
('5f0b5df5-0fd2-4ccf-a712-757010b36454', 'Hyundai', 'Ioniq 5', 'SUV', 'Hybrid', 553.0, 'R400_599', 'Rome', 92.6),
('da3f492f-ca03-4652-a1df-6e5ac5614ddf', 'Renault', 'Clio', 'Hatchback', 'Petrol', 333.0, 'LT_399', 'Stockholm', 76.0),
('48e1fdea-ae52-4dbf-86bc-3875dcd13d23', 'Kia', 'EV6', 'SUV', 'Petrol', 514.0, 'R400_599', 'Lisbon', 45.6),
('0aea9bb0-7b87-4bd7-b6bb-7c50cee0637d', 'Kia', 'Ceed', 'Hatchback', 'Petrol', 372.0, 'LT_399', 'Lisbon', 88.1),
('c721e361-5299-43c6-953b-c99c105fc37f', 'Toyota', 'C-HR', 'SUV', 'Hybrid', 694.0, 'GE_600', 'Paris', 52.4),
('07436671-4dfa-4b9e-ab1a-dcbdc9571494', 'Volkswagen', 'ID.3', 'Hatchback', 'Diesel', 398.0, 'LT_399', 'Madrid', 75.6),
('6ef5a894-0778-49d1-985e-7e33b3e33865', 'Dacia', 'Duster', 'SUV', 'EV', 785.0, 'GE_600', 'Stockholm', 31.0),
('83639111-368e-428f-b14a-97bbd61a1337', 'Citroën', 'C5 Aircross', 'SUV', 'Hybrid', 733.0, 'GE_600', 'Berlin', 49.0),
('4023e34d-8295-4a13-92cc-9e7c90a0427f', 'Renault', 'Austral', 'SUV', 'Petrol', 517.0, 'R400_599', 'Rome', 90.1),
('51fb309b-5b31-42a7-9fc1-e1202c949c39', 'Citroën', 'C4', 'Hatchback', 'Hybrid', 508.0, 'R400_599', 'Berlin', 71.2),
('3fbf2cf2-70e7-4895-a9e5-5444531afc24', 'Peugeot', '308', 'Hatchback', 'Hybrid', 399.0, 'R400_599', 'Copenhagen', 51.7),
('c12164ff-5964-4066-b675-b6184dcdde57', 'Fiat', 'Tipo', 'Sedan', 'EV', 685.0, 'GE_600', 'Brussels', 68.6);