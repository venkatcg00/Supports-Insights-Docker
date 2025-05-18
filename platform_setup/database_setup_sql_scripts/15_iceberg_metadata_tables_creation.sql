\c hive_metastore

CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location TEXT NOT NULL,
    previous_metadata_location TEXT,
    iceberg_type VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS table_metadata (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY (catalog_name, table_namespace, table_name, metadata_json),
    FOREIGN KEY (catalog_name, table_namespace, table_name) 
    REFERENCES iceberg_tables(catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value TEXT,
    PRIMARY KEY (catalog_name, namespace, property_key)
);