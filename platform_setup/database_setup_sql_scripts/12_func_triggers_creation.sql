\c support_insights

-- Function to sync DS.SOURCES to dwh.ds_sources (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_ds_sources_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.ds_sources
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE source_id = NEW.source_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.ds_sources (source_id, source_name, source_file_type, data_nature, description, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.source_id, OLD.source_name, OLD.source_file_type, OLD.data_nature, OLD.description, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.ds_sources (source_id, source_name, source_file_type, data_nature, description, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.source_id, NEW.source_name, NEW.source_file_type, NEW.data_nature, NEW.description, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ds_sources_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON ds.sources
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_ds_sources_to_dwh();

-- Function to sync DS.CUSTOMER_TYPES to dwh.ds_customer_types (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_ds_customer_types_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.ds_customer_types
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE customer_type_id = NEW.customer_type_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.ds_customer_types (customer_type_id, customer_type_name, source_id, description, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.customer_type_id, OLD.customer_type_name, OLD.source_id, OLD.description, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.ds_customer_types (customer_type_id, customer_type_name, source_id, description, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.customer_type_id, NEW.customer_type_name, NEW.source_id, NEW.description, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ds_customer_types_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON ds.customer_types
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_ds_customer_types_to_dwh();

-- Function to sync DS.SUPPORT_AREAS to dwh.ds_support_areas (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_ds_support_areas_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.ds_support_areas
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE support_area_id = NEW.support_area_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.ds_support_areas (support_area_id, support_area_name, source_id, description, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.support_area_id, OLD.support_area_name, OLD.source_id, OLD.description, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.ds_support_areas (support_area_id, support_area_name, source_id, description, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.support_area_id, NEW.support_area_name, NEW.source_id, NEW.description, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ds_support_areas_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON ds.support_areas
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_ds_support_areas_to_dwh();

-- Function to sync DS.AGENTS to dwh.ds_agents (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_ds_agents_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.ds_agents
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE agent_id = NEW.agent_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.ds_agents (agent_id, first_name, middle_name, last_name, pseudo_code, source_id, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.agent_id, OLD.first_name, OLD.middle_name, OLD.last_name, OLD.pseudo_code, OLD.source_id, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.ds_agents (agent_id, first_name, middle_name, last_name, pseudo_code, source_id, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.agent_id, NEW.first_name, NEW.middle_name, NEW.last_name, NEW.pseudo_code, NEW.source_id, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ds_agents_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON ds.agents
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_ds_agents_to_dwh();

-- Function to sync INFO.OBJECT_TYPES to dwh.info_object_types (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_info_object_types_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.info_object_types
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE object_type_id = NEW.object_type_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.info_object_types (object_type_id, object_type_code, description, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.object_type_id, OLD.object_type_code, OLD.description, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.info_object_types (object_type_id, object_type_code, description, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.object_type_id, NEW.object_type_code, NEW.description, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_info_object_types_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON info.object_types
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_info_object_types_to_dwh();

-- Function to sync INFO.OBJECT_LOAD_STRATEGIES to dwh.info_object_load_strategies (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_info_object_load_strategies_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.info_object_load_strategies
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE object_load_strategy_id = NEW.object_load_strategy_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.info_object_load_strategies (object_load_strategy_id, object_load_strategy_code, description, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.object_load_strategy_id, OLD.object_load_strategy_code, OLD.description, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.info_object_load_strategies (object_load_strategy_id, object_load_strategy_code, description, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.object_load_strategy_id, NEW.object_load_strategy_code, NEW.description, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_info_object_load_strategies_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON info.object_load_strategies
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_info_object_load_strategies_to_dwh();

-- Function to sync INFO.OBJECT_NAMES to dwh.info_object_names (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_info_object_names_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.info_object_names
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE object_name_id = NEW.object_name_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.info_object_names (object_name_id, object_name, object_type_id, object_load_strategy_id, description, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.object_name_id, OLD.object_name, OLD.object_type_id, OLD.object_load_strategy_id, OLD.description, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.info_object_names (object_name_id, object_name, object_type_id, object_load_strategy_id, description, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.object_name_id, NEW.object_name, NEW.object_type_id, NEW.object_load_strategy_id, NEW.description, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_info_object_names_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON info.object_names
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_info_object_names_to_dwh();

-- Function to sync INFO.DATA_DICTIONARY to dwh.info_data_dictionary (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_info_data_dictionary_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- If this is an update, mark the previous record as inactive
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.info_data_dictionary
        SET end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE data_dictionary_id = NEW.data_dictionary_id
          AND end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert the new record (for INSERT, UPDATE, or DELETE)
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.info_data_dictionary (data_dictionary_id, source_id, field_name, data_type, values_allowed, description, example, is_active, start_date, end_date, dml_operation)
        VALUES (OLD.data_dictionary_id, OLD.source_id, OLD.field_name, OLD.data_type, OLD.values_allowed, OLD.description, OLD.example, FALSE, OLD.start_date, CURRENT_TIMESTAMP, TG_OP);
        RETURN OLD;
    ELSE
        INSERT INTO dwh.info_data_dictionary (data_dictionary_id, source_id, field_name, data_type, values_allowed, description, example, is_active, start_date, end_date, dml_operation)
        VALUES (NEW.data_dictionary_id, NEW.source_id, NEW.field_name, NEW.data_type, NEW.values_allowed, NEW.description, NEW.example, TRUE, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', TG_OP);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_info_data_dictionary_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON info.data_dictionary
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_info_data_dictionary_to_dwh();

-- Function to sync AUD.DAG_RUNS to dwh.aud_dag_runs (SCD Type 2)
CREATE OR REPLACE FUNCTION trg_fn_sync_aud_dag_runs_to_dwh()
RETURNS TRIGGER AS $$
BEGIN
    -- Mark previous record as inactive on UPDATE
    IF TG_OP = 'UPDATE' THEN
        UPDATE dwh.aud_dag_runs
        SET run_end_date = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE dag_run_id = NEW.dag_run_id
          AND run_end_date = '9999-12-31 23:59:59'
          AND is_active = TRUE;
    END IF;

    -- Insert new record into DWH for INSERT, UPDATE, or DELETE
    IF TG_OP = 'DELETE' THEN
        INSERT INTO dwh.aud_dag_runs (
            dag_run_id, airflow_dag_run_id, source_id, dag_run_status,
            run_start_date, run_end_date, batch_count, insert_count, update_count,
            duplicate_count, valid_count, validity_percentage, run_duration,
            source_checkpoint, is_active, dml_operation
        )
        VALUES (
            OLD.dag_run_id, OLD.airflow_dag_run_id, OLD.source_id, OLD.dag_run_status,
            OLD.run_start_date, CURRENT_TIMESTAMP, OLD.batch_count, OLD.insert_count, OLD.update_count,
            OLD.duplicate_count, OLD.valid_count, OLD.validity_percentage, OLD.run_duration,
            OLD.source_checkpoint, FALSE, TG_OP
        );
        RETURN OLD;
    ELSE
        INSERT INTO dwh.aud_dag_runs (
            dag_run_id, airflow_dag_run_id, source_id, dag_run_status,
            run_start_date, run_end_date, batch_count, insert_count, update_count,
            duplicate_count, valid_count, validity_percentage, run_duration,
            source_checkpoint, is_active, dml_operation
        )
        VALUES (
            NEW.dag_run_id, NEW.airflow_dag_run_id, NEW.source_id, NEW.dag_run_status,
            NEW.run_start_date, '9999-12-31 23:59:59', NEW.batch_count, NEW.insert_count, NEW.update_count,
            NEW.duplicate_count, NEW.valid_count, NEW.validity_percentage, NEW.run_duration,
            NEW.source_checkpoint, TRUE, TG_OP
        );
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_aud_dag_runs_to_dwh
AFTER INSERT OR UPDATE OR DELETE ON aud.dag_runs
FOR EACH ROW
EXECUTE FUNCTION trg_fn_sync_aud_dag_runs_to_dwh();