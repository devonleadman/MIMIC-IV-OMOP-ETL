DO $$
DECLARE
    table_name TEXT;
    is_empty BOOLEAN;
BEGIN
    FOR table_name IN 
        SELECT t.table_name
        FROM information_schema.tables AS t
        WHERE t.table_name IN (
            'z_check_voc_1', 'z_check_voc_2', 'z_check_voc_3', 
            'z_check_voc_5', 'z_check_voc_6', 'z_check_voc_7',
            'z_check_voc_8', 'z_check_voc_9', 'z_check_voc_10',
            'z_check_voc_11', 'z_check_voc_12', 'z_check_voc_13',
            'z_check_voc_14', 'z_check_voc_15', 'z_check_voc_16',
            'z_check_voc_17'
        )
    LOOP
        -- Dynamically check if the table is empty
        EXECUTE 'SELECT NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ')' INTO is_empty;

        -- Drop the table if it is empty
        IF is_empty THEN
            EXECUTE 'DROP TABLE ' || quote_ident(table_name);
            RAISE NOTICE 'Dropped empty table: %', table_name;
        END IF;
    END LOOP;
END $$;