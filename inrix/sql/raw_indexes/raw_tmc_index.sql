CREATE OR REPLACE FUNCTION inrix.create_raw_tmc_idx(tablename text)
RETURNS integer
AS $$
BEGIN
    EXECUTE format('CREATE INDEX ON inrix.%I (tmc);', tablename);
    RETURN 1;
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION inrix.create_raw_tmc_idx (text) TO rdumas_py;