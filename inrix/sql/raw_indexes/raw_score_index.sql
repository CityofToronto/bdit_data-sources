CREATE OR REPLACE FUNCTION inrix.create_raw_score_idx (tablename TEXT)
RETURNS INTEGER
AS $$
BEGIN
    EXECUTE format('CREATE INDEX ON inrix.%I (score);', tablename);
    RETURN 1;
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION inrix.create_raw_score_idx (TEXT) TO rdumas_py;