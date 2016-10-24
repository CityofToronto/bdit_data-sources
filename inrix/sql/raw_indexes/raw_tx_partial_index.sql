/*Reducing timestamp index size by applying it only to rows
  where the speed record is based on observed data (score=30)*/

CREATE OR REPLACE FUNCTION inrix.create_raw_tx_idx (tablename TEXT)
RETURNS INTEGER
AS $$
BEGIN
    EXECUTE format('CREATE INDEX ON inrix.%I (tx) WHERE score = 30;', tablename);
    RETURN 1;
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION inrix.create_raw_tx_idx (TEXT) TO rdumas_py;