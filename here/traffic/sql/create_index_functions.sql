CREATE OR REPLACE FUNCTION here.create_link_id_idx (tablename TEXT)
RETURNS INTEGER
AS $$
BEGIN
    EXECUTE format('CREATE INDEX ON here.%I (link_id);', tablename);
    RETURN 1;
END;
$$
SECURITY DEFINER
LANGUAGE plpgsql;
ALTER FUNCTION here.create_link_id_idx (TEXT) OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here.create_link_id_idx (TEXT) TO here_admin_bot;

CREATE OR REPLACE FUNCTION here.create_tx_idx (tablename TEXT)
RETURNS INTEGER
AS $$
BEGIN
    EXECUTE format('CREATE INDEX ON here.%I (tx);', tablename);
    RETURN 1;
END;
$$
SECURITY DEFINER
LANGUAGE plpgsql;
ALTER FUNCTION here.create_tx_idx (TEXT) OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here.create_tx_idx (TEXT) TO here_admin_bot;