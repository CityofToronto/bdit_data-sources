CREATE OR REPLACE FUNCTION ttc.cis_insert_trigger()
  RETURNS trigger AS
$BODY$
BEGIN
IF ( message_datetime >= '2017-01-01' AND message_datetime < '2017-01-01' + INTERVAL '1 year') THEN 
    INSERT INTO ttc.cis_2017 VALUES (NEW.*) ON CONFLICT DO NOTHING;
ELSIF (message_datetime >= '2018-01-01' AND message_datetime < '2018-01-01' + INTERVAL '1 year') THEN
    INSERT INTO ttc.cis_2018 VALUES (NEW.*) ON CONFLICT DO NOTHING;
ELSE 
    RAISE EXCEPTION 'message_datetime out of range';
RETURN NULL;
END;
$BODY$
LANGUAGE plgpsql VOLATILE SECURITY DEFINER;
