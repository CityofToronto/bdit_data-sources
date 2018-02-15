CREATE OR REPLACE FUNCTION ttc.cis_insert_trigger()
  RETURNS trigger AS
$BODY$
BEGIN
IF ( NEW.message_datetime >= '2017-01-01' AND NEW.message_datetime < '2017-01-01'::DATE + INTERVAL '1 year') THEN 
    INSERT INTO ttc.cis_2017 VALUES (NEW.*) ON CONFLICT DO NOTHING;
ELSIF (NEW.message_datetime >= '2018-01-01' AND NEW.message_datetime < '2018-01-01'::DATE + INTERVAL '1 year') THEN
    INSERT INTO ttc.cis_2018 VALUES (NEW.*) ON CONFLICT DO NOTHING;
ELSE 
    RAISE EXCEPTION 'message_datetime out of range';
END IF;
RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

