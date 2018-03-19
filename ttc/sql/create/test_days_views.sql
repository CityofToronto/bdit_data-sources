CREATE OR REPLACE VIEW ttc.cis_20171004 AS 
SELECT * FROM 
ttc.cis_2017
WHERE message_datetime >= '2017-10-04' AND message_datetime < '2017-10-04'::DATE + INTERVAL '1 Day';
GRANT SELECT ON ttc.cis_20171004 TO bdit_humans;

CREATE OR REPLACE VIEW ttc.cis_20171114 AS 
SELECT * FROM 
ttc.cis_2017
WHERE message_datetime >= '2017-11-14' AND message_datetime < '2017-11-14'::DATE + INTERVAL '1 Day';
GRANT SELECT ON ttc.cis_20171114 TO bdit_humans;