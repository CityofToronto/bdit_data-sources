CREATE OR REPLACE FUNCTION wys.update_mobile_api_id (IN _mon DATE)
RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

INSERT INTO wys.mobile_api_id
SELECT a.id AS location_id,
       a.ward_no,
       a.location,
       a.from_street,
       a.to_street,
       a.direction,
       a.installation_date,
       a.removal_date,
       a.comments,
       a.combined,
       b.api_id
FROM (SELECT mobile_sign_installations.ward_no,
             mobile_sign_installations.location,
             mobile_sign_installations.from_street,
             mobile_sign_installations.to_street,
             mobile_sign_installations.direction,
             mobile_sign_installations.installation_date,
             mobile_sign_installations.removal_date,
             mobile_sign_installations.new_sign_number,
             mobile_sign_installations.comments,
             mobile_sign_installations.id,
             CASE
                 WHEN mobile_sign_installations.new_sign_number !~~ 'W%'::text THEN (('Ward '::text || mobile_sign_installations.ward_no) || ' - S'::text) || mobile_sign_installations.new_sign_number
                 WHEN mobile_sign_installations.new_sign_number ~~ 'W%'::text AND mobile_sign_installations.new_sign_number ~~ '% - S%'::text THEN 'Ward '::text || "substring"(mobile_sign_installations.new_sign_number, 2, 10)
                 WHEN mobile_sign_installations.new_sign_number ~~ 'W%'::text AND mobile_sign_installations.new_sign_number !~~ '% - S%'::text THEN (('Ward '::text || "substring"(mobile_sign_installations.new_sign_number, 2, "position"(mobile_sign_installations.new_sign_number, ' - '::text) + 1)) || 'S'::text) || "right"(mobile_sign_installations.new_sign_number, 1)
                 ELSE NULL::text
             END AS combined
      FROM wys.mobile_sign_installations) a
LEFT JOIN (SELECT DISTINCT api_id, sign_name, start_date,
                  lead(start_date) OVER w AS next_start,
                  lag(start_date) OVER w AS prev_start
           FROM wys.locations
           WHERE locations.sign_name ~~ 'Ward%'::text
           WINDOW w AS (PARTITION BY sign_name ORDER BY start_date)) b 
ON a.combined = b.sign_name
   AND (b.prev_start IS NULL OR installation_date >= b.prev_start)
   AND (b.next_start IS NULL OR installation_date < b.start_date)
WHERE installation_date >= _mon;
$BODY$;

REVOKE EXECUTE ON FUNCTION wys.update_mobile_api_id (DATE)FROM public;
GRANT EXECUTE ON FUNCTION wys.update_mobile_api_id (DATE) TO wys_bot;