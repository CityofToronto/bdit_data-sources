--to create a table from the m. view to do the update
SELECT *
INTO gis.bylaws_speed_limit_layer_hwy 
FROM gis.bylaws_speed_limit_layer

--gardiner west of humber river (39 rows)
UPDATE gis.bylaws_speed_limit_layer_hwy  SET speed_limit = 100 
WHERE geo_id IN (913014,913062,913089,913152,913159,913172,913187,913249,913264,913354,913364,913367
,913403,913493,913503,913520,913534,913625,913633,913670,913677,913714,913720,913728
,913733,913748,913776,913783,913829,913835,913844,913864,913875,20043572,20043579,20043650,20043655,30005878,30005881);

--gardiner east of humber river (128 rows)
UPDATE gis.bylaws_speed_limit_layer_hwy  SET speed_limit = 90
WHERE lf_name ILIKE '%F G Gardiner Xy%' 
AND geo_id NOT IN 
(913014,913062,913089,913152,913159,913172,913187,913249,913264,913354,913364,913367
,913403,913493,913503,913520,913534,913625,913633,913670,913677,913714,913720,913728
,913733,913748,913776,913783,913829,913835,913844,913864,913875,20043572,20043579,20043650,20043655,30005878,30005881,
14646841,14646863,14646867); --last 3 are ramps in disguise

--highway 2a & 27 (88 rows)
UPDATE gis.bylaws_speed_limit_layer_hwy SET speed_limit = 80
WHERE lf_name ILIKE '%highway 2%' ;

--highway 400 series (915 rows)
UPDATE gis.bylaws_speed_limit_layer_hwy SET speed_limit = 100
WHERE lf_name ILIKE '%highway 4%' ;
 
--don valley parkway (127 rows)
UPDATE gis.bylaws_speed_limit_layer_hwy SET speed_limit = 90
WHERE lf_name ILIKE '%don valley parkway%' ;