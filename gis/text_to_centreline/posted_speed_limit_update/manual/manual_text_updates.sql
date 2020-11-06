
SELECT classifi6, classifi7, elevatio10, COUNT(*)
FROM gis.centreline_intersection
GROUP BY classifi6, classifi7, elevatio10
ORDER BY classifi6


ALTER TABLE posted_speed_geoms_2019 ADD COLUMN geom geometry;

UPDATE posted_speed_geoms_2019 SET geom = ST_GeomFromText(geom_str)



DROP TABLE IF EXISTS crosic.speed_limit_conversion_text_update;
CREATE TABLE crosic.speed_limit_conversion_text_update AS 
 SELECT x.id,
    x.street_name,
    x.extents,
    x.date_passed,
    x.bylaw_no,
    x.speed_limit_from,
    x.speed_limit_to
   FROM ( SELECT a.id,
            a.highway AS street_name,
            a.btwn AS extents,
            "substring"(s.split, '[0-9]{4}-[0-9]{2}-[0-9]{2}'::text)::date AS date_passed,
            "substring"(s.split, '[0-9]{1,3}-[0-9]{4}'::text) AS bylaw_no,
                CASE
                    WHEN "substring"(s.split, 'Repealed'::text) IS NOT NULL THEN a.speed_limit_kph
                    ELSE 50
                END AS speed_limit_from,
                CASE
                    WHEN "substring"(s.split, 'Repealed'::text) IS NOT NULL THEN 50
                    ELSE a.speed_limit_kph
                END AS speed_limit_to
           FROM city.speed_limit_changes a,
            LATERAL unnest(string_to_array(a.bylawno, ']['::text)) s(split)
        UNION ALL
         SELECT speed_limit_changes.id,
            speed_limit_changes.highway AS street_name,
            speed_limit_changes.btwn AS extents,
            NULL::date AS date_passed,
            'Original Schedule'::text AS bylaw_no,
            NULL::integer AS speed_limit_from,
            speed_limit_changes.speed_limit_kph AS speed_limit_to
           FROM city.speed_limit_changes
          WHERE "substring"(speed_limit_changes.bylawno, 'Added'::text) IS NULL) x
  ORDER BY x.street_name, x.id, (
        CASE
            WHEN x.date_passed IS NULL THEN '2000-01-01'::date
            ELSE x.date_passed
        END);



UPDATE crosic.posted_speed_geoms_2019_all a
SET geom_str = txt 
FROM (SELECT ST_AsText(ST_Transform(geom, 4326)) txt, * FROM posted_speed_geoms_2019_all_originally_non_matched JOIN vz_safety_programs_staging.speed_reduction_final USING (id) WHERE extents NOT LIKE '% metres %' AND 
geom_str IS NULL) AS b
WHERE a.geom_str IS NULL AND a.id = b.id;




UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Yarmouth Road' WHERE street_name = 'Yarmouth Avenue';




UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Franklin Avenue and a point 50 metres east' 
WHERE street_name = 'Antler Street' AND extents LIKE 'Franklin Avenue and a point 100 metres west%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he east end of', '(the east end of)', 'g')
WHERE extents LIKE '% east end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he easterly end of', '(the east end of)', 'g')
WHERE extents LIKE '% easterly end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, ' east end of', '(the east end of)', 'g')
WHERE extents LIKE '% east end of%' AND extents NOT LIKE '%(the east end of)%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he northeast end of', '(the northeast end of)', 'g')
WHERE extents LIKE '% northeast end of%';

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he southeast end of', '(the southeast end of)', 'g')
WHERE extents LIKE '% southeast end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he west end of', '(the west end of)', 'g')
WHERE extents LIKE '% west end of%';

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, ' west end of', '(the west end of)', 'g')
WHERE extents LIKE '% west end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he southwest end of', '(the southwest end of)', 'g')
WHERE extents LIKE '% southwest end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he northwest end of', '(the northwest end of)', 'g')
WHERE extents LIKE '% northwest end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he south west end of', '(the southwest end of)', 'g')
WHERE extents LIKE '% south west end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he north west end of', '(the northwest end of)', 'g')
WHERE extents LIKE '% north west end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he north end of', '(the north end of)', 'g')
WHERE extents LIKE '% north end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, ' north end of ', ' (north end of) ', 'g')
WHERE extents LIKE '% north end of %' AND extents NOT LIKE '%[Tt]he north end of %';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, '[Tt]he south end of', '(the south end of)', 'g')
WHERE extents LIKE '% south end of%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'George Street South and Henry Lane Terrace' 
WHERE street_name = 'Albert Franck Place' AND extents = 'Henry Lane Terrace and George Street South';



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Alcorn Avenue and Yonge Street' 
WHERE street_name = 'Alcorn Avenue';








-- march 8 

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Regent Street and Sackville Street'
WHERE extents = 'Regent Street and (the east end of) St. Bartholomew Street' AND street_name = 'St. Bartholomew Street';



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Regent Street and Sackville Street'
WHERE extents = 'Regent Street and (the east end of) St. David Walk' AND street_name = 'St. David Walk';





UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Regent Street and Sackville Street'
WHERE extents = 'Regent Street and (the east end of) St. David Walk' AND street_name = 'St. David Walk';






-- march 11 

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, ' further ', ' (further) ', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, ' further ', ' (further) ', 'g');




UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, ' approximately ', ' (approximately) ', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, ' approximately ', ' (approximately) ', 'g');
















UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'North limit of road', 'The Queensway', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'North limit of road', 'The Queensway', 'g');





UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'West limit of road', 'Waniska Avenue', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'West limit of road', 'Waniska Avenue', 'g');




UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'West limit of Bannon Road', '(West limit of) Bannon Road', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'West limit of Bannon Road', '(West limit of) Bannon Road', 'g');




UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'the northeast limit of Bayview Wood', '(the northeast limit of) Bayview Wood', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, '(the northeast limit of) Bayview Wood', '(the northeast limit of) Bayview Wood', 'g');




UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'the east limit of Cynthia Road', '(the east limit of) Cynthia Road', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET  extents = regexp_REPLACE(extents, 'the east limit of Cynthia Road', '(the east limit of) Cynthia Road', 'g');




UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'North limit of Delia Court and Palm Drive', '(North limit of) Delia Court and Palm Drive', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET  extents = regexp_REPLACE(extents, 'North limit of Delia Court and Palm Drive', '(North limit of) Delia Court and Palm Drive', 'g');



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'the south limit of Gange Avenue', '(the south limit of) Gange Avenue', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET  extents = regexp_REPLACE(extents, 'the south limit of Gange Avenue', '(the south limit of) Gange Avenue', 'g');



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'The northerly limit of the Highway 401 overpass and Belfield Road', '(The northerly limit of the) Highway 401 (overpass) and Belfield Road', 'g');



UPDATE crosic.posted_speed_geoms_2019_all
SET  extents = regexp_REPLACE(extents, 'The northerly limit of the Highway 401 overpass and Belfield Road', '(The northerly limit of the) Highway 401 (overpass) and Belfield Road', 'g');



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = '(The westerly limit of the) Highway 401 (overpass) and a point (approximately) 2150 metres west'
WHERE extents = 'The westerly limit of the Highway 401 overpass and a point (approximately) 2,150 metres west';



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = '(The westerly limit of the) Highway 401 (overpass) and a point (approximately) 2150 metres west'
WHERE extents = 'The westerly limit of the Highway 401 overpass and a point (approximately) 2,150 metres west';



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'East limit of road', '(East limit of road)', 'g')
WHERE extents LIKE 'East limit of road (east of First Street)%';



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'East limit of road', '(East limit of road)', 'g')
WHERE extents LIKE 'East limit of road (east of First Street)%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'The south limit of Lauren Court', '(The south limit of) Lauren Court', 'g')
WHERE extents LIKE 'The south limit of Lauren Court%';



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'The south limit of Lauren Court', '(The south limit of) Lauren Court', 'g')
WHERE extents LIKE 'The south limit of Lauren Court%';



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'the westerly limit of Ridgemount Road', '(the westerly limit of) Ridgemount Road', 'g')
WHERE extents LIKE '%the westerly limit of Ridgemount Road';


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'the westerly limit of Ridgemount Road', '(the westerly limit of) Ridgemount Road', 'g')
WHERE extents LIKE '%the westerly limit of Ridgemount Road';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'the north limit of the bridge south of St. Clair Avenue West', '165 metres north of Russell Hill Drive', 'g')
WHERE extents LIKE '%the north limit of the bridge south of St. Clair Avenue West';


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'the north limit of the bridge south of St. Clair Avenue West', '165 metres north of Russell Hill Drive', 'g')
WHERE extents LIKE '%the north limit of the bridge south of St. Clair Avenue West';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'the west limit of Sunnydene Crescent', '(the west limit of) Sunnydene Crescent', 'g')
WHERE extents LIKE 'the west limit of Sunnydene Crescent%';


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'the west limit of Sunnydene Crescent', '(the west limit of) Sunnydene Crescent', 'g')
WHERE extents LIKE 'the west limit of Sunnydene Crescent%';


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,  '487 metres west of the westerly limit of the Humber Bridge', '487 metres west of (the westerly limit of the Humber Bridge) Humber River Recreational Trl',  'g');


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,  '487 metres west of the westerly limit of the Humber Bridge', '487 metres west of (the westerly limit of the Humber Bridge) Humber River Recreational Trl',  'g');



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,  'Islington Avenue and west limit of road', 'Islington Avenue and (west limit of) Graystone Gardens',  'g');


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,  'Islington Avenue and west limit of road', 'Islington Avenue and (west limit of) Graystone Gardens',  'g');



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,  'Spring Garden Road and East limit of road', 'Spring Garden Road and (East limit of) Orchard Crescent',  'g');


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,  'Spring Garden Road and East limit of road', 'Spring Garden Road and (East limit of) Orchard Crescent',  'g');



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,  'Van Dusen Boulevard and north limit of road', 'Van Dusen Boulevard and (north limit of) Vanellan Court',  'g');


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,  'Van Dusen Boulevard and north limit of road', 'Van Dusen Boulevard and (north limit of) Vanellan Court',  'g');


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,  'A point 478 metres east of Sheppard Avenue East and the east limit of the City of Toronto', 'A point 478 metres east of Sheppard Avenue East and (the east limit of the City of Toronto) Twyn Rivers Drive',  'g');

UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,  'A point 478 metres east of Sheppard Avenue East and the east limit of the City of Toronto', 'A point 478 metres east of Sheppard Avenue East and (the east limit of the City of Toronto) Twyn Rivers Drive',  'g');







-- march 12

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,  'Nursewood Road', 'Balmy Beach Park Trl',  'g')
WHERE id = 2753;

UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,  'Nursewood Road', 'Balmy Beach Park Trl',  'g')
WHERE id = 2753;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'a point 15 metres south of Simpson Avenue and a point 37 metres south of Simpson Avenue'
WHERE id = 2830;

UPDATE crosic.posted_speed_geoms_2019_all
SET extents = 'a point 15 metres south of Simpson Avenue and a point 37 metres south of Simpson Avenue'
WHERE id = 2830;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,  ' lof ', ' of ',  'g')
WHERE id = 1896;

UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,  ' lof ', ' of ',  'g')
WHERE id = 1896;



-- march 18 

UPDATE crosic.posted_speed_geoms_2019_all
SET extents = 'Palace Pier Court and Humber River'
WHERE id = 2206;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Palace Pier Court and Humber River'
WHERE id = 2206;





UPDATE crosic.posted_speed_geoms_2019_all
SET extents = 'Entire Length'
WHERE id = 1081;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Entire Length'
WHERE id = 1081;


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = 'Entire Length'
WHERE id = 1082;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Entire Length'
WHERE id = 1082;



UPDATE crosic.posted_speed_geoms_2019_all
SET street_name = regexp_replace(street_name, 'Frederick G. Gardiner Expressway', 'GARDINER EXPRESS')
WHERE id IN (1459, 1457, 1460, 1458);


UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = regexp_replace(street_name, 'Frederick G. Gardiner Expressway', 'GARDINER EXPRESS')
WHERE id IN (1459, 1457, 1460, 1458);







UPDATE crosic.speed_limit_conversion_text_update 
SET extents = '427 X S Gardiner X E Ramp and Humber River' 
WHERE id IN (1459, 1458);

UPDATE crosic.posted_speed_geoms_2019_all
SET extents = '427 X S Gardiner X E Ramp and Humber River' 
WHERE id IN (1459, 1458);





UPDATE crosic.posted_speed_geoms_2019_all
SET extents = 'A point 244 metres west of Gardiner W S Kingsway Ramp and Lake Shore Boulevard East'
WHERE id IN (1460, 1457);


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'A point 244 metres west of Gardiner W S Kingsway Ramp and Lake Shore Boulevard East'
WHERE id IN (1460, 1457);


-- ABOVE NOT MATCHING TO CORRECT LINE GEOM (1457, 1460)



-- march 19 

/*

CREATE FUNTION update_val(tbl_name text, new_extents text, id int)
RETURNS VOID AS $$ 
BEGIN
RETRUN QUERY EXECUTE 'UPDATE ' || tbl_name || ' SET extents = ' || 'new_extents' || ' WHERE id = ' || id::TEXT
END; 
$$ LANGUAGE plpgsql;

*/

-- march 20 


UPDATE crosic.posted_speed_geoms_2019_all
SET street_name = 'Strickland Ave'
WHERE id IN (3616);


UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Strickland Ave'
WHERE id IN (3616);



-- march 26

UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Sackville Street', 
extents = 'Shuter Street/Sackville Street and (the north end of) Sackville Street'
WHERE id IN (3298);

UPDATE crosic.posted_speed_geoms_2019_all
SET street_name = 'Sackville Street', 
extents = 'Shuter Street/Sackville Street and (the north end of) Sackville Street'
WHERE id IN (3298);


UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'St David Street'
WHERE id IN (3524);

UPDATE crosic.posted_speed_geoms_2019_all
SET street_name = 'St David Street'
WHERE id IN (3524);


UPDATE  crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'The north end of ', '(The north end of) ');

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'The north end of ', '(The north end of) ');



UPDATE  crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'The north end of ', '(The north end of) ');

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents,'The north end of ', '(The north end of) ');



UPDATE  crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'Crescent', 'Cres')
WHERE id IN (945);

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'Crescent', 'Cres')
WHERE id IN (945);




UPDATE  crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents,'approximatley', '');


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'approximatley', '');


UPDATE  crosic.posted_speed_geoms_2019_all
SET street_name = 'Lake Shore Drive', 
extents = 'Entire Length (East limit of road east of First Street and Waniska Avenue west of Thirteenth Street)'
WHERE id IN (2209);

UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Lake Shore Drive', 
extents = 'Entire Length (East limit of road east of First Street and Waniska Avenue west of Thirteenth Street)'
WHERE id IN (2209);


UPDATE  crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, 'A point', '')
WHERE id IN (770);

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, 'A point', '')
WHERE id IN (770);



UPDATE  crosic.posted_speed_geoms_2019_all
SET extents = 'Cranfield Road and Bermondsey Road'
WHERE id IN (4574);

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Cranfield Road and Bermondsey Road'
WHERE id IN (4574);


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = regexp_REPLACE(extents, ',', '')
WHERE id IN (2005);

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = regexp_REPLACE(extents, ',', '')
WHERE id IN (2005);



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = 'Victoria Park Avenue (south intersection) and Victoria Park Avenue (north intersection)'
WHERE id IN (1729);

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Victoria Park Avenue (south intersection) and Victoria Park Avenue (north intersection)'
WHERE id IN (1729);



UPDATE crosic.posted_speed_geoms_2019_all
SET extents = '(West branch of) Spadina Crescent and Robert Street'
WHERE id IN (3288);

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = '(West branch of) Spadina Crescent and Robert Street'
WHERE id IN (3288);


UPDATE crosic.posted_speed_geoms_2019_all
SET extents = 'Entire Length'
WHERE id IN (3734);

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Entire Length'
WHERE id IN (3734);


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Shaunavon Heights Crescent (east intersection) and Mewata Gate'
WHERE id IN (537);


UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = 'Shaunavon Heights Crescent (east intersection) and Mewata Gate'
WHERE id IN (537);



UPDATE crosic.speed_limit_conversion_text_update 
SET extents = '138 metres northwest of Hullrick Drive (west intersection) and a point 970 metres (further) north'
WHERE id IN (1975);


UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = '138 metres northwest of Hullrick Drive (west intersection) and a point 970 metres (further) north'
WHERE id IN (1975);


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'George Henry Boulevard and Parkway Forest Drive'
WHERE id IN (1430);


UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = 'George Henry Boulevard and Parkway Forest Drive'
WHERE id IN (1430);


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'A point 77.64 metres west of ALLEN Road and Bathurst Street'
WHERE id IN (4717);


UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = 'A point 77.64 metres west of ALLEN Road and Bathurst Street'
WHERE id IN (4717);


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Entire Length'
WHERE id IN (2063);

UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = 'Entire Length'
WHERE id IN (2063);


UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Turnbridge Crescent'
WHERE id IN (4162);

UPDATE crosic.posted_speed_geoms_2019_all 
SET street_name = 'Turnbridge Crescent'
WHERE id IN (4162);

UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = 'Stanley Greene Boulevard and Frederick Tisdale Drive'
WHERE id = 6342;

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Stanley Greene Boulevard and Frederick Tisdale Drive'
WHERE id = 6342;


UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = '(West branch of) Kenilworth Avenue and Norway Avenue'
WHERE id = 2107;

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = '(West branch of) Kenilworth Avenue and Norway Avenue'
WHERE id = 2107;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'A point 150 metres southweast of Teak Avenue and (the southwest end of) Carnforth Road'
WHERE id = 673;

UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = 'A point 150 metres southweast of Teak Avenue and (the southwest end of) Carnforth Road'
WHERE id = 673;

-- intersections DNE: id = 2472, 2005



-- March 27
UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Clydesdale Drive and a point 120 metres north'
WHERE id = 2091;

UPDATE crosic.posted_speed_geoms_2019_all 
SET extents = 'Clydesdale Drive and a point 120 metres north'
WHERE id = 2091;



UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'ALLEN RD'
WHERE id = 4004;

UPDATE crosic.posted_speed_geoms_2019_all 
SET street_name = 'ALLEN RD'
WHERE id = 4004;



-- March 28 


UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Pardee Avenue'
WHERE id = 5516;

UPDATE crosic.posted_speed_geoms_2019_all 
SET street_name = 'Pardee Avenue'
WHERE id = 5516;



-- march 29 

UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Cuffley Cres S'
WHERE id = 945;

UPDATE crosic.posted_speed_geoms_2019_all 
SET street_name = 'Cuffley Cres S'
WHERE id = 945;



UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'Ln N Bloor E Brentwood', extents = 'Brentwood Road North and (the east end of) Ln N Bloor E Brentwood'
WHERE id = 1401;

UPDATE crosic.posted_speed_geoms_2019_all 
SET street_name = 'Ln N Bloor E Brentwood', extents = 'Brentwood Road North and (the east end of) Ln N Bloor E Brentwood'
WHERE id = 1401;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Hotspur Road and Ameer Avenue'
WHERE id = 6499;

UPDATE crosic.posted_speed_geoms_2019_all_2
SET street_name = 'Ln N Bloor E Brentwood', extents = 'Brentwood Road North and (the east end of) Ln N Bloor E Brentwood'
WHERE id = 6499;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Entire Length'
WHERE id = 6484;

UPDATE crosic.posted_speed_geoms_2019_all_2
SET extents = 'Entire Length'
WHERE id = 6484;




UPDATE crosic.posted_speed_geoms_2019_all_2
SET extents = 'Holland Avenue and a point 43.28 metres north'
WHERE id = 6412;

UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Holland Avenue and a point 43.28 metres north'
WHERE id = 6412;


UPDATE crosic.posted_speed_geoms_2019_all_2
SET extents = regexp_REPLACE(extents, '\(the\(the west end of\)\)', '(the west end of)', 'g');


UPDATE crosic.speed_limit_conversion_text_update 
SET extents =  regexp_REPLACE(extents, '\(the\(the west end of\)\)', '(the west end of)', 'g');


UPDATE crosic.posted_speed_geoms_2019_all_2
SET extents = 'Holland Avenue and a point 43.28 metres north'


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Holland Avenue and a point 43.28 metres north'


UPDATE crosic.posted_speed_geoms_2019_all_2
SET street_name = 'ALLEN RD'
WHERE id IN (4000, 4003, 4004);


UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'ALLEN RD'
WHERE id IN (4000, 4003, 4004);

/*
SELECT text_to_centreline('ALLEN RD', 'A point 600 metres north of Eglinton Avenue West and Lawrence Avenue West', NULL)


SELECT * FROM crosic.speed_limit_conversion_text_update 
WHERE street_name LIKE '%ALLEN%' or street_name LIKE '% Allen %'
*/


-- April 1


UPDATE crosic.posted_speed_geoms_2019_all_2
SET street_name = 'St Ives Crescent'
WHERE id IN (6543, 6544);


UPDATE crosic.speed_limit_conversion_text_update 
SET street_name = 'St Ives Crescent'
WHERE id IN (6543, 6544);


-- needs to be manually inputted later: "1453" 

UPDATE crosic.posted_speed_geoms_2019_all_2
SET extents = 'Entire Length'
WHERE id = 2581;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Entire Length'
WHERE id = 2581;


UPDATE crosic.posted_speed_geoms_2019_all_2
SET extents = 'Prince Edward Drive S'
WHERE id = 3004;


UPDATE crosic.speed_limit_conversion_text_update 
SET extents = 'Prince Edward Drive S'
WHERE id = 3004;

/*
UPDATE crosic.posted_speed_geoms_2019_all_2 x
SET geom = u.geom
FROM crosic.posted_speed_geoms_2019_all_originally_unmatched_2 u
WHERE x.geom is null and x.id = u.id
*/
