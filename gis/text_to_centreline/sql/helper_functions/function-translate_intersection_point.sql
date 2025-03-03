-- Modified the function so that the translation is happening at an angle 17.5 like the Toronto map
CREATE OR REPLACE FUNCTION gis._translate_intersection_point(
    oid_geom geometry, metres float, direction text
)
RETURNS geometry AS $translated_geom$
DECLARE
direction text := trim(lower(_translate_intersection_point.direction));
translated_geom text := (
    CASE
        WHEN regexp_instr(direction, '(?<!\S)(w|west)(?!\S)') = 1
        THEN ST_Translate(oid_geom, -metres, 0)
        WHEN regexp_instr(direction, '(?<!\S)(e|east)(?!\S)') = 1
        THEN ST_Translate(oid_geom, metres, 0)
        WHEN regexp_instr(direction, '(?<!\S)(n|north)(?!\S)') = 1
        THEN ST_Translate(oid_geom, 0, metres)
        WHEN regexp_instr(direction, '(?<!\S)(s|south)(?!\S)') = 1
        THEN ST_Translate(oid_geom, 0, -metres)
        WHEN direction = 'southwest'
        THEN ST_Translate(oid_geom, -cos(45)*metres, -sin(45)*metres)
        WHEN direction = 'southeast'
        THEN ST_Translate(oid_geom, cos(45)*metres, -sin(45)*metres)
        WHEN direction = 'northwest'
        THEN ST_Translate(oid_geom, -cos(45)*metres, sin(45)*metres)
        WHEN direction = 'northeast'
        THEN ST_Translate(oid_geom, cos(45)*metres, sin(45)*metres)
    END
);

BEGIN
--rotate every point to 17.5 degree anticlockwise (0.305 radians) based on the intersection point
RETURN ST_Transform(ST_SetSRID(ST_Rotate(translated_geom, 0.305, oid_geom), 2952), 4326);

END;
$translated_geom$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gis._translate_intersection_point(
    oid_geom geometry, metres float, direction text
) IS 'Inputs are the geometry of a point, the number of units that you would like the point to be
translated, and the direction that you would like the point to be translated. The function
returns the input point geometry translated in the direction inputted, by the number of metres
inputted. Note: if you would like to move your geometry by a certian number of metres, make
sure the point geometry has a projection that has metres as the unit. If the input geometry
is unprojected (i.e. SRID = 4326) then the function returns a geometry that has been translated
by the number of degrees inputted.';
