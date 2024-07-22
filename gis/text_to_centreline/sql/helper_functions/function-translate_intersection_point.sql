-- Modified the function so that the translation is happening at an angle 17.5 like the Toronto map
CREATE OR REPLACE FUNCTION gis._translate_intersection_point(
    oid_geom GEOMETRY, metres FLOAT, direction TEXT
)
RETURNS GEOMETRY AS $translated_geom$
DECLARE
translated_geom TEXT := (
	(CASE WHEN TRIM(direction) = 'west' THEN ST_Translate(oid_geom, -metres, 0)
	WHEN TRIM(direction) = 'east' THEN ST_Translate(oid_geom, metres, 0)
	WHEN TRIM(direction) = 'north' THEN ST_Translate(oid_geom, 0, metres)
	WHEN TRIM(direction) = 'south' THEN ST_Translate(oid_geom, 0, -metres)
	WHEN TRIM(direction) IN ('southwest', 'south west') THEN ST_Translate(oid_geom, -cos(45)*metres, -sin(45)*metres)
	WHEN TRIM(direction) IN ('southeast', 'south east') THEN ST_Translate(oid_geom, cos(45)*metres, -sin(45)*metres)
	WHEN TRIM(direction) IN ('northwest', 'north west') THEN ST_Translate(oid_geom, -cos(45)*metres, sin(45)*metres)
	WHEN TRIM(direction) IN ('northeast', 'north east') THEN ST_Translate(oid_geom, cos(45)*metres, sin(45)*metres)
	END)
	);

BEGIN
--rotate every point to 17.5 degree anticlockwise (0.305 radians) based on the intersection point
RETURN ST_Transform(ST_SetSRID(ST_Rotate(translated_geom, 0.305, oid_geom), 2952), 4326);

END;
$translated_geom$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gis._translate_intersection_point(
    oid_geom GEOMETRY, metres FLOAT, direction TEXT
) IS '
Inputs are the geometry of a point, the number of units that you would like the point to be translated, and the direction that you would like the point to be translated.
The function returns the input point geometry translated in the direction inputted, by the number of metres inputted.
Note: if you would like to move your geometry by a certian number of metres, make sure the point geometry has a projection that has metres as the unit.
If the input geometry is unprojected (i.e. SRID = 4326) then the function returns a geometry that has been translated by the number of degrees inputted.
';
