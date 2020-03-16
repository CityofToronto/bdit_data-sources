CREATE OR REPLACE FUNCTION jchew._centreline_separate_an_interxn_and_offset
(line_geom_cut GEOMETRY, ind_line_geom GEOMETRY, oid1_geom GEOMETRY, combined_section NUMRANGE,
OUT section NUMRANGE, OUT line_geom_separated GEOMETRY)

LANGUAGE 'plpgsql'
AS $BODY$

BEGIN

section :=
--combined_section = '[0,1]'
(CASE WHEN lower(combined_section) = 0 AND upper(combined_section) = 1
THEN numrange(0, 1,'[]')

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
THEN numrange(0, 1, '[]')

--where part of the centreline is within the buffer, and then find out the startpoint of the individual centreline to know which part of the centreline needs to be cut
WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
THEN numrange(0, (ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))::numeric, '[]')

WHEN ST_Within(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
THEN numrange((ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))::numeric, 1, '[]')  

ELSE NULL

END);

/*
ELSE (CASE WHEN ST_Within(ST_Transform(ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
    THEN numrange(0, 1, '[]') 
    ELSE
    (CASE WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
             THEN numrange(0, (ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))::numeric, '[]') 
        ELSE numrange((ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))::numeric, 1, '[]')  
        END)
    END)
END);
*/

line_geom_separated :=
--combined_section = '[0,1]'
(CASE WHEN lower(combined_section) = 0 AND upper(combined_section) = 1
THEN ind_line_geom

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
THEN ind_line_geom

--where part of the centreline is within the buffer, and then find out the startpoint of the individual centreline to know which part of the centreline needs to be cut
WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
THEN ST_LineSubstring(ind_line_geom, 0 , ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))

WHEN ST_Within(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
THEN ST_LineSubstring(ind_line_geom, ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)), 1)  

ELSE NULL

END);

/*
ELSE (CASE WHEN ST_Within(ST_Transform(ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
    THEN ind_line_geom
    ELSE
    (CASE WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=flat join=round')) = TRUE
             THEN line_geom_separated = ST_LineSubstring(ind_line_geom, 0 , ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))
        ELSE line_geom_separated = ST_LineSubstring(ind_line_geom, ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)), 1)  
        END)
    END)
END);
*/

RAISE NOTICE 'Centrelines are now separated into their respective geo_id row. combined_section: %, section: %',
combined_section, section;

END;
$BODY$;


COMMENT ON FUNCTION jchew._centreline_cut_an_interxn_and_offset(text, FLOAT, geometry,geometry, geometry) IS '
Meant to split line geometries of bylaw in effect locations where the bylaw occurs between an intersection and an offset.
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information';
