--First create a table
CREATE TABLE IF NOT EXISTS gis.cleaned_bylaws_text (
    bylaw_id int,
    highway2 text,
    btwn1 text,
    direction_btwn1 text,
    metres_btwn1 float,
    btwn2 text,
    direction_btwn2 text,
    metres_btwn2 float,
    btwn2_orig text,
    btwn2_check text
);

--Then, create a function 
DROP FUNCTION IF EXISTS gis._clean_bylaws_text(int, text, text, text);
CREATE OR REPLACE FUNCTION gis._clean_bylaws_text(
    _bylaw_id int, highway text, frm text, t text
)
RETURNS gis.cleaned_bylaws_text
LANGUAGE 'plpgsql'
AS $$


DECLARE

--STEP 1
    -- clean data

    -- when the input was btwn instead of from and to
    btwn1_v1 text := CASE WHEN t IS NULL THEN
    gis.abbr_street(regexp_REPLACE
    (regexp_REPLACE
    (regexp_REPLACE
    (split_part
    (split_part
    (regexp_REPLACE
    (regexp_REPLACE(frm, '[0123456789.,]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast) of ', '', 'g')
    , '\(.*?\)', '', 'g')
    , ' to ', 1)
    , ' and ', 1)
    , '\(.*\)', '', 'g')
    , '[Bb]etween ', '', 'g')
    , 'A point', '', 'g'))
    
    ELSE gis.abbr_street(regexp_REPLACE
    (regexp_REPLACE
    (regexp_REPLACE(frm, '\(.*?\)', '', 'g')
    , '[0123456789.,]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast) of ', '', 'g')
    , 'A point', '', 'g'))
    END;

    -- cases when three roads intersect at once (i.e. btwn1_v1 = Terry Dr/Woolner Ave) ... just take first street
    btwn1 text := (CASE WHEN btwn1_v1 LIKE '%/%'
            THEN split_part(btwn1_v1, '/', 1)
            WHEN btwn1_v1 LIKE '% of %' THEN split_part(btwn1_v1, ' of ', 2)
            ELSE btwn1_v1
            END
    );

    btwn2_orig_v1 text := CASE WHEN t IS NULL THEN
            (CASE WHEN split_part(regexp_REPLACE(frm,  '\(.*?\)', '', 'g'), ' and ', 2) <> ''
            THEN gis.abbr_street(regexp_REPLACE
            (split_part
            (regexp_REPLACE
            (regexp_REPLACE
            (regexp_REPLACE(frm, '\(.*?\)', '', 'g')
            , '[0123456789.,]* metres (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) of ', '', 'g')
            , 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g')
            , ' and ', 2)
            --Delete 'thereof' and some other words
            , '[Bb]etween |(A point)|(thereof)|(the northeast of)', '', 'g'))

            WHEN split_part(frm, ' to ', 2) <> ''
            THEN gis.abbr_street(regexp_REPLACE
            (regexp_REPLACE
            (split_part
            (regexp_REPLACE
            (regexp_REPLACE
            (regexp_REPLACE(frm, '\(.*?\)', '', 'g')
            , '[0123456789.,]* metres (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) of ', '', 'g')
            , 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g')
            , ' to ', 2)
            , '[Bb]etween ', '', 'g')
            , 'A point', '', 'g'))
            END)

            ELSE
            gis.abbr_street(regexp_REPLACE
            (regexp_REPLACE
            (regexp_REPLACE
            (regexp_REPLACE(t, '\(.*?\)', '', 'g')
            , '[0123456789.]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast|south west) of ', '', 'g')
            , 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g')
            , 'the northeast of', '', 'g'))
            END ;

    -- cases when three roads intersect at once (i.e. btwn2_orig_v1 = Terry Dr/Woolner Ave)
    -- if there is still a '/' after cresting btwn2_orig_v1 then we know there are 3 intersections in one
    btwn2_orig text := (CASE WHEN btwn2_orig_v1 LIKE '%/%'
            THEN split_part(btwn2_orig_v1, '/', 1)
            ELSE btwn2_orig_v1
            END
    );

    highway2 text :=  gis.abbr_street(highway);

    direction_btwn1 text := CASE WHEN t IS NULL THEN
                (
                CASE WHEN btwn1 LIKE '% m %'
                OR gis.abbr_street( regexp_REPLACE
                (split_part
                (split_part
                (frm, ' to ', 1)
                , ' and ', 1)
                , '[Bb]etween ', '', 'g')) LIKE '% m %'
                THEN split_part
                (split_part
                (gis.abbr_street
                (regexp_REPLACE
                (regexp_REPLACE
                (regexp_REPLACE
                (split_part
                (split_part
                (frm, ' to ', 1)
                , ' and ', 1)
                , '[Bb]etween ', '', 'g')
                , 'further ', '', 'g')
                , 'east/north', 'northeast', 'g'))
                , ' m ', 2)
                , ' of ', 1)
                ELSE NULL
                END )
                ELSE
                (
                CASE WHEN btwn1 LIKE '% m %'
                OR gis.abbr_street(frm) LIKE '% m %'
                THEN regexp_replace(regexp_replace
                (split_part
                (split_part
                (gis.abbr_street(frm), ' m ', 2)
                , ' of ', 1)
                , 'further ', '', 'g')
                , 'east/north', 'northeast', 'g')
                ELSE NULL
                END )
                END;


    direction_btwn2 text := CASE WHEN t IS NULL THEN (
                CASE WHEN btwn2_orig LIKE '% m %'
                OR
                (
                    CASE WHEN split_part(frm, ' and ', 2) <> ''
                    THEN gis.abbr_street( regexp_REPLACE(split_part(frm, ' and ', 2), '[Bb]etween ', '', 'g'))
                    WHEN split_part(frm, ' to ', 2) <> ''
                    THEN gis.abbr_street( regexp_REPLACE(split_part(frm, ' to ', 2), '[Bb]etween ', '', 'g'))
                    END
                ) LIKE '% m %'
                THEN
                (
                    CASE WHEN split_part(frm, ' and ', 2) <> ''
                    THEN regexp_REPLACE(regexp_replace(split_part(split_part( gis.abbr_street(regexp_REPLACE(split_part(frm, ' and ', 2), '[Bb]etween ', '', 'g')), ' m ', 2), ' of ', 1), 'further | thereof', '', 'g'), 'east/north', 'northeast', 'g')
                    WHEN split_part(frm, ' to ', 2) <> ''
                    THEN regexp_REPLACE(regexp_replace(split_part(split_part(gis.abbr_street(regexp_REPLACE(split_part(frm, ' to ', 2), '[Bb]etween ', '', 'g')), ' m ', 2), ' of ', 1), 'further | thereof', '', 'g'), 'east/north', 'northeast', 'g')
                    END
                )
                ELSE NULL
                END)
                ELSE
                (
                CASE WHEN btwn2_orig LIKE '% m %'
                OR gis.abbr_street(t) LIKE '% m %'
                THEN
                regexp_REPLACE(regexp_replace(split_part(split_part(gis.abbr_street(t), ' m ', 2), ' of ', 1), 'further ', '', 'g'), 'east/north', 'northeast', 'g')
                ELSE NULL
                END
                )
                END;


    metres_btwn1 float :=    (CASE WHEN t IS NULL THEN
                (
                CASE WHEN btwn1 LIKE '% m %'
                OR gis.abbr_street(regexp_REPLACE
                (split_part
                (split_part(frm, ' to ', 1)
                , ' and ', 1)
                , 'Between ', '', 'g')) LIKE '% m %'
                THEN regexp_REPLACE
                (regexp_REPLACE
                (regexp_REPLACE
                (split_part
                (gis.abbr_street
                (regexp_REPLACE
                (split_part
                (split_part(frm, ' to ', 1)
                , ' and ', 1)
                , '[Bb]etween ', '', 'g'))
                , ' m ' ,1)
                , 'a point ', '', 'g')
                , 'A point', '', 'g')
                , ',', '', 'g')::float
                ELSE NULL
                END
                )
                ELSE
                (
                CASE WHEN btwn1 LIKE '% m %'
                OR gis.abbr_street(frm) LIKE '% m %'
                THEN regexp_REPLACE
                (regexp_REPLACE
                (regexp_REPLACE
                (split_part
                (gis.abbr_street(frm), ' m ' ,1)
                , 'a point ', '', 'g')
                , 'A point', '', 'g')
                , ',', 'g')::float
                ELSE NULL
                END
                )
                END)::float;


    metres_btwn2 float :=    (CASE WHEN t IS NULL THEN
                ( CASE WHEN btwn2_orig LIKE '% m %' OR
                    (
                        CASE WHEN split_part(frm, ' and ', 2) <> ''
                        THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' and ', 2), '\(.*?\)', '', 'g'), '[Bb]etween ', '', 'g'))
                        WHEN split_part(frm, ' to ', 2) <> ''
                        THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' to ', 2), '\(.*?\)', '', 'g'), '[Bb]etween ', '', 'g'))
                        END
                    )
                LIKE '% m %'
                THEN
                (
                CASE WHEN split_part(frm, ' and ', 2) <> ''
                THEN regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part( gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' and ', 2), '\(.*\)', '', 'g'), '[Bb]etween ', '', 'g')), ' m ', 1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', '', 'g')::float
                WHEN split_part(frm, ' to ', 2) <> ''
                THEN regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' to ', 2), '\(.*\)', '', 'g'), '[Bb]etween ', '', 'g')), ' m ', 1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', '', 'g')::float
                END
                )
                ELSE NULL
                END )

                ELSE
                (
                CASE WHEN btwn2_orig LIKE '% m %'
                OR gis.abbr_street(t) LIKE '% m %'
                THEN
                regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(gis.abbr_street(t), ' m ', 1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', '', 'g')::float
                ELSE NULL
                END
                )
                END)::float;

    -- to help figure out if the row is case 1
    -- i.e. Watson road from St. Mark's Road to a point 100 metres north
    -- we want the btwn2 to be St. Mark's Road (which is also btwn1)
    -- there are also cases like: street= Arundel Avenue  and  btwn=  Danforth Avenue and a point 44.9 metres north of Fulton Avenue
    -- we need to be able to differentiate the two cases
    -- the difference between the two is that one of the cases has a 'of' to describe the second road that intersects with "street"/"highway2"
    btwn2_check text := CASE WHEN t IS NULL THEN
            (CASE WHEN split_part(frm, ' and ', 2) <> ''
            THEN gis.abbr_street(regexp_REPLACE(regexp_REPLACE(split_part(frm, ' and ', 2), '[Bb]etween ', '', 'g'), 'A point', '', 'g'))
            WHEN split_part(frm, ' to ', 2) <> ''
            THEN gis.abbr_street(regexp_REPLACE(regexp_REPLACE(split_part(frm, ' to ', 2), '[Bb]etween ', '', 'g'), 'A point', '', 'g'))
            END)

            ELSE
            gis.abbr_street(t)
            END ;


    btwn2 text := (
    CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
        -- for case one
        -- i.e. Watson road from St. Mark's Road to a point 100 metres north
        -- we want the btwn2 to be St. Mark's Road (which is also btwn1)
    THEN TRIM(gis.abbr_street(btwn1))
    ELSE TRIM(gis.abbr_street(regexp_replace(btwn2_orig , 'a point', '', 'g')))
    END
    );

BEGIN
RAISE NOTICE 'btwn1: %, btwn2: %, btwn2_check: %, highway2: %, metres_btwn1: %, metres_btwn2: %, direction_btwn1: %, direction_btwn2: %', 
btwn1, btwn2, btwn2_check, highway2, metres_btwn1, metres_btwn2, direction_btwn1, direction_btwn2;

RETURN ROW(_bylaw_id, highway2, btwn1, direction_btwn1, metres_btwn1, btwn2, direction_btwn2, metres_btwn2,
btwn2_orig, btwn2_check)::gis.cleaned_bylaws_text ;

END;
$$;


--For testing purposes only
DO $$
DECLARE
 return_test gis.cleaned_bylaws_text; --the table
BEGIN
 return_test := gis._clean_bylaws_text(123::int, 'Chesham Drive'::text, 'The west end of Chesham Drive and Heathrow Drive'::text, NULL::text); --the function
 RAISE NOTICE 'Testing 123';
END;
$$ LANGUAGE 'plpgsql';
