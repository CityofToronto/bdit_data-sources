UPDATE miovision_api.intersections
SET
    s_leg_restricted = CASE
        WHEN api_name = 'Lawrence Avenue East and Fortune Gate' THEN TRUE
        WHEN api_name = 'Lawrence Avenue East and Mossbank Drive' THEN TRUE
        WHEN api_name = 'Orton Park Road and Lawrence Avenue East' THEN TRUE

    END
WHERE api_name IN (
    'Lawrence Avenue East and Fortune Gate',
    'Lawrence Avenue East and Mossbank Drive',
    'Orton Park Road and Lawrence Avenue East'
);

UPDATE miovision_api.intersections
SET
    n_leg_restricted = CASE
        WHEN api_name = 'Overture Road and Lawrence Avenue East' THEN TRUE

    END
WHERE api_name IN (
    'Overture Road and Lawrence Avenue East'
);
