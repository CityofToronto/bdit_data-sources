--DROP VIEW gwolofs.miovision_valid_legs;

CREATE OR REPLACE VIEW gwolofs.miovision_valid_legs AS

WITH individual_tests AS (
    SELECT
        ael.intersection_uid,
        ael.intersection_name,
        ael.data_date AS dt,
        ael.classification,
        ael.leg,
        ael.pass
    FROM miovision_validation.agg_error_leg AS ael
    --this test applies to all modes
    WHERE classification NOT IN ('vehicle_light')
    
    UNION ALL
    
    SELECT
        pel.intersection_uid,
        pel.intersection_name,
        pel.dt,
        pel.classification,
        pel.leg,
        pass_95th_percentile
    FROM miovision_validation.percentle_error_leg AS pel
    --this test does not apply to vehicles, which are measured at the movement level
    WHERE classification NOT IN ('vehicle_all', 'vehicle_light')
    
    UNION ALL
    
    SELECT
        pem.intersection_uid,
        pem.intersection_name,
        pem.dt,
        pem.classification,
        pem.leg,
        pem.pass_85th_percentile
    FROM miovision_validation.percentle_error_mvmt AS pem
    --this only applies to vehicles, while bikes/peds are measured at the leg level
    WHERE
        classification IN ('vehicle_all')
        AND classification NOT IN ('vehicle_light')
)

SELECT
    intersection_uid,
    intersection_name,
    dt AS start_date,
    lead(dt) OVER (PARTITION BY intersection_uid, intersection_name, classification, leg ORDER BY dt) AS end_date,
    classification,
    CASE classification
        WHEN 'pedestrian' THEN '{6}' 
        WHEN 'bike_tmc' THEN '{2}'
        WHEN 'bike_approach' THEN '{10}'
        WHEN 'vehicle_all' THEN '{1, 3, 4, 5, 9}'
        WHEN 'vehicle_light' THEN '{1}'
    END AS classification_uids,
    leg,
    bool_and(pass) AS all_pass
FROM individual_tests
GROUP BY
    intersection_uid,
    intersection_name,
    dt,
    classification,
    classification_uids,
    leg;

SELECT * FROM gwolofs.miovision_valid_legs WHERE all_pass;
