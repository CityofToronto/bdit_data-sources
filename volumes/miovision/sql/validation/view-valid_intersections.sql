--DROP VIEW gwolofs.miovision_valid_intersections;

CREATE OR REPLACE VIEW gwolofs.miovision_valid_intersections AS
SELECT
    intersection_uid,
    intersection_name,
    start_date,
    end_date,
    classification,
    classification_uids,
    bool_and(all_pass) AS all_pass
FROM gwolofs.miovision_valid_legs
GROUP BY
    intersection_uid,
    intersection_name,
    start_date,
    end_date,
    classification,
    classification_uids;

SELECT * FROM gwolofs.miovision_valid_intersections WHERE all_pass;
