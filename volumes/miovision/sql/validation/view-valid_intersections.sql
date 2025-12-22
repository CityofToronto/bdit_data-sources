--DROP VIEW miovision_validation.valid_intersections;

--DROP VIEW miovision_validation.valid_intersections_view;

CREATE VIEW miovision_validation.valid_intersections_view AS
SELECT
    intersection_uid,
    intersection_name,
    start_date,
    end_date,
    classification,
    classification_uids,
    bool_and(all_pass) AS all_pass
FROM miovision_validation.valid_legs_view
GROUP BY
    intersection_uid,
    intersection_name,
    start_date,
    end_date,
    classification,
    classification_uids;

SELECT * FROM miovision_validation.valid_intersections_view WHERE all_pass;

ALTER TABLE miovision_validation.valid_intersections_view
OWNER TO miovision_validators;

GRANT SELECT ON TABLE miovision_validation.valid_intersections_view TO bdit_humans;
GRANT SELECT ON TABLE miovision_validation.valid_intersections_view TO miovision_validation_bot;
GRANT ALL ON TABLE miovision_validation.valid_intersections_view TO miovision_validators;
