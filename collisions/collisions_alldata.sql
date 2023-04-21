-- View to shorten joining the events and involved tables together, effectively
-- creating a version of `collisions.acc` with all numeric codes translated into
-- text. This is a very common first analysis step.


CREATE OR REPLACE VIEW collisions.alldata AS (
    SELECT *
    FROM collisions.events
    LEFT JOIN collisions.involved USING (collision_no)
);

ALTER TABLE collisions.alldata
OWNER TO collision_admins;

COMMENT ON VIEW collisions.alldata
IS 'View linking collisions.events and collisions.involved together through collision_no.';

GRANT SELECT ON TABLE collisions.alldata TO bdit_humans;
GRANT SELECT ON TABLE collisions.alldata TO rsaunders;
GRANT SELECT ON TABLE collisions.alldata TO kchan;
GRANT SELECT ON TABLE collisions.alldata TO ksun;
