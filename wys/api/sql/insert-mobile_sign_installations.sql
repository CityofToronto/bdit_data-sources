WITH new_data (
    ward_no, location, from_street, to_street, direction,
    installation_date, removal_date, new_sign_number, comments,
    work_order, confirmed
) AS (
    VALUES %s --noqa: PRS
),

dupes AS (
    SELECT
        new_sign_number,
        installation_date
    FROM new_data
    GROUP BY
        new_sign_number,
        installation_date
    HAVING COUNT(*) > 1
),

full_dupes AS (
    /*Identify full rows that will violate the unique constraint in the upsert*/
    INSERT INTO wys.mobile_sign_installations_dupes
    SELECT
        n.ward_no::int,
        n.location,
        n.from_street,
        n.to_street,
        n.direction,
        n.installation_date,
        n.removal_date::date,
        n.new_sign_number,
        n.comments,
        n.work_order
    FROM new_data AS n
    NATURAL JOIN dupes
    ON CONFLICT (
        location, from_street, to_street, direction, installation_date, removal_date,
        new_sign_number, comments
    )
    /*do update required here because do nothing does not return dupes
    on conflict. */
    DO UPDATE SET 
    location = excluded.location,
    from_street = excluded.from_street,
    to_street = excluded.to_street,
    direction = excluded.direction,
    removal_date = excluded.removal_date,
    comments = excluded.comments
    RETURNING new_sign_number, installation_date
)

INSERT INTO wys.mobile_sign_installations AS existing (
    ward_no, location, from_street, to_street, direction, installation_date,
    removal_date, new_sign_number, comments, confirmed, work_order
)
SELECT
    n.ward_no::int,
    n.location,
    n.from_street,
    n.to_street,
    n.direction,
    n.installation_date,
    n.removal_date::date,
    n.new_sign_number,
    n.comments,
    n.confirmed,
    n.work_order
FROM new_data AS n
LEFT JOIN full_dupes AS dupes USING (new_sign_number, installation_date)
--Don't try to insert dupes
WHERE dupes.new_sign_number IS NULL
ON CONFLICT (ward_no, installation_date, new_sign_number)
DO UPDATE SET
removal_date = excluded.removal_date,
comments = excluded.comments,
direction = excluded.direction,
location = excluded.location,
from_street = excluded.from_street,
to_street = excluded.to_street,
work_order = excluded.work_order
-- Prevent unnecessary update if data are unchanged
WHERE
(
    existing.removal_date IS NULL
    OR existing.removal_date != excluded.removal_date
)
OR (
    existing.comments IS NULL
    OR existing.comments != excluded.comments
)
OR (
    existing.direction IS NULL
    OR existing.direction != excluded.direction
)
OR (
    existing.location IS NULL
    OR existing.location != excluded.location
)
OR (
    existing.from_street IS NULL
    OR existing.from_street != excluded.from_street
)
OR (
    existing.to_street IS NULL
    OR existing.to_street != excluded.to_street
)
OR (
    existing.work_order IS NULL
    OR existing.work_order != excluded.work_order
)