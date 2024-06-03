WITH missing AS (
    SELECT
        vdsconfig_uid,
        detector_id
    FROM vds.detector_inventory
    WHERE
        centreline_id IS NULL
        AND division_id = 2
    ORDER BY vdsconfig_uid
)

SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
    || ' vds detector with missing centreline_id. Please update in `vds.centreline_vds`.'
    AS summ,
    array_agg(
        'vdsconfig_uid: `' || vdsconfig_uid
        || '`, detector_id: `' || detector_id || '`'
    ) AS gaps
FROM missing