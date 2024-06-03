WITH missing AS (
    SELECT
        vdsconfig_uid,
        detector_id
    FROM vds.detector_inventory
    WHERE expected_bins IS NULL
    ORDER BY detector_id
)

SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
    || ' vds detector with missing `expected_bins`. Please update `vds.detector_inventory` query.'
    AS summ,
    array_agg(
        'vdsconfig_uid: `' || vdsconfig_uid
        || '`, detector_id: `' || detector_id || '`'
    ) AS gaps
FROM missing