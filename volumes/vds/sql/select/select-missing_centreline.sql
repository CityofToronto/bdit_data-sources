WITH missing AS (
    SELECT
        di.vdsconfig_uid,
        di.detector_id
    FROM vds.detector_inventory AS di
    LEFT JOIN vds.centreline_vds AS c USING (vdsconfig_uid)
    WHERE
        di.division_id = 2
        AND c.vdsconfig_uid IS NULL --missing, but allowing for manual null centrelines
    ORDER BY vdsconfig_uid
)

SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
    || ' vds detector with missing centreline_id which could not be matched automatically. Please update in `vds.centreline_vds`.'
    AS summ,
    array_agg(
        'vdsconfig_uid: `' || vdsconfig_uid
        || '`, detector_id: `' || detector_id || '`'
    ) AS gaps
FROM missing;
