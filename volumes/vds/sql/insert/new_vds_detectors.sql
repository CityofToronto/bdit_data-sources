SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
    || ' new VDS detectors:'
    AS summ,
    array_agg(
        'row_id: `' || n.row_id
        || '`, detector_id: `' || n.detector_id
        || '`, detector_loc: `' || n.detector_loc || '`'
    ) AS new_detectors
--insert with new row return
FROM vds.insert_detector_inventory_rows() AS n
--anti join existing sensors with the same detector_id
--(only want to show new sensors, not updates)
LEFT JOIN vds.detector_inventory AS di
    ON n.detector_id = di.detector_id
    AND n.row_id != di.row_id
WHERE
    n.division_id = 2
    AND di.row_id IS NULL --anti join
