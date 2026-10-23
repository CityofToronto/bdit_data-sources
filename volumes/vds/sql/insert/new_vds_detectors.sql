SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
    || ' new VDS detectors:'
    AS summ,
    array_agg(
        'row_id: `' || row_id
        || '`, detector_id: `' || detector_id || '`'
        || '`, detector_loc: `' || detector_loc || '`'
    ) AS gaps
--insert with new row return
FROM vds.insert_detector_inventory_rows();