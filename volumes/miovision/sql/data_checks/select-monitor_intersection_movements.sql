SELECT
    COUNT(*) > 0 AS "_check",
    'There are ' || COUNT(*) || ' rows in `monitor_intersection_movements`. '
    || 'Please review and add to either `miovision_api.intersection_movements` '
    || 'or `miovision_api.intersection_movements_denylist`.' AS msg
FROM miovision_api.monitor_intersection_movements;