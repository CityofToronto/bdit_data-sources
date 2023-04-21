--Select "Download as CSV" on this query.

SELECT row_to_json(fc)
FROM (
    SELECT
        'FeatureCollection' AS "type",
        array_to_json(array_agg(f)) AS "features"
    FROM (
        SELECT
            'Feature' AS "type",
            ST_AsGeoJSON(ST_Transform(geom, 4326), 6)::json AS "geometry",
            (
                SELECT json_strip_nulls(row_to_json(t))
                FROM (
                    SELECT
                        "resultId",
                        start_street,
                        direction,
                        start_cross_street,
                        end_street,
                        end_cross_street,
                        start_date,
                        end_date,
                        length
                ) AS t
            ) AS "properties"
        FROM open_data.bluetooth_segments
    ) AS f
) AS fc;