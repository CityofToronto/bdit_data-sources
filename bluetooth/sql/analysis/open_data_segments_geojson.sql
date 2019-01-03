--Select "Download as CSV" on this query.

select row_to_json(fc)
from (
    select
        'FeatureCollection' as "type",
        array_to_json(array_agg(f)) as "features"
    from (
        select
            'Feature' as "type",
            ST_AsGeoJSON(ST_Transform(geom, 4326), 6) :: json as "geometry",
            (
                select json_strip_nulls(row_to_json(t))
                from (
                    select
                        "resultId", start_street, direction, start_cross_street, end_street, end_cross_street, start_date, end_date, length
                ) t
            ) as "properties"
        from open_data.bluetooth_segments
    ) as f
) as fc;