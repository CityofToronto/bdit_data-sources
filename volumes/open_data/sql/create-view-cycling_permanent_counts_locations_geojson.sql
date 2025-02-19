CREATE OR REPLACE VIEW open_data.cycling_permanent_counts_locations_geojson AS
WITH features AS (
    SELECT
        json_build_object(
            'type', 'Feature',
            'id', location_dir_id,
            'geometry', st_asgeojson(st_setsrid(st_makepoint(longitude, latitude), 4326))::jsonb,
            'properties', json_build_object(
                'location_dir_id', location_dir_id,
                'location_name', location_name,
                'direction', direction,
                'linear_name_full', linear_name_full,
                'side_street', side_street,
                'centreline_id', centreline_id,
                'bin_size', bin_size,
                'latest_calibration_study', latest_calibration_study,
                'first_active', first_active,
                'last_active', last_active,
                'date_decommissioned', date_decommissioned,
                'technology', technology
            )
        ) AS features
    FROM open_data.cycling_permanent_counts_locations
)

--aggregate features into FeatureCollection
SELECT
    json_build_object(
        'type',
        'FeatureCollection',
        'features',
        json_agg(features.features)
    ) AS featurecollection
FROM features;

ALTER VIEW open_data.cycling_permanent_counts_locations_geojson OWNER TO od_admins;

GRANT SELECT ON open_data.cycling_permanent_counts_locations_geojson TO ecocounter_bot;
GRANT SELECT ON open_data.cycling_permanent_counts_locations_geojson TO bdit_humans;
