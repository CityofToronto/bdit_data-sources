CREATE OR REPLACE FUNCTION gis_core.refresh_centreline_latest_all_feature()
RETURNS VOID AS $$

    TRUNCATE gis_core.centreline_latest_all_feature;

    INSERT INTO gis_core.centreline_latest_all_feature (
        version_date, centreline_id, linear_name_id, linear_name_full, linear_name_full_legal, address_l, address_r, parity_l, parity_r, lo_num_l, hi_num_l, lo_num_r, hi_num_r, begin_addr_point_id_l, end_addr_point_id_l, begin_addr_point_id_r, end_addr_point_id_r, begin_addr_l, end_addr_l, begin_addr_r, end_addr_r, linear_name, linear_name_type, linear_name_dir, linear_name_desc, linear_name_label, from_intersection_id, to_intersection_id, oneway_dir_code, oneway_dir_code_desc, feature_code, feature_code_desc, jurisdiction, centreline_status, shape_length, objectid, shape_len, mi_prinx, low_num_odd, high_num_odd, low_num_even, high_num_even, geom
    )
    SELECT version_date, centreline_id, linear_name_id, linear_name_full, linear_name_full_legal, address_l, address_r, parity_l, parity_r, lo_num_l, hi_num_l, lo_num_r, hi_num_r, begin_addr_point_id_l, end_addr_point_id_l, begin_addr_point_id_r, end_addr_point_id_r, begin_addr_l, end_addr_l, begin_addr_r, end_addr_r, linear_name, linear_name_type, linear_name_dir, linear_name_desc, linear_name_label, from_intersection_id, to_intersection_id, oneway_dir_code, oneway_dir_code_desc, feature_code, feature_code_desc, jurisdiction, centreline_status, shape_length, objectid, shape_len, mi_prinx, low_num_odd, high_num_odd, low_num_even, high_num_even, geom
    FROM gis_core.centreline
    WHERE
        version_date = (
            SELECT MAX(centreline.version_date)
            FROM gis_core.centreline
        );

    COMMENT ON TABLE gis_core.centreline_latest_all_feature
    IS 'Table containing the latest version of centreline with all feature code, derived from gis_core.centreline.'
    || ' Last updated: ' || CURRENT_DATE;
$$ LANGUAGE sql;

ALTER FUNCTION gis_core.refresh_centreline_latest_all_feature OWNER TO gis_admins;

GRANT EXECUTE ON gis_core.refresh_centreline_latest_all_feature TO gcc_bot;

REVOKE ALL ON FUNCTION gis_core.refresh_centreline_latest_all_feature() FROM public;

COMMENT ON FUNCTION gis_core.refresh_centreline_latest_all_feature()
IS 'Function to refresh gis_core.centreline_latest_all_feature with truncate/insert.';
