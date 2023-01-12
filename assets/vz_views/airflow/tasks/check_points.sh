psql $vz_pg_uri -v "ON_ERROR_STOP=1" -c "SELECT COUNT(*) FROM vz_safety_programs.safetymeasures_points_new;"
