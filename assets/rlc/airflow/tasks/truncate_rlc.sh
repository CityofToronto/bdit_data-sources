psql $vz_pg_uri -v "ON_ERROR_STOP=1" -c "TRUNCATE vz_safety_programs_staging.rlc;"
