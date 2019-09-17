psql -v ON_ERROR_STOP=1 -U vzairflow -h 10.160.12.47 -p 5432 bigdata -c "TRUNCATE vz_safety_programs_staging.rlc_test;"
