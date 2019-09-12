# psql -U airflow -h localhost traffic_signals -c "SELECT * FROM signals_cart LIMIT 10;"
psql -U airflow -h localhost traffic_signals <  ~/PROJECTS/bdit_data-sources/assets/traffic_signals/airflow/tasks/psql_command.sql

# pg_dump -U candu -h 10.160.12.47 -p 5432 -t "prj_volume.artery_tcl" -x --no-owner --clean --if-exists bigdata | psql -v ON_ERROR_STOP=1 -U flashcrow -h fr194ibxx9jxbj3.ccca5v4b7zsj.us-east-1.rds.amazonaws.com -p 5432 flashcrow


# pg_dump -U candu -h 10.160.12.47 -p 5432 -t "prj_volume.artery_tcl" -x --no-owner --clean --if-exists bigdata | psql -v ON_ERROR_STOP=1 -U flashcrow -h fr194ibxx9jxbj3.ccca5v4b7zsj.us-east-1.rds.amazonaws.com -p 5432 flashcrow
