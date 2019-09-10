# psql -U airflow -h localhost traffic_signals -c "SELECT * FROM signals_cart LIMIT 10;"
psql -U airflow -h localhost traffic_signals <  ~/PROJECTS/bdit_data-sources/assets/traffic_signals/airflow/tasks/psql_command.sql
