echo $HOME;
psql -U airflow -h localhost traffic_signals -c "SELECT * FROM signals_cart LIMIT 10;"
