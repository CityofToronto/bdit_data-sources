UPDATE gtfs.calendar_imp SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
UPDATE gtfs.calendar_dates_imp SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
UPDATE gtfs.routes SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
UPDATE gtfs.shapes SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
UPDATE gtfs.shapes_geom SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
UPDATE gtfs.stop_times SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
UPDATE gtfs.stops SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
UPDATE gtfs.trips SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;