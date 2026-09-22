### Loop through start_dates and intersections

start_date=2026-08-20

for intersection_uid in 166 167 176 178 168 177
do
    echo "Running aggregation with start_date=$start_date and intersection=$intersection_uid"
    ####python3 intersection_tmc.py run-api-cli --pull --agg --start_date=$start_date --intersection=$intersection_uid
done

start_date=2026-08-21

for intersection_uid in 174 181
do
    echo "Running aggregation with start_date=$start_date and intersection=$intersection_uid"
    ####python3 intersection_tmc.py run-api-cli --pull --agg --start_date=$start_date --intersection=$intersection_uid
done


