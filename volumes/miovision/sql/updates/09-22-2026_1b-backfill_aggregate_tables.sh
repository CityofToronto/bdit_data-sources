### Loop through start_dates and intersections

end_date=$(date +%Y-%m-%d)
start_date=2026-08-20

echo "Running aggregation with start_date=$start_date , end_date=$end_date and intersections 166 167 176 178 168 177"
####python3 intersection_tmc.py run-api-cli --pull --agg --start_date=$start_date --end_date=$end_date \
#--intersection=166 --intersection=167 --intersection=176 --intersection=178 --intersection=168 --intersection=177


start_date=2026-08-21

echo "Running aggregation with start_date=$start_date , end_date=$end_date and intersections 174 181"
    #python3 intersection_tmc.py run-api-cli --pull --agg --start_date=$start_date --end_date=$end_date \
    #--intersection=174 --intersection=181
done


