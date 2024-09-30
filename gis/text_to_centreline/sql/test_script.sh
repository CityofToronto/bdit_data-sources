#a helper script to implement gis.text_to_centreline in a new schema
#and then revert back to gis for committing.

#only adjust the schema of these functions
functions=("abbr_street" "custom_case" "text_to_centreline" "text_to_centreline_geom" "_get_intersection_id" "_get_intersection_geom" "_get_lines_btwn_interxn" "_centreline_case1" "_centreline_case2" "_translate_intersection_point" "_get_entire_length" "cleaned_bylaws_text" "bylaws_route_id" "bylaws_get_id_to_route" "_get_intersection_id_highway_equals_btwn", "_clean_bylaws_text")
#destination schema
test_schema="gwolofs"

for func in ${functions[@]}; do
  echo 's/gis.'"$func"'/'"$test_schema"'.'"$func"'/g'
done

#change all the `gis` and `gis_admins` references to `test_schema`
cd ~/bdit_data-sources/gis/text_to_centreline/sql
ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/gis_admins/'"$test_schema"'/g'
for func in ${functions[@]}; do
  ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/gis.'"$func"'/'"$test_schema"'.'"$func"'/g'
done

#again in sub directory
cd ~/bdit_data-sources/gis/text_to_centreline/sql/helper_functions
ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/gis_admins/'"$test_schema"'/g'
for func in ${functions[@]}; do
  ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/gis.'"$func"'/'"$test_schema"'.'"$func"'/g'
done

#recreate the entire gis.text_to_centreline stack in new schema
#scripts ordered based on dependencies
host_name={BIGDATA_HOST_NAME}
cd ~/bdit_data-sources/gis/text_to_centreline/sql
psql gwolofs -d bigdata -h $host_name -f helper_functions/function-custom_case.sql
psql gwolofs -d bigdata -h $host_name -f helper_functions/function-abbr_street.sql
psql gwolofs -d bigdata -h $host_name -f helper_functions/function-bylaws_get_id_to_route.sql
psql gwolofs -d bigdata -h $host_name -f helper_functions/function-bylaws_route_id.sql
psql gwolofs -d bigdata -h $host_name -f helper_functions/function-get_intersection_id.sql
psql gwolofs -d bigdata -h $host_name -f helper_functions/function-get_intersection_id_highway_equals_btwn.sql
psql gwolofs -d bigdata -h $host_name -f helper_functions/function-translate_intersection_point.sql
psql gwolofs -d bigdata -h $host_name -f function-centreline_case1.sql
psql gwolofs -d bigdata -h $host_name -f function-centreline_case2.sql
psql gwolofs -d bigdata -h $host_name -f function-clean_bylaws_text.sql
psql gwolofs -d bigdata -h $host_name -f function-get_entire_length.sql
psql gwolofs -d bigdata -h $host_name -f function-get_intersection_geom.sql
psql gwolofs -d bigdata -h $host_name -f function-get_lines_btwn_interxn.sql
psql gwolofs -d bigdata -h $host_name -f function-text_to_centreline.sql
psql gwolofs -d bigdata -h $host_name -f function-text_to_centreline_geom.sql

#revert back to `gis` schema:
#here we need to alter the functions before owner statements because `gwolofs` schema and user overlap.
cd ~/bdit_data-sources/gis/text_to_centreline/sql
for func in ${functions[@]}; do
  ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/'"$test_schema"'.'"$func"'/gis.'"$func"'/g'
done
ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/'"$test_schema"'/gis_admins/g'

cd ~/bdit_data-sources/gis/text_to_centreline/sql/helper_functions
for func in ${functions[@]}; do
  ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/'"$test_schema"'.'"$func"'/gis.'"$func"'/g'
done
ls -1 | grep .sql | xargs -d '\n' sed -i -e 's/'"$test_schema"'/gis_admins/g'
