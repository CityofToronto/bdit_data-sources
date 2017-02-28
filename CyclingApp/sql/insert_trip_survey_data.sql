/*Upload the trip survey data
Since on RDS there's no superuser privilege, can't perform the COPY [FROM|TO] command on a file, have to use psql \copy command instead.
*/


/*First create a temporary table to store the raw survey data*/
CREATE TABLE cycling.trip_surveys_temp(
  trip_id bigint NOT NULL,
  app_user_id integer NOT NULL,
  started_at timestamp without time zone NOT NULL,
  purpose TEXT NOT NULL,
  notes text
);


--Execute in psql
\COPY trip_surveys FROM 'H:/Data/CyclingApp/2017trip_surveys.csv' WITH (FORMAT 'csv', HEADER TRUE);

WITH ins AS (
	INSERT INTO cycling.trip_purpose (purpose)
	SELECT DISTINCT purpose
	FROM cycling.trip_surveys_temp
	ON CONFLICT DO NOTHING
)
INSERT INTO cycling.trip_surveys
SELECT trip_id, app_user_id, started_at, purpose_id, notes
  FROM cycling.trip_surveys_temp 
  INNER JOIN cycling.trip_purpose USING (purpose)
 ON CONFLICT DO NOTHING;

TRUNCATE cycling.trip_surveys_temp;

DROP TABLE IF EXISTS cycling.trip_surveys_temp;