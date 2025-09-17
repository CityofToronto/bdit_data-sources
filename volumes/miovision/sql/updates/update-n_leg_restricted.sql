UPDATE miovision_api.intersections
SET n_leg_restricted= CASE
	WHEN api_name = 'Overture Road and Lawrence Avenue East' THEN true

END
WHERE api_name IN (
'Overture Road and Lawrence Avenue East'
);
