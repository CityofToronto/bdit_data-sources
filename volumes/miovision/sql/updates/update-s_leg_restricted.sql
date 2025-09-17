UPDATE miovision_api.intersections
SET s_leg_restricted= CASE
	WHEN api_name = 'Lawrence Avenue East and Fortune Gate' THEN true
	WHEN api_name = 'Lawrence Avenue East and Mossbank Drive' THEN true
	WHEN api_name = 'Orton Park Road and Lawrence Avenue East' THEN true

END
WHERE api_name IN (
	'Lawrence Avenue East and Fortune Gate',
	'Lawrence Avenue East and Mossbank Drive',
	'Orton Park Road and Lawrence Avenue East'	
);
