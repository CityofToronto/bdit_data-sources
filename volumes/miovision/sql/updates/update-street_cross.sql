UPDATE miovision_api.intersections
SET street_cross= CASE
    WHEN api_name = 'Cherry Street and Lake Shore Boulevard East' THEN 'Cherry Street'
    WHEN api_name = 'Eglinton Avenue East and Yonge Street' THEN 'Yonge Street'
    WHEN api_name = 'Eglinton Avenue West and Avenue Road' THEN 'Avenue Road'
    WHEN api_name = 'Eglinton Avenue West and Bathurst Street' THEN 'Bathurst Street'
    WHEN api_name = 'Eglinton Avenue West and Keele Street' THEN 'Keele Street'
    WHEN api_name = 'Fleet Street and Fort York Boulevard' THEN 'Fort York Boulevard'
    WHEN api_name = 'Fleet Street and Strachan Avenue' THEN 'Strachan Avenue'
    WHEN api_name = 'The Queensway and The East Mall' THEN 'The East Mall'
    WHEN api_name = 'The Queensway and The West Mall' THEN 'The West Mall'
	WHEN api_name = 'The Queensway and 427 Ramp / Sherway Gardens Road' THEN '427 Ramp / Sherway Gardens Road'
	WHEN api_name = 'The Queensway and North Queen Street / Private Access' THEN 'North Queen Street / Private Access'
	WHEN api_name = 'The Queensway at 60m East of Algie Ave' THEN '60m East of Algie Ave'
	WHEN api_name = 'The Queensway at Atomic Avenue and Private Access' THEN 'Atomic Avenue and Private Access'
	WHEN api_name = 'Yonge Street and Bloor Street' THEN 'Bloor Street'
	WHEN api_name = 'Yonge Street and Dundas Street' THEN 'Dundas Street'
	WHEN api_name = 'Yonge Street and MacPherson Avenue' THEN 'MacPherson Avenue'
	WHEN api_name = 'Greenholm Circuit and Lawrence Avenue East' THEN 'Greenholm Circuit'
	WHEN api_name = 'Lawrence Avenue East and Fortune Gate' THEN 'Fortune Gate'
	WHEN api_name = 'Lawrence Avenue East and Galloway Road' THEN 'Galloway Road'
	WHEN api_name = 'Lawrence Avenue East and Mossbank Drive' THEN 'Mossbank Drive'
	WHEN api_name = 'Lawrence Avenue East and Scarborough Golf Club Road' THEN 'Scarborough Golf Club Road'
	WHEN api_name = 'Orton Park Road and Lawrence Avenue East' THEN 'Orton Park Road'
	WHEN api_name = 'Overture Road and Lawrence Avenue East' THEN 'Overture Road'
	WHEN api_name = 'The Queensway and 230 M East of The East Mall' THEN '230 M East of The East Mall'
    WHEN api_name = 'The Queensway and 240m East of The West Mall' THEN '240m East of The West Mall'
	
END
WHERE api_name IN (
    'Cherry Street and Lake Shore Boulevard East',
    'Eglinton Avenue East and Yonge Street',
    'Eglinton Avenue West and Avenue Road',
    'Eglinton Avenue West and Bathurst Street',
    'Eglinton Avenue West and Keele Street',
    'Fleet Street and Fort York Boulevard',
    'Fleet Street and Strachan Avenue',
    'The Queensway and The East Mall',
    'The Queensway and The West Mall',
	'The Queensway and 427 Ramp / Sherway Gardens Road',
	'The Queensway and North Queen Street / Private Access',
	'The Queensway at 60m East of Algie Ave',
	'The Queensway at Atomic Avenue and Private Access',
	'Yonge Street and Bloor Street',
	'Yonge Street and Dundas Street',
	'Greenholm Circuit and Lawrence Avenue East',
	'Lawrence Avenue East and Fortune Gate',
	'Lawrence Avenue East and Galloway Road',
	'Lawrence Avenue East and Mossbank Drive',
	'Lawrence Avenue East and Scarborough Golf Club Road',
	'Orton Park Road and Lawrence Avenue East',
	'Overture Road and Lawrence Avenue East',
	'The Queensway and 230 M East of The East Mall',
	'The Queensway and 240m East of The West Mall',
	'Yonge Street and MacPherson Avenue'
	
);
