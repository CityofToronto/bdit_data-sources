UPDATE miovision_api.intersections
SET street_main= CASE
    WHEN api_name = 'Cherry Street and Lake Shore Boulevard East' THEN 'Lake Shore Boulevard East'
    WHEN api_name = 'Eglinton Avenue East and Yonge Street' THEN 'Eglinton Avenue East'
    WHEN api_name = 'Eglinton Avenue West and Avenue Road' THEN 'Eglinton Avenue West'
    WHEN api_name = 'Eglinton Avenue West and Bathurst Street' THEN 'Eglinton Avenue West'
    WHEN api_name = 'Eglinton Avenue West and Keele Street' THEN 'Eglinton Avenue West'
    WHEN api_name = 'Fleet Street and Fort York Boulevard' THEN 'Fleet Street'
    WHEN api_name = 'Fleet Street and Strachan Avenue' THEN 'Fleet Street'
    WHEN api_name = 'The Queensway and The East Mall' THEN 'The Queensway'
    WHEN api_name = 'The Queensway and The West Mall' THEN 'The Queensway'
	WHEN api_name = 'The Queensway and 427 Ramp / Sherway Gardens Road' THEN 'The Queensway'
	WHEN api_name = 'The Queensway and North Queen Street / Private Access' THEN 'The Queensway'
	WHEN api_name = 'The Queensway at 60m East of Algie Ave' THEN 'The Queensway'
	WHEN api_name = 'The Queensway at Atomic Avenue and Private Access' THEN 'The Queensway'
	WHEN api_name = 'Yonge Street and Bloor Street' THEN 'Yonge Street'
	WHEN api_name = 'Yonge Street and Dundas Street' THEN 'Yonge Street'
	WHEN api_name = 'Yonge Street and MacPherson Avenue' THEN 'Yonge Street'
	WHEN api_name = 'Greenholm Circuit and Lawrence Avenue East' THEN 'Lawrence Avenue East'
	WHEN api_name = 'Lawrence Avenue East and Fortune Gate' THEN 'Lawrence Avenue East'
	WHEN api_name = 'Lawrence Avenue East and Galloway Road' THEN 'Lawrence Avenue East'
	WHEN api_name = 'Lawrence Avenue East and Mossbank Drive' THEN 'Lawrence Avenue East'
	WHEN api_name = 'Lawrence Avenue East and Scarborough Golf Club Road' THEN 'Lawrence Avenue East'
	WHEN api_name = 'Orton Park Road and Lawrence Avenue East' THEN 'Lawrence Avenue East'
	WHEN api_name = 'Overture Road and Lawrence Avenue East' THEN 'Lawrence Avenue East'
	WHEN api_name = 'The Queensway and 230 M East of The East Mall' THEN 'The Queensway'
    WHEN api_name = 'The Queensway and 240m East of The West Mall' THEN 'The Queensway'
	
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
