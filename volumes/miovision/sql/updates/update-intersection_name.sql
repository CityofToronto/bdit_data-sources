UPDATE miovision_api.intersections
SET
    intersection_name = CASE
        WHEN api_name = 'Greenholm Circuit and Lawrence Avenue East' THEN 'Lawrence / Greenholm'
        WHEN api_name = 'Lawrence Avenue East and Fortune Gate' THEN 'Lawrence / Fortune Gate'
        WHEN api_name = 'Lawrence Avenue East and Galloway Road' THEN 'Lawrence / Galloway'
        WHEN api_name = 'Lawrence Avenue East and Mossbank Drive' THEN 'Lawrence / Mossbank'
        WHEN
            api_name = 'Lawrence Avenue East and Scarborough Golf Club Road'
            THEN 'Lawrence / Scarborough Golf Club'
        WHEN api_name = 'Orton Park Road and Lawrence Avenue East' THEN 'Lawrence / Orton Park'
        WHEN api_name = 'Overture Road and Lawrence Avenue East' THEN 'Lawrence / Overture'
        WHEN api_name = 'The Queensway and The East Mall' THEN 'The Queensway / The East Mall'
        WHEN api_name = 'The Queensway and The West Mall' THEN 'The Queensway / The West Mall'

    END
WHERE api_name IN (
    'Greenholm Circuit and Lawrence Avenue East',
    'Lawrence Avenue East and Fortune Gate',
    'Lawrence Avenue East and Galloway Road',
    'Lawrence Avenue East and Mossbank Drive',
    'Lawrence Avenue East and Scarborough Golf Club Road',
    'Orton Park Road and Lawrence Avenue East',
    'Overture Road and Lawrence Avenue East',
    'The Queensway and The East Mall',
    'The Queensway and The West Mall'

);
