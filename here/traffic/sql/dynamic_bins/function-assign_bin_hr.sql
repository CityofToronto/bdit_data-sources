CREATE OR REPLACE FUNCTION gwolofs.assign_bin_hour(bin_range tsrange)
RETURNS timestamp
LANGUAGE plpgsql
IMMUTABLE
AS $$
    DECLARE
        lower_hour timestamp;
        upper_hour timestamp;
        
    BEGIN
    --calculate hour boundaries
    lower_hour := date_trunc('hour', lower(bin_range));
    upper_hour := date_trunc('hour', upper(bin_range));
    
    --early return if same hour
    IF lower_hour = upper_hour THEN
        RETURN lower_hour;
    --if the intersection between the hours is equal or in favour of lower.
    ELSIF
        (least(upper(bin_range), upper_hour) - lower(bin_range)) >= 
        upper(bin_range) - least(upper_hour, upper(bin_range)) THEN
        RETURN lower_hour;
    ELSE
        RETURN upper_hour;
    END IF;
    END;
$$;

COMMENT ON FUNCTION gwolofs.assign_bin_hour
IS 'Assign hour to a tsrange based on how much overlap there is with start/end hour.';
