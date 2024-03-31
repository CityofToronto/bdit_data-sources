/*
Parameters:
Name | Type | Description
_mon | DATE | Month to clear

Return: Void
Purpose: Clears mobile summary for the given month
*/
CREATE OR REPLACE FUNCTION wys.clear_mobile_summary_for_month ( -- noqa: PRS
    _mon date
)
RETURNS void
LANGUAGE 'sql'

COST 100
VOLATILE SECURITY DEFINER
AS $BODY$

WITH active_mobile_signs AS (
    --identify all signs active during the month. 
    SELECT location_id, api_id, installation_date, removal_date
    FROM wys.mobile_api_id
    WHERE (
            --added during the month
            installation_date >= _mon::date
            AND installation_date < _mon::date + interval '1 month'
        ) OR (
            --or added before the month and active during
            installation_date < _mon::date
            AND (
                removal_date >= _mon::date
                OR removal_date IS NULL
            )
        )
)

--delete existing summaries for these signs
DELETE FROM wys.mobile_summary AS summ
USING active_mobile_signs AS active
WHERE
    --base on location_id and installation_date
    --since removal date may have changed (null->not null)
    active.location_id = summ.location_id
    AND active.installation_date = summ.installation_date;

$BODY$;

ALTER FUNCTION wys.clear_mobile_summary_for_month(date) OWNER TO wys_admins;

REVOKE EXECUTE ON FUNCTION wys.clear_mobile_summary_for_month(date) FROM public;
GRANT EXECUTE ON FUNCTION wys.clear_mobile_summary_for_month(date) TO wys_bot;