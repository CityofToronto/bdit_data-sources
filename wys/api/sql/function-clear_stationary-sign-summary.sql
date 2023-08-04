/*
Parameters:
Name | Type | Description
_mon | DATE | Month to clear

Return: Void
Purpose: Clears stationary WYS summary for the given month
*/
CREATE OR REPLACE FUNCTION wys.clear_stationary_summary_for_month ( -- noqa: PRS
    _mon date
)
RETURNS void
LANGUAGE 'sql'

COST 100
VOLATILE SECURITY DEFINER
AS $BODY$

DELETE FROM wys.stationary_summary
WHERE mon = _mon;

$BODY$;

ALTER FUNCTION wys.clear_stationary_summary_for_month(date) OWNER TO wys_admins;

REVOKE EXECUTE ON FUNCTION wys.clear_stationary_summary_for_month (date) FROM public;
GRANT EXECUTE ON FUNCTION wys.clear_stationary_summary_for_month (date) TO wys_bot;