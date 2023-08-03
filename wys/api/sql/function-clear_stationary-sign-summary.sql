/*
Parameters:
Name | Type | Description
_mon | DATE | Month to clear

Return: Void
Purpose: Clear stationary summary for the given month
*/
CREATE OR REPLACE FUNCTION wys.clear_stationary_summary_for_month ( -- noqa: PRS
    _mon DATE
)
RETURNS void
LANGUAGE 'sql'

COST 100
VOLATILE SECURITY DEFINER
AS $BODY$

DELETE FROM wys.stationary_summary
WHERE mon = _mon;
$BODY$;

REVOKE EXECUTE ON FUNCTION wys.clear_stationary_summary_for_month (DATE)FROM public;
GRANT EXECUTE ON FUNCTION wys.clear_stationary_summary_for_month (DATE) TO wys_bot;