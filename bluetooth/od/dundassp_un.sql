with final as (with combos as (select startend_path.datetime_bin::date, startend_path.userid, s_du_sp, e_du_un, path_total from startend_path, others_path 
where s_du_sp = 1 and e_du_un = 1 and startend_path.userid = others_path.userid and startend_path.datetime_bin::date = others_path.datetime_bin::date
and path_total > 2 and path_total < 7)

select distinct * from combos

INNER JOIN (select user_id, measured_timestamp::date, startpoint_name, endpoint_name from bluetooth.observations) se ON se.user_id = combos.userid and se.measured_timestamp::date = combos.datetime_bin::date)

select datetime_bin, userid, s_du_sp, e_du_un, path_total, startpoint_name, endpoint_name
from final
order by datetime_bin, userid




