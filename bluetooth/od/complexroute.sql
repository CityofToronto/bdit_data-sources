with route as (with ones as (select startend_path.datetime_bin, startend_path.userid, s_du_sp, e_du_ja from alouis2.startend_path
where s_du_sp = 1 and e_du_ja = 1)

select ones.datetime_bin, ones.userid, du_sp, du_ja, qu_sp, qu_ja, rm_sp, rm_ja, ad_sp, ad_ja, kn_sp, kn_yo, kn_ja, path_total from ones

INNER JOIN others_path on ones.userid = others_path.userid and ones.datetime_bin = others_path.datetime_bin

where path_total = 11)

select datetime_bin, userid, du_sp, du_ja, qu_sp, qu_ja, rm_sp, rm_ja, ad_sp, ad_ja, kn_sp, kn_yo, kn_ja, path_total, startpoint_name, endpoint_name from route, bluetooth.observations

where observations.user_id = route.userid and observations.measured_timestamp = route.datetime_bin;