select distinct(datetime_bin) from 
(select intersection_uid, datetime_bin, leg, dir, sum(volume) from miovision.volumes_15min
where (intersection_uid = 18 and leg = 'W' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' 
and classification_uid in (1,4,5))
group by intersection_uid, datetime_bin, leg, dir
order by intersection_uid, datetime_bin) f


EXCEPT

select distinct(datetime_bin) from 
(select intersection_uid, datetime_bin, leg, dir, sum(volume) from miovision.volumes_15min
where (intersection_uid = 15 and leg = 'E' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' 
and classification_uid in (1,4,5))
group by intersection_uid, datetime_bin, leg, dir
order by intersection_uid, datetime_bin) f2;

