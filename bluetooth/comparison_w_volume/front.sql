with missing_dates as (select distinct(datetime_bin) from 
(select intersection_uid, datetime_bin, leg, dir, sum(volume) from miovision.volumes_15min
where (intersection_uid = 6 and leg = 'W' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' 
and classification_uid in (1,4,5))
group by intersection_uid, datetime_bin, leg, dir
order by intersection_uid, datetime_bin) f


EXCEPT

select distinct(datetime_bin) from 
(select intersection_uid, datetime_bin, leg, dir, sum(volume) from miovision.volumes_15min
where (intersection_uid = 5 and leg = 'E' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' 
and classification_uid in (1,4,5))
group by intersection_uid, datetime_bin, leg, dir
order by intersection_uid, datetime_bin) f)






select intersection_uid, datetime_bin, leg, dir, sum(volume) from miovision.volumes_15min
where (intersection_uid = 5 and leg = 'E' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' and datetime_bin not in (select * from missing_dates)
and classification_uid in (1,4,5)) or 

(intersection_uid = 6 and leg = 'W' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' and datetime_bin not in (select * from missing_dates)
and classification_uid in (1,4,5))

group by intersection_uid, datetime_bin, leg, dir
order by intersection_uid, datetime_bin; 


