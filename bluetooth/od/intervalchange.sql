WITH final as(
SELECT complete.datetime_bin, complete.userid, complete.start as origin, complete.end as destination, complete.path_total, startpoint_name as segment_start, endpoint_name as segment_end
FROM segs
LEFT JOIN alouis2.complete
ON date_trunc('minute', segs.datetime_bin) > date_trunc('minute', complete.datetime_bin) - INTERVAL '45 minute'
AND segs.datetime_bin <= complete.datetime_bin
AND segs.userid = complete.userid
WHERE path_total > 1)

SELECT datetime_bin, userid, count(distinct(final.*)) as segment_total, origin, destination, path_total
FROM final
GROUP BY datetime_bin, userid, origin, destination, path_total
--having count(distinct(final.*)) = path_total -1;