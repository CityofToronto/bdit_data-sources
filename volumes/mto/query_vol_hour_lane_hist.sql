SELECT width_bucket(volume/num_lanes*2, 0, 3600, 35) AS buckets, COUNT(*)
FROM mto.sensors JOIN mto.mto_agg_30 USING (detector_id)
WHERE num_lanes IS NOT NULL AND volume !=0 AND within_cot=TRUE
GROUP BY buckets
ORDER BY buckets