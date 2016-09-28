SELECT COALESCE(roadname, roadnumber) AS "Roadname", direction, COUNT(1) AS "Number of TMCs", SUM(miles) *1.60934 AS "Total Length", SUM(miles)/COUNT(1) "Average Segment Length",avg(speed) *1.60934 AS "Average Speed"
  FROM gis.inrix_tmc_tor
  GROUP BY COALESCE(roadname, roadnumber), direction
  ORDER BY "Number of TMCs" DESC
