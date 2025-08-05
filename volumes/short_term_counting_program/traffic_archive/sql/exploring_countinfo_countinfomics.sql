 SELECT COUNT(DISTINCT c.count_info_id) AS "countinfo count",
 COUNT(DISTINCT cim.count_info_id) "countinfomics count",
 SUM(CASE WHEN c.count_info_id = cim.count_info_id THEN 1 ELSE 0 END) AS "Both"
 FROM traffic.countinfo c
 FULL OUTER JOIN traffic.countinfomics cim ON c.count_info_id = cim.count_info_id
 /*
 'countinfo count','countinfomics count','Both'
 762388,20455,4777
*/



SELECT COUNT(DISTINCT c.count_info_id) AS "countinfomics count",
COUNT(DISTINCT det.count_info_id) "det count",
SUM(CASE WHEN c.count_info_id = det.count_info_id THEN 1 ELSE 0 END) AS "Both"
FROM traffic.countinfomics c
FULL OUTER JOIN traffic.det det ON c.count_info_id = det.count_info_id
-- 
-- 'countinfomics count','det count','Both'
-- 20455,20454,661937

SELECT min(c.count_date) AS "Earliest countinfo date", max(c.count_date) AS "Last countinfo date", min(cim.count_date) AS "Earliest countinfomics date", max(cim.count_date) AS "Earliest countinfomics date"
FROM traffic.countinfo c
FULL OUTER JOIN traffic.countinfomics cim ON c.count_info_id = cim.count_info_id

SELECT COUNT(*)
FROM traffic.countinfo c
INNER JOIN traffic.countinfomics cim ON c.count_info_id = cim.count_info_id AND c.count_date = cim.count_date::DATE

SELECT extract('year' FROM COALESCE(c.count_date, cim.count_date)) AS "Year", COUNT(DISTINCT c.count_info_id) AS "countinfo count",
 COUNT(DISTINCT cim.count_info_id) "countinfomics count"
FROM traffic.countinfo c
FULL OUTER JOIN traffic.countinfomics cim ON c.count_date = cim.count_date::DATE
GROUP BY "Year"
/*
'Year'|'countinfo count'|'countinfomics count'
------|-----------------|--------------------
1984|0|565
1985|0|495
1986|0|585
1987|0|408
1988|0|143
1989|0|689
1990|0|376
1991|0|591
1992|0|408
1993|2627|488
1994|2162|452
1995|16524|600
1996|24063|566
1997|24117|493
1998|27771|491
1999|26263|635
2000|24676|693
2001|21618|855
2002|27620|834
2003|25450|959
2004|40776|773
2005|49834|808
2006|46889|830
2007|31926|842
2008|30212|694
2009|47374|807
2010|63300|816
2011|50494|666
2012|63751|631
2013|18041|488
2014|15370|581
2015|46071|636
2016|35459|557
*/

SELECT source1, COUNT(1)
FROM traffic.countinfo 
GROUP BY source1
ORDER BY count DESC

/*
'source1'|'count'
---------|-------
'RESCU'|298463
''|264233
'JAMAR'|92935
'PERMSTA'|61114
'TRANSCORE'|41555
'24HOUR'|3163
'SENSYS'|645
'TRANSUITE'|267
'MTO'|8
'MTSS'|5
*/

SELECT category_name, COUNT(*)
FROM traffic.countinfomics
NATURAL JOIN traffic.category
GROUP BY category_name
ORDER BY count DESC

/*
'category_name'|'count'
---------------|-------
'MANUAL'|20455
*/

SELECT category_name, COUNT(*)
FROM traffic.countinfomics cim
INNER JOIN traffic.countinfo c ON c.count_info_id = cim.count_info_id 
INNER JOIN traffic.category ON c.category_id = category.category_id
GROUP BY category_name
ORDER BY count DESC

/*
'category_name'|'count'
'24 HOUR'|3353
'PERM STN'|1148
'BICYCLE'|202
'RESCU'|74
*/