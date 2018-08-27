SELECT * FROM miovision.volumes
WHERE (intersection_uid IN (2,3,4)
AND leg='E' 
AND classification_uid IN (1,3,4,5))

OR (intersection_uid IN (27,28,29,31,1,5)
AND leg='W' 
AND classification_uid IN (1,3,4,5))

OR (intersection_uid IN (14)
AND leg='S' 
AND classification_uid IN (1,3,4,5))

OR (intersection_uid IN (16)
AND leg='N' 
AND classification_uid IN (1,3,4,5))

OR (intersection_uid IN (26)
AND classification_uid IN (1,3,4,5)
AND ((leg IN ('E','W') 
AND movement_uid IN (1,4))
OR (leg IN ('N','S') 
AND movement_uid IN (2,3))))

OR (intersection_uid IN (30)
AND leg IN ('W') 
AND classification_uid IN (1,3,4,5)
AND movement_uid IN (1,4))

OR (intersection_uid IN (27,28,29,31,30)
AND classification_uid IN (1,3,4,5)
AND ((leg IN ('S') 
AND movement_uid IN (3)) OR (leg IN ('N') 
AND movement_uid IN (2))))

OR (intersection_uid IN (2,3,4)
AND classification_uid IN (1,3,4,5)
AND ((leg IN ('S') 
AND movement_uid IN (2)) OR (leg IN ('N') 
AND movement_uid IN (3))))

OR (intersection_uid IN (14)
AND classification_uid IN (1,3,4,5)
AND ((leg IN ('E') 
AND movement_uid IN (3)) OR (leg IN ('W') 
AND movement_uid IN (2))))

OR (intersection_uid IN (16)
AND classification_uid IN (1,3,4,5)
AND ((leg IN ('E') 
AND movement_uid IN (2)) OR (leg IN ('W') 
AND movement_uid IN (3))))

ORDER BY intersection_uid, datetime_bin
