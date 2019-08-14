# Storing historical centreline versions

Since the toronto centreline layer is ever changing and there are a bunch of datasets mapped to the centreline layer, we need to keep track of what changes are made to the layer over time and update our datesets with new attributes accordingly.

The `centreline_wh` layer avaliable in GCCVIEW stores all historical centreline versions and record them in these four attributes:

|Attribute name|Description|
|-----------------|-------------------------------------------------------------------------|
| date_effective  | Effective date from   trans_id_create                                 |  
| date_expiry     | Expiry date from trans_id_expiry                                     | 
| trans_id_create | Transaction created the   centreline segment                            |
| trans_id_expire | Transaction expired the centreline segment; -1 for current   centreline |  


Relationship between old and new versions of centreline:

![image](https://user-images.githubusercontent.com/46324452/55912374-8a0d2600-5bb0-11e9-8262-570460a9c326.png)

Now we know which features replaced which features by looking at the relationships of transaction id.

How do we link it and what type of reference table should we create:

We can do a recursive join on `centreline_wh` on `trans_id_create` = `trans_id_expiry`

Example sql:
```sql
with recursive wh(centreline, trans_id_create, trans_id_expire, date_effective, date_expiry, geom, loop_nm) as 
(
select centreline, trans_id_create, trans_id_expire, date_effective, date_expiry, aa.geom, 1 as loop_nm from centreline_wh cc
inner join (SELECT geo_id, b.geom
from gis.centreline b
left join (
	select centreline, geom
	from centreline_wh
	where trans_id_expire = -1 and state = 0) aa on geo_id = aa.centreline
where aa.centreline is null limit 10) aa on centreline = aa.geo_id and cc.geom = aa.geom 
	
union all
select n.centreline, n.trans_id_create, n.trans_id_expire, n.date_effective, n.date_expiry, n.geom, loop_nm + 1 as loop_nm
from centreline_wh n, wh w
where n.trans_id_create = w.trans_id_expire)
select distinct centreline, trans_id_create, trans_id_expire, date_effective, date_expiry, geom, loop_nm from wh
```
From this relationship, we can understand what happen during each transaction. 
We can also know which centreline got deleted:
```sql
???
```
and which centreline was created:
```sql
select aa.centreline, aa.trans_id_create, aa.trans_id_expire, bb.trans_id_create from centreline_wh aa 						 
left join centreline_wh bb on aa.trans_id_create = bb.trans_id_expire 
where aa.trans_id_expire = -1  and bb.trans_id_expire is null	
```



