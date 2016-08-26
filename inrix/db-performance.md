

```sql
SELECT min(tx), max(tx)
FROM inrix.raw_data201207 
```
Time: 26:18
|parameter|condition|
|---------|---------|
|index|yes|
|partition|no|
|phase|2|

```sql
CREATE INDEX ON inrix.raw_data201301(tx) WHERE score = 30;
```
Time: 2:04hrs

## Local Desktop




### COPY 201606 data
1:17:36 

### CREATE INDEX 201606
2:16:72
```sql

CREATE INDEX ON inrix.raw_data201606(score);

CREATE INDEX ON inrix.raw_data201606(tmc);  

CREATE INDEX ON inrix.raw_data201606(tx) WHERE score = 30;  
```
### Aggregate 201606 w index
3:31:11
### Aggregate 201604
35:34

### Aggregate 201604ymd (no index)
4:30:14

### Aggregate mnt-dow 201604 (ymd)
1:36 

### Aggregate mnt-dow 201606 
1:42