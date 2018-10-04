
# Traffic Volumes - Turning Movement Counts (TMCs)
# Identifying Extreme Values 


```python
from psycopg2 import connect
import configparser
%matplotlib inline
import numpy as np
import pandas as pd
import pandas.io.sql as pandasql
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import matplotlib.ticker as ticker

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

from IPython.display import HTML

def print_table(sql, con):
    return HTML(pandasql.read_sql(sql, con).to_html(index=False))
```

## Checking for Extreme Values 

Create a histogram of volume numbers to determine which values are "extreme" 


```python
sql_volumes = '''
SELECT volume_15min
FROM open_data.volumes_tmc_bikes
'''
volumes = pandasql.read_sql(sql_volumes, con)


bin_val = np.arange(min(volumes['volume_15min']), max(volumes['volume_15min']), 1)

volumes.hist(bins=bin_val, figsize=(15,10))
plt.title('Volume counts for 15 minute bins')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)

```


![png](output_3_0.png)


There are some values greater than 20, but it is hard to see how many of these larger volumes exist. Create a new histogram to zoom into the volumes that are greater than 20. 


```python
volumes.hist(bins = np.arange(20,  max(volumes['volume_15min']), 1), figsize=(18,10))
plt.title('Volume counts for 15 minute bins that have a volume greater than 20')
ax = plt.gca()
f = lambda x,pos: str(x).rstrip('0').rstrip('.')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)
```


![png](output_5_0.png)


Looking at the above histogram, it is still tough to see the number of records with a volume above 70. 


```python
volumes.hist(bins = np.arange(70, max(volumes['volume_15min']) , 1), figsize=(18,10))
plt.title('Volume counts for 15 minute bins that have a volume greater than 70')
ax = plt.gca()
f = lambda x,pos: str(x).rstrip('0').rstrip('.')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)
```


![png](output_7_0.png)


The trend is that the volumes are gradually decreasing. Even the highest volume values in the dataset do not seem to be incorrect or out of place because of the trend 

# Removing outliers from dataset


```python
sql_volumes = '''
SELECT px, location, class_type, leg, movement, datetime_bin, volume_15min 
FROM open_data.volumes_tmc_bikes
'''

sql_locations = '''
SELECT DISTINCT ON (px, leg, movement) px, leg, movement
FROM open_data.volumes_tmc_bikes
'''

locations = pandasql.read_sql(sql_locations, con)
```


```python
volumes = pandasql.read_sql(sql_volumes, con)
```


```python
final_df = pd.DataFrame(columns=[ 'px', 'location', 'class_type', 'leg', 'movement', 'datetime_bin', 'volume_15min' ])
old_outliers = pd.DataFrame(columns=[ 'px', 'location', 'class_type', 'leg', 'movement', 'datetime_bin', 'volume_15min' ])

# test to make sure these outliers will be caught
#d = {'px':[2, 275], 'location':['Front / Jarvis', "Richmond / Spadina"], 'class_type':['Cylcists', "Cyclists"], 'leg':['E', 'E'], 
   #  'movement':['Through', "Through"], 'datetime_bin':['2017-10-31 13:00:00', "2018-11-07 17:15:00"], 'volume_15min':[60, 400] }   
#df = pd.DataFrame(data=d)
#volumes = volumes.append(df)


for row in locations.itertuples():
    subset = volumes.loc[ (volumes['px'] == row.px) & (volumes['leg'] == row.leg) & (volumes['movement'] == row.movement)]

    p25, p75 = np.percentile(subset['volume_15min'], 25), np.percentile(subset['volume_15min'], 75)
    iqr = p75 - p25

    # calculate outlier cut off 
    # if volume value is not between lower and upper than it will be considered an outlier
    cut_off = iqr*1.5
    lower, upper = int(p25 - cut_off), int(p75 + cut_off)
    # we cannot have a negative traffic volume
    if lower < 0: 
        lower = 0
    
    # identify outliers
    frames = [old_outliers, subset.loc[(subset['volume_15min'] < lower) | (subset['volume_15min'] > upper)  ]  ]
    old_outliers = pd.concat(frames)
    
    
    # all records that are not outliers
    frames = [final_df, subset.loc[(subset['volume_15min'] >= lower) & (subset['volume_15min'] <= upper)]  ]
    final_df = pd.concat(frames)

```

Other percentile values (instead of 75 and 25) such as 90 and 10, 95 and 5, 90 and 5 were tested, but similar results were found in the end. These results included there being a huge amount of outliers that have values between 2 and 4, which are not "extreme values". 


```python
# get percentage of total records that are outliers 
(len(old_outliers.index) / len(volumes.index))*100 
```




    7.627231856834




```python
# QC checks
print((len(old_outliers.index) + len(final_df.index)) == len(volumes.index))
```

    True
    


```python
# write the dataframe final_df to a postgres table

from sqlalchemy import create_engine
engine = create_engine('postgresql://crosic:jAcoevpFfP28TonGsk0e@10.160.12.47:5432/bigdata')
# final_df.to_sql('open_data.volumes_tmc_bikes', engine)

# pandasql.to_sql(final_df, 'volumes_tmc_bikes', con, flavor='postgresql', schema='open_data')
```


```python
final_df.to_sql('volumes_tmc_bikes_final', engine)
```

The percentage of outliers is too high. Investigate outliers to determine if they really are outliers that should be removed from analysis. 

## Investigating outliers


```python
old_outliers['volume_15min'].hist(bins = np.arange(0, max(old_outliers['volume_15min']) , 1), figsize = (18, 10))
ax = plt.gca()
minor_ticks = np.arange(0, 50, 10)
major_ticks = np.arange(0, 50, 100)

#ax.set_xticks(major_ticks, labels=[major_ticks])
ax.set_xticks(minor_ticks, minor=True)

```




    [<matplotlib.axis.XTick at 0xf289908>,
     <matplotlib.axis.XTick at 0x26cc13c8>,
     <matplotlib.axis.XTick at 0xf296908>,
     <matplotlib.axis.XTick at 0xf07d240>,
     <matplotlib.axis.XTick at 0xf07d940>]




![png](output_20_1.png)



```python
#outliers

# 40% of outliers are equal to the value of 2
# 8% equal 3
subset_outliers = old_outliers.loc[(old_outliers['volume_15min']  == 2) | (old_outliers['volume_15min']  == 4) ]
len(subset_outliers.index)/len(old_outliers.index)*100
```




    47.91607091792965



## Incorporating analysis by date

The volumes were already being analysed by location, movement, and leg. This means that the volumes for every distinct location, movement and leg, were being analysed for outliers. Now we also want to inlcude month and hour into the analysis in order to see if this type of analysis will yield more accurate outlier results. 


```python
sql_volumes = '''
SELECT px, location, class_type, leg, movement, datetime_bin, volume_15min,  
date_part('month', datetime_bin) AS month, date_part('hour', datetime_bin) AS hour
FROM open_data.volumes_tmc_bikes
'''

sql_locations = '''
SELECT DISTINCT ON (px, leg, movement, date_part('month', datetime_bin), date_part('hour', datetime_bin)) 
px, leg, movement, date_part('month', datetime_bin) AS month, date_part('hour', datetime_bin) AS hour
FROM open_data.volumes_tmc_bikes
'''

locations = pandasql.read_sql(sql_locations, con)
```


```python
volumes = pandasql.read_sql(sql_volumes, con)
```


```python
final_df = pd.DataFrame(columns=[ 'px', 'location', 'class_type', 'leg', 'movement', 'datetime_bin', 'volume_15min', 'month', 'hour' ])
outliers = pd.DataFrame(columns=[ 'px', 'location', 'class_type', 'leg', 'movement', 'datetime_bin', 'volume_15min', 'month', 'hour'])

for row in locations.itertuples():
    subset = volumes.loc[ (volumes['px'] == row.px) & (volumes['leg'] == row.leg) & (volumes['movement'] == row.movement)
                        & (volumes['month'] == row.month)  & (volumes['hour'] == row.hour) ]

    p25, p75 = np.percentile(subset['volume_15min'], 25), np.percentile(subset['volume_15min'], 75)
    iqr = p75 - p25

    # calculate outlier cut off 
    # if volume value is not between lower and upper than it will be considered an outlier
    cut_off = iqr*1.5
    lower, upper = int(p25 - cut_off), int(p75 + cut_off)
    # we cannot have a negative traffic volume
    if lower < 0: 
        lower = 0
    
    # identify outliers
    frames = [outliers, subset.loc[(subset['volume_15min'] < lower) | (subset['volume_15min'] > upper)]  ]
    outliers = pd.concat(frames)
    
    
    # all records that are not outliers
    frames = [final_df, subset.loc[(subset['volume_15min'] >= lower) & (subset['volume_15min'] <= upper)]  ]
    final_df = pd.concat(frames)
```


```python
# get percentage of total records that are outliers 
(len(outliers.index) / len(volumes.index))*100
```




    4.353705516220409




```python
# QC checks
print((len(outliers.index) + len(final_df.index)) == len(volumes.index))
```

    True
    


```python
outliers['volume_15min'].hist(bins = np.arange(min(outliers['volume_15min']), 
                                               max(outliers['volume_15min']), 1), figsize = (18, 10))
```




    <matplotlib.axes._subplots.AxesSubplot at 0x16cec0f0>




![png](output_29_1.png)


Most of the outliers are low numbers. 


```python
outliers['month'].hist(bins = np.arange(min(outliers['month']), 
                                               max(outliers['month']), 1), figsize = (18, 10))
```




    <matplotlib.axes._subplots.AxesSubplot at 0x14b58860>




![png](output_31_1.png)


Notice that most of the outliers occur in the late fall, early spring and winter seasons. This means that the outliers occur in colder weather. 


```python
outliers['hour'].hist(bins = np.arange(min(outliers['hour']), 
                                               max(outliers['hour']), 1), figsize = (18, 10))
```




    <matplotlib.axes._subplots.AxesSubplot at 0x1bb9fa90>




![png](output_33_1.png)



```python

# 39% of outliers are equal to the value of 2
# 12% equal 3
subset_outliers = outliers.loc[(outliers['volume_15min']  == 2)  | (outliers['volume_15min']  == 3) ]
len(subset_outliers.index)/len(outliers.index)*100
```




    51.681382678940444



After looking at the analysis including date and hour, the outliers identified in both analyses do not seem to be "extreme values" (i.e. incorrect volumes collected). Therefore no "extremem values" records need to be removed from the dataset.  

# TMC Permanent Volumes

## Visualize data


```python
sql_volumes = '''
SELECT volume_15min
FROM open_data.volumes_tmc_permanent
'''
volumes = pandasql.read_sql(sql_volumes, con)


bin_val = np.arange(min(volumes['volume_15min']), max(volumes['volume_15min']), 1)

volumes.hist(bins=bin_val, figsize=(15,10))
plt.title('Volume counts for 15 minute bins')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)

```


![png](output_38_0.png)



```python
# max value is an extreme value 
print(min(volumes['volume_15min']), max(volumes['volume_15min']))
```

    1 2351
    


```python
sql_volumes = '''
SELECT volume_15min
FROM open_data.volumes_tmc_permanent
'''
volumes = pandasql.read_sql(sql_volumes, con)


bin_val = np.arange(min(volumes['volume_15min']), 250, 1)

volumes.hist(bins=bin_val, figsize=(15,10))
plt.title('Volume counts for 15 minute bins')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)
```


![png](output_40_0.png)



```python
sql_volumes = '''
SELECT volume_15min
FROM open_data.volumes_tmc_permanent
'''
volumes = pandasql.read_sql(sql_volumes, con)


bin_val = np.arange(250, max(volumes['volume_15min']), 1)

volumes.hist(bins=bin_val, figsize=(15,10))
plt.title('Volume counts for 15 minute bins that are above 250')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)
```


![png](output_41_0.png)



```python
sql_volumes = '''
SELECT volume_15min
FROM open_data.volumes_tmc_permanent
'''
volumes = pandasql.read_sql(sql_volumes, con)


bin_val = np.arange(1000, max(volumes['volume_15min']), 1)

volumes.hist(bins=bin_val, figsize=(15,10))
plt.title('Volume counts for 15 minute bins that are above 1000')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)
```


![png](output_42_0.png)


The volumes with values over 1500 seem to be extreme values. However, to be certain of this more research is needed. 


```python
check_1500 = '''
SELECT * 
FROM open_data.volumes_tmc_permanent
WHERE volume_15min > 1500
'''
pandasql.read_sql(check_1500, con)
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>int_id</th>
      <th>px</th>
      <th>location</th>
      <th>class_type</th>
      <th>leg</th>
      <th>movement</th>
      <th>datetime_bin</th>
      <th>volume_15min</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2017-11-06 16:30:00</td>
      <td>1575</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2017-11-06 16:45:00</td>
      <td>2351</td>
    </tr>
    <tr>
      <th>2</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-04-10 17:00:00</td>
      <td>2167</td>
    </tr>
    <tr>
      <th>3</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-04-11 17:00:00</td>
      <td>1720</td>
    </tr>
    <tr>
      <th>4</th>
      <td>13467163</td>
      <td>60</td>
      <td>Wellington / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2017-10-13 17:15:00</td>
      <td>1535</td>
    </tr>
  </tbody>
</table>
</div>



All of the counts are pedestrian counts. 4/5 are at the 'Front and Bay' intersection and the first two occur on the same day but 15 minutes apart.

To put more investigation in to the high pedestrian counts at Front and Bay at 4 and 5 pm, check the average amount of pedestrians at that intersection during those hours. 


```python
avg_16 = '''
SELECT AVG(volume_15min) FROM open_data.volumes_tmc_permanent
WHERE px = 59 and class_type = 'Pedestrians' and date_part('hour', datetime_bin) = 16
'''

pandasql.read_sql(avg_16, con)
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>avg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>387.884511</td>
    </tr>
  </tbody>
</table>
</div>




```python
avg_17 = '''
SELECT AVG(volume_15min) FROM open_data.volumes_tmc_permanent
WHERE px = 59 and class_type = 'Pedestrians' and date_part('hour', datetime_bin) = 17
'''

pandasql.read_sql(avg_17, con)
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>avg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>416.262228</td>
    </tr>
  </tbody>
</table>
</div>



The average values at those hours are a lot lower than 1500.  

Take a look at the 30 highest values at the Front and Bay intersection to see if there are any other values in and around the 1500 mark. 


```python
bay_front_top30 = '''
SELECT * 
FROM open_data.volumes_tmc_permanent
WHERE px = 59 and class_type = 'Pedestrians' and 
(date_part('hour', datetime_bin) = 17 OR date_part('hour', datetime_bin) = 16)
ORDER BY volume_15min DESC
LIMIT 30
'''

pandasql.read_sql(bay_front_top30, con)
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>int_id</th>
      <th>px</th>
      <th>location</th>
      <th>class_type</th>
      <th>leg</th>
      <th>movement</th>
      <th>datetime_bin</th>
      <th>volume_15min</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2017-11-06 16:45:00</td>
      <td>2351</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-04-10 17:00:00</td>
      <td>2167</td>
    </tr>
    <tr>
      <th>2</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-04-11 17:00:00</td>
      <td>1720</td>
    </tr>
    <tr>
      <th>3</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2017-11-06 16:30:00</td>
      <td>1575</td>
    </tr>
    <tr>
      <th>4</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>S</td>
      <td>None</td>
      <td>2017-11-08 16:45:00</td>
      <td>1442</td>
    </tr>
    <tr>
      <th>5</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-04-11 17:15:00</td>
      <td>1431</td>
    </tr>
    <tr>
      <th>6</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2018-03-05 17:00:00</td>
      <td>1315</td>
    </tr>
    <tr>
      <th>7</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-02-14 17:15:00</td>
      <td>1276</td>
    </tr>
    <tr>
      <th>8</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2018-04-09 17:00:00</td>
      <td>1229</td>
    </tr>
    <tr>
      <th>9</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-02-14 17:00:00</td>
      <td>1221</td>
    </tr>
    <tr>
      <th>10</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-03-05 17:00:00</td>
      <td>1168</td>
    </tr>
    <tr>
      <th>11</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2018-03-05 17:15:00</td>
      <td>1162</td>
    </tr>
    <tr>
      <th>12</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-03-05 17:30:00</td>
      <td>1157</td>
    </tr>
    <tr>
      <th>13</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2017-12-05 17:00:00</td>
      <td>1155</td>
    </tr>
    <tr>
      <th>14</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2017-11-02 17:00:00</td>
      <td>1102</td>
    </tr>
    <tr>
      <th>15</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2018-04-09 17:15:00</td>
      <td>1083</td>
    </tr>
    <tr>
      <th>16</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-03-05 17:15:00</td>
      <td>1055</td>
    </tr>
    <tr>
      <th>17</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2017-12-05 17:15:00</td>
      <td>1034</td>
    </tr>
    <tr>
      <th>18</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>S</td>
      <td>None</td>
      <td>2017-11-06 17:00:00</td>
      <td>1034</td>
    </tr>
    <tr>
      <th>19</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-01-17 17:00:00</td>
      <td>1032</td>
    </tr>
    <tr>
      <th>20</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-02-14 16:45:00</td>
      <td>1028</td>
    </tr>
    <tr>
      <th>21</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-03-06 17:00:00</td>
      <td>1025</td>
    </tr>
    <tr>
      <th>22</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-03-05 17:45:00</td>
      <td>1009</td>
    </tr>
    <tr>
      <th>23</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-04-10 17:15:00</td>
      <td>1004</td>
    </tr>
    <tr>
      <th>24</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-02-15 17:00:00</td>
      <td>996</td>
    </tr>
    <tr>
      <th>25</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-01-18 16:30:00</td>
      <td>989</td>
    </tr>
    <tr>
      <th>26</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-01-16 16:45:00</td>
      <td>986</td>
    </tr>
    <tr>
      <th>27</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2018-02-12 17:00:00</td>
      <td>983</td>
    </tr>
    <tr>
      <th>28</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2018-02-14 16:45:00</td>
      <td>971</td>
    </tr>
    <tr>
      <th>29</th>
      <td>13467389</td>
      <td>59</td>
      <td>Front / Bay</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>2017-11-07 17:15:00</td>
      <td>971</td>
    </tr>
  </tbody>
</table>
</div>



From the above data, there seems to be a lot of times where the pedestrian counts near the Front/Bay area are above 1000 around 4 or 5 pm in the afternoon. Looking at the above table, large amounts of traffic near that intersection during the hour of 4 or 5 pm is a trend. Therefore the values above 1500 are most likely not extreme/wrong. 

## IQR outlier analysis


```python
sql_volumes = '''
SELECT px, location, class_type, leg, movement, datetime_bin, volume_15min 
FROM open_data.volumes_tmc_permanent
'''

sql_locations = '''
SELECT DISTINCT ON (px, leg, movement, class_type) px, leg, movement, class_type
FROM open_data.volumes_tmc_permanent
'''

locations = pandasql.read_sql(sql_locations, con)
```


```python
volumes = pandasql.read_sql(sql_volumes, con)
```


```python
final_df = pd.DataFrame(columns=[ 'px', 'location', 'class_type', 'leg', 'movement', 'datetime_bin', 'volume_15min' ])
outliers = pd.DataFrame(columns=[ 'px', 'location', 'class_type', 'leg', 'movement', 'datetime_bin', 'volume_15min' ])


for row in locations.itertuples():
    subset = volumes.loc[ (volumes['px'] == row.px) & (volumes['leg'] == row.leg) & (volumes['movement'] == row.movement)
                        & (volumes['class_type'] == row.class_type)]

    p25, p75 = np.percentile(subset['volume_15min'], 25), np.percentile(subset['volume_15min'], 75)
    iqr = p75 - p25

    # calculate outlier cut off 
    # if volume value is not between lower and upper than it will be considered an outlier
    cut_off = iqr*1.5
    lower, upper = int(p25 - cut_off), int(p75 + cut_off)
    # we cannot have a negative traffic volume
    if lower < 0: 
        lower = 0
    
    # identify outliers
    frames = [outliers, subset.loc[(subset['volume_15min'] < lower) | (subset['volume_15min'] > upper)  ]  ]
    outliers = pd.concat(frames)
    
    
    # all records that are not outliers
    frames = [final_df, subset.loc[(subset['volume_15min'] >= lower) & (subset['volume_15min'] <= upper)]  ]
    final_df = pd.concat(frames)
```


```python
# get percentage of total records that are outliers 
(len(outliers.index) / len(volumes.index))*100 
```




    3.360777858020377




```python
# QC checks
print((len(outliers.index) + len(final_df.index)) == len(volumes.index))
```

    True
    


```python
outliers['volume_15min'].hist(bins = np.arange(0, max(outliers['volume_15min']) , 1), figsize = (18, 10))
ax = plt.gca()
minor_ticks = np.arange(0, 50, 10)
major_ticks = np.arange(0, 50, 100)

#ax.set_xticks(major_ticks, labels=[major_ticks])
ax.set_xticks(minor_ticks, minor=True)

```




    [<matplotlib.axis.XTick at 0x2312d860>,
     <matplotlib.axis.XTick at 0x230aa5c0>,
     <matplotlib.axis.XTick at 0x2312d710>,
     <matplotlib.axis.XTick at 0x2d5bb668>,
     <matplotlib.axis.XTick at 0x231ab080>]




![png](output_57_1.png)


The above histogram is displaying all of the values that were considered to be outliers. None of these values are "extreme values", the outliers can be attributed to the large variation in volume values. Therefore there are no extremem values to be removed from the TMC permanent dataset. 

# TMC Short term
## Visualize data


```python
sql_volumes = '''
SELECT volume_15min
FROM open_data.volumes_tmc_shortterm
'''
volumes = pandasql.read_sql(sql_volumes, con)

```


```python
bin_val = np.arange(min(volumes['volume_15min']), max(volumes['volume_15min']), 1)

volumes.hist(bins=bin_val, figsize=(15,10))
plt.title('Volume counts for 15 minute bins')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)
plt.xlim(0, 1500)
```




    (0, 1500)




![png](output_61_1.png)



```python
max(volumes['volume_15min'])
```




    13218




```python
# look at large values ... i.e. volumes more than 1000

bin_val = np.arange(1000, max(volumes['volume_15min']), 1)

volumes.hist(bins=bin_val, figsize=(15,10))
plt.title('Volume counts for 15 minute bins')
plt.rc('font', size=10)          
plt.rc('axes', titlesize=15)
```


![png](output_63_0.png)



```python
check_1000 = '''
SELECT * 
FROM open_data.volumes_tmc_shortterm
WHERE volume_15min > 1000
'''
pandasql.read_sql(check_1000, con)
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>int_id</th>
      <th>px</th>
      <th>location</th>
      <th>class_type</th>
      <th>leg</th>
      <th>movement</th>
      <th>datetime_bin</th>
      <th>volume_15min</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1998-01-19 08:00:00</td>
      <td>1084</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1998-01-19 08:15:00</td>
      <td>1114</td>
    </tr>
    <tr>
      <th>2</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1998-01-19 08:30:00</td>
      <td>1102</td>
    </tr>
    <tr>
      <th>3</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1998-01-19 08:45:00</td>
      <td>1067</td>
    </tr>
    <tr>
      <th>4</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-12-17 07:45:00</td>
      <td>1003</td>
    </tr>
    <tr>
      <th>5</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-12-17 08:00:00</td>
      <td>1058</td>
    </tr>
    <tr>
      <th>6</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-12-17 08:15:00</td>
      <td>1073</td>
    </tr>
    <tr>
      <th>7</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-12-17 08:30:00</td>
      <td>1061</td>
    </tr>
    <tr>
      <th>8</th>
      <td>13469154</td>
      <td>1344</td>
      <td>BRITISH COLUMBIA RD AT LAKE SHORE BLVD (PX 1344)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-12-17 08:45:00</td>
      <td>1017</td>
    </tr>
    <tr>
      <th>9</th>
      <td>13469216</td>
      <td>930</td>
      <td>LAKE SHORE BLVD AT ONTARIO DR &amp; REMEMBRANCE DR...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1995-03-02 07:45:00</td>
      <td>1022</td>
    </tr>
    <tr>
      <th>10</th>
      <td>13469216</td>
      <td>930</td>
      <td>LAKE SHORE BLVD AT ONTARIO DR &amp; REMEMBRANCE DR...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1995-03-02 08:00:00</td>
      <td>1122</td>
    </tr>
    <tr>
      <th>11</th>
      <td>13469216</td>
      <td>930</td>
      <td>LAKE SHORE BLVD AT ONTARIO DR &amp; REMEMBRANCE DR...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1995-03-02 08:15:00</td>
      <td>1205</td>
    </tr>
    <tr>
      <th>12</th>
      <td>13469216</td>
      <td>930</td>
      <td>LAKE SHORE BLVD AT ONTARIO DR &amp; REMEMBRANCE DR...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1995-03-02 08:30:00</td>
      <td>1161</td>
    </tr>
    <tr>
      <th>13</th>
      <td>13469216</td>
      <td>930</td>
      <td>LAKE SHORE BLVD AT ONTARIO DR &amp; REMEMBRANCE DR...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1995-03-02 08:45:00</td>
      <td>1040</td>
    </tr>
    <tr>
      <th>14</th>
      <td>13469216</td>
      <td>930</td>
      <td>LAKE SHORE BLVD AT ONTARIO DR &amp; REMEMBRANCE DR...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1997-03-04 08:15:00</td>
      <td>1080</td>
    </tr>
    <tr>
      <th>15</th>
      <td>13469216</td>
      <td>930</td>
      <td>LAKE SHORE BLVD AT ONTARIO DR &amp; REMEMBRANCE DR...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1997-03-04 08:30:00</td>
      <td>1023</td>
    </tr>
    <tr>
      <th>16</th>
      <td>13468803</td>
      <td>222</td>
      <td>LAKE SHORE BLVD AT STRACHAN AVE (PX 222)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1992-08-12 08:00:00</td>
      <td>1021</td>
    </tr>
    <tr>
      <th>17</th>
      <td>13468803</td>
      <td>222</td>
      <td>LAKE SHORE BLVD AT STRACHAN AVE (PX 222)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-10-20 07:30:00</td>
      <td>1035</td>
    </tr>
    <tr>
      <th>18</th>
      <td>13468803</td>
      <td>222</td>
      <td>LAKE SHORE BLVD AT STRACHAN AVE (PX 222)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-10-20 07:45:00</td>
      <td>1059</td>
    </tr>
    <tr>
      <th>19</th>
      <td>13468803</td>
      <td>222</td>
      <td>LAKE SHORE BLVD AT STRACHAN AVE (PX 222)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-10-20 08:00:00</td>
      <td>1091</td>
    </tr>
    <tr>
      <th>20</th>
      <td>13468803</td>
      <td>222</td>
      <td>LAKE SHORE BLVD AT STRACHAN AVE (PX 222)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-10-20 08:15:00</td>
      <td>1120</td>
    </tr>
    <tr>
      <th>21</th>
      <td>13468803</td>
      <td>222</td>
      <td>LAKE SHORE BLVD AT STRACHAN AVE (PX 222)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-10-20 08:45:00</td>
      <td>1016</td>
    </tr>
    <tr>
      <th>22</th>
      <td>13457092</td>
      <td>447</td>
      <td>O CONNOR DR AT ST CLAIR AVE (PX 447)</td>
      <td>Vehicles</td>
      <td>S</td>
      <td>Through</td>
      <td>1990-06-26 09:15:00</td>
      <td>1280</td>
    </tr>
    <tr>
      <th>23</th>
      <td>13450532</td>
      <td>1053</td>
      <td>KINGSTON RD AT SCARBOROUGH GOLF CLUB RD (PX 1053)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1989-02-08 16:00:00</td>
      <td>1055</td>
    </tr>
    <tr>
      <th>24</th>
      <td>13450532</td>
      <td>1053</td>
      <td>KINGSTON RD AT SCARBOROUGH GOLF CLUB RD (PX 1053)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1989-02-08 16:15:00</td>
      <td>1113</td>
    </tr>
    <tr>
      <th>25</th>
      <td>13450532</td>
      <td>1053</td>
      <td>KINGSTON RD AT SCARBOROUGH GOLF CLUB RD (PX 1053)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1989-02-08 16:30:00</td>
      <td>1181</td>
    </tr>
    <tr>
      <th>26</th>
      <td>13450532</td>
      <td>1053</td>
      <td>KINGSTON RD AT SCARBOROUGH GOLF CLUB RD (PX 1053)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1989-02-08 16:45:00</td>
      <td>1199</td>
    </tr>
    <tr>
      <th>27</th>
      <td>13450532</td>
      <td>1053</td>
      <td>KINGSTON RD AT SCARBOROUGH GOLF CLUB RD (PX 1053)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1989-02-08 17:00:00</td>
      <td>1355</td>
    </tr>
    <tr>
      <th>28</th>
      <td>13450532</td>
      <td>1053</td>
      <td>KINGSTON RD AT SCARBOROUGH GOLF CLUB RD (PX 1053)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1989-02-08 17:15:00</td>
      <td>1529</td>
    </tr>
    <tr>
      <th>29</th>
      <td>13450532</td>
      <td>1053</td>
      <td>KINGSTON RD AT SCARBOROUGH GOLF CLUB RD (PX 1053)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>1989-02-08 17:30:00</td>
      <td>1542</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>195</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2000-07-13 08:15:00</td>
      <td>1074</td>
    </tr>
    <tr>
      <th>196</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2000-07-13 08:30:00</td>
      <td>1042</td>
    </tr>
    <tr>
      <th>197</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-05-11 07:30:00</td>
      <td>1078</td>
    </tr>
    <tr>
      <th>198</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-05-11 07:45:00</td>
      <td>1108</td>
    </tr>
    <tr>
      <th>199</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-05-11 08:00:00</td>
      <td>1116</td>
    </tr>
    <tr>
      <th>200</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-05-11 08:15:00</td>
      <td>1082</td>
    </tr>
    <tr>
      <th>201</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-05-11 08:30:00</td>
      <td>1053</td>
    </tr>
    <tr>
      <th>202</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2004-05-11 08:45:00</td>
      <td>1005</td>
    </tr>
    <tr>
      <th>203</th>
      <td>13468529</td>
      <td>1456</td>
      <td>COLBORNE LODGE DR AT LAKE SHORE BLVD (PX 1456)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2008-08-07 08:15:00</td>
      <td>1019</td>
    </tr>
    <tr>
      <th>204</th>
      <td>13467345</td>
      <td>76</td>
      <td>UNIVERSITY AVE AT WELLINGTON ST (PX 76)</td>
      <td>Vehicles</td>
      <td>S</td>
      <td>Through</td>
      <td>2015-04-18 16:45:00</td>
      <td>1588</td>
    </tr>
    <tr>
      <th>205</th>
      <td>13467123</td>
      <td>77</td>
      <td>KING ST AT UNIVERSITY AVE (PX 77)</td>
      <td>Pedestrians</td>
      <td>E</td>
      <td>None</td>
      <td>1991-06-20 14:45:00</td>
      <td>2911</td>
    </tr>
    <tr>
      <th>206</th>
      <td>13467163</td>
      <td>60</td>
      <td>BAY ST AT WELLINGTON ST (PX 60)</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2009-08-12 16:30:00</td>
      <td>1008</td>
    </tr>
    <tr>
      <th>207</th>
      <td>13467163</td>
      <td>60</td>
      <td>BAY ST AT WELLINGTON ST (PX 60)</td>
      <td>Pedestrians</td>
      <td>W</td>
      <td>None</td>
      <td>2009-08-12 17:00:00</td>
      <td>1260</td>
    </tr>
    <tr>
      <th>208</th>
      <td>13458463</td>
      <td>433</td>
      <td>DIXON RD AT ROYAL YORK RD &amp; ST PHILLIPS RD (PX...</td>
      <td>Pedestrians</td>
      <td>N</td>
      <td>None</td>
      <td>1986-05-13 09:15:00</td>
      <td>2101</td>
    </tr>
    <tr>
      <th>209</th>
      <td>13463078</td>
      <td>262</td>
      <td>BLOOR ST AT SHERBOURNE ST (PX 262)</td>
      <td>Vehicles</td>
      <td>E</td>
      <td>Through</td>
      <td>1989-07-31 17:15:00</td>
      <td>2261</td>
    </tr>
    <tr>
      <th>210</th>
      <td>13464943</td>
      <td>303</td>
      <td>BATHURST ST AT HARBORD ST (PX 303)</td>
      <td>Pedestrians</td>
      <td>N</td>
      <td>None</td>
      <td>1991-06-25 11:45:00</td>
      <td>4315</td>
    </tr>
    <tr>
      <th>211</th>
      <td>13466312</td>
      <td>18</td>
      <td>CHURCH ST AT RICHMOND ST (PX 18)</td>
      <td>Vehicles</td>
      <td>N</td>
      <td>Through</td>
      <td>1990-08-09 13:00:00</td>
      <td>1076</td>
    </tr>
    <tr>
      <th>212</th>
      <td>13447128</td>
      <td>744</td>
      <td>LESLIE ST AT SHEPPARD AVE (PX 744)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2008-08-19 17:45:00</td>
      <td>2655</td>
    </tr>
    <tr>
      <th>213</th>
      <td>14255079</td>
      <td>1917</td>
      <td>FORT YORK BLVD AT LAKE SHORE BLVD (PX 1917)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-04-02 07:30:00</td>
      <td>1147</td>
    </tr>
    <tr>
      <th>214</th>
      <td>14255079</td>
      <td>1917</td>
      <td>FORT YORK BLVD AT LAKE SHORE BLVD (PX 1917)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-04-02 07:45:00</td>
      <td>1189</td>
    </tr>
    <tr>
      <th>215</th>
      <td>14255079</td>
      <td>1917</td>
      <td>FORT YORK BLVD AT LAKE SHORE BLVD (PX 1917)</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2003-04-02 08:15:00</td>
      <td>1001</td>
    </tr>
    <tr>
      <th>216</th>
      <td>0</td>
      <td>1915</td>
      <td>STEELES AVE AT 185M W OF HIDDEN TRAIL &amp; AVENTI...</td>
      <td>Vehicles</td>
      <td>E</td>
      <td>Through</td>
      <td>2008-03-20 08:15:00</td>
      <td>1059</td>
    </tr>
    <tr>
      <th>217</th>
      <td>13449271</td>
      <td>2344</td>
      <td>COMBERMERE DR AT VICTORIA PARK AVE (PX 2344)</td>
      <td>Vehicles</td>
      <td>S</td>
      <td>Through</td>
      <td>2005-02-07 07:30:00</td>
      <td>2857</td>
    </tr>
    <tr>
      <th>218</th>
      <td>0</td>
      <td>2089</td>
      <td>LAKE SHORE BLVD AT 25M E OF BUDAPEST LANE (PX ...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2010-03-29 07:45:00</td>
      <td>1038</td>
    </tr>
    <tr>
      <th>219</th>
      <td>0</td>
      <td>2089</td>
      <td>LAKE SHORE BLVD AT 25M E OF BUDAPEST LANE (PX ...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2010-03-29 08:00:00</td>
      <td>1069</td>
    </tr>
    <tr>
      <th>220</th>
      <td>0</td>
      <td>2089</td>
      <td>LAKE SHORE BLVD AT 25M E OF BUDAPEST LANE (PX ...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2010-03-29 08:15:00</td>
      <td>1097</td>
    </tr>
    <tr>
      <th>221</th>
      <td>0</td>
      <td>2089</td>
      <td>LAKE SHORE BLVD AT 25M E OF BUDAPEST LANE (PX ...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2010-03-29 08:30:00</td>
      <td>1186</td>
    </tr>
    <tr>
      <th>222</th>
      <td>0</td>
      <td>2089</td>
      <td>LAKE SHORE BLVD AT 25M E OF BUDAPEST LANE (PX ...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2010-03-29 08:45:00</td>
      <td>1204</td>
    </tr>
    <tr>
      <th>223</th>
      <td>0</td>
      <td>2089</td>
      <td>LAKE SHORE BLVD AT 25M E OF BUDAPEST LANE (PX ...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2010-03-29 09:00:00</td>
      <td>1077</td>
    </tr>
    <tr>
      <th>224</th>
      <td>0</td>
      <td>2089</td>
      <td>LAKE SHORE BLVD AT 25M E OF BUDAPEST LANE (PX ...</td>
      <td>Vehicles</td>
      <td>W</td>
      <td>Through</td>
      <td>2010-03-29 09:15:00</td>
      <td>1055</td>
    </tr>
  </tbody>
</table>
<p>225 rows × 8 columns</p>
</div>



The maximum value in this dataset is a volume of 13218, and there are very few volumes in general over 1000. Values this high are normal as discovered when analysing the TMC permanent dataset. Also, the volume values and their counts decrease gradually as the volume increases, so these high values are not random. An IQR test would pull some outliers, but since we can verifuy that none of the values in the dataset are extreme, the test is not necessary in this case. 


```python
con.close()
```
