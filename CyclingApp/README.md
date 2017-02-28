# Cycling App Data

Transportation Services commissioned the [Toronto Cycling App](http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=5c555cb1e7506410VgnVCM10000071d60f89RCRD&vgnextchannel=6f65970aa08c1410VgnVCM10000071d60f89RCRD&appInstanceName=default) by Brisk Synergies in 2014. The App allows cyclists to record their cycling routes and provide this data to the City. This data was used to inform the development of the [new Cycling Plan](https://static1.squarespace.com/static/574710fe2fe131d4ab0794d8/t/5762e8cc46c3c4824d561a75/1466099934772/4.2) (pdf) and also assists in the ongoing monitoring of cycling patterns over time as cycling infrastructure is improved and expanded. 

The following data dumps are available from Brisk's analytics portal. 
```
trip_surveys.csv
user_surveys.csv

```

Additionally, for the TrafficJam Hackathon were provided:  
 - daily od-matrices (TTS 2006 TAZ)
 - a shapefile of volume by links (TCL)

## Trip_surveys
"2017" data has 1867 rows
There is overlap in trips between 2014 & 2015 files

trip_id: bigint
app_user_id: 
started_at: timestamp in UTC, 2017 file from 2016-01-25 to 2017-02-02
purpose: trip purpose, see below table
notes: Mostly empty, otherwise trip purpose for "other", some are in json format like: `{"comments":"test"}`

|Purpose|Trips|
|-------|----:|
|Autre|0.05%|
|Commute|62.22%|
|Errand|2.41%|
|Exercise|1.18%|
|Home|5.14%|
|Leisure|2.41%|
|Loisirs|0.05%|
|Other|9.43%|
|School|3.43%|
|Shopping|4.82%|
|Social|5.25%|
|Workrelated|0.16%|
|Work-related|3.43%|



## User_surveys
7934 rows, spanning ids from 1 to 8094. Previous years' portals have a subset of the most recent file. Some of the values for the variables don't seem to match the answer key in the json below.

|column| type| notes|
|------|-----|------|
|app_user_id|int| |
|winter| [0-2]| see below |
|rider_history| [0-4] | not defined |
|workZIP|varchar(7) |Postal Code|
|income|[0-6]| see below |
|cyclingFreq| 0 or null | see below |
|age|  [0-7]| see below |
|cycling_level| [0-3] |see below  |
|gender| [0-3] | see below |
|rider_type| 0 or null| see below |
|schoolZIP| varchar(7) |Postal Code|
|homeZIP| varchar(7) |Postal Code||
|cyclingExperience| [0-4]| see below |
|preference_key_userpref| boolean | TRUE or null |

The `preference_key_userpref` column starts at app_user 6821, only three users have the "True" value. The number of commas in the csv wasn't consistent, so use [`python/fix_user_surveys.py`](python/fix_user_surveys.py) to correct this.

The dictionary for most of the answers are in the json below. See [`sql/insert_json_data.sql`](sql/insert_json_data.sql) for a way of automating creation of a normalized table structure for each variable in the json.

```json
"cyclingfreq": {
        "0": "Less than once a month",
        "1": "Several times a month",
        "2": "Several times per week",
        "3": "Daily"
    },
    "age": {
        "1": "Less than 18",
        "2": "1824",
        "3": "2534",
        "4": "3544",
        "5": "4554",
        "6": "5564",
        "7": "65"
    },
    "gender": {
        "1": "Female",
        "2": "Male",
        "3": "Other",
        "4": "Not Specified"
    },
    "winter": {
        "1": "No",
        "2": "Yes"
    },
    "income": {
        "1": "Less than 20,000",
        "2": "20,000 to 39,999",
        "3": "40,000 to 59,999",
        "4": "60,000 to 74,999",
        "5": "75,000 to 99,999",
        "6": "100,000 or greater"
    },
    "rider_type": {
        "1": "Strong & fearless",
        "2": "Enthused & confident",
        "3": "Comfortable, but cautious",
        "4": "Interested, but concerned"
    },
    "cyclingExperience": {
        "1": "Since childhood",
        "2": "Several years",
        "3": "One year or less",
        "4": "Just trying it out just started"
    },
    "cycling_level": {
        "1": "Not comfortable riding with traffic",
        "2": "Only comfortable with dedicated facilities",
        "3": "Comfortable riding with traffic"
    }
```

## Trip GPS
Download portal requires you to enter a bounding box and start and end date for trip filtering. Can only download 1000 trips at a time. We got a month of data for TrafficJam which was ~1GB. 

Fields (all values in the csv are quoted)

|column| type| notes|
|------|-----|------|
|coord_id| bigint |serial for GPS ping |
|trip_id| bigint | trip uid |
|recorded_at| timestamp | **timezone unclear** (probably UTC) |
|longitude| numeric | 6 decimal | 
|latitude| numeric | 6 decimal |
|altitude| numeric | appears to be meters |
|speed| numeric | Avg: 4.81 so probably km/h?  |
|hort_accuracy| numeric | probably meters |
|vert_accuracy| numeric | probably meters |