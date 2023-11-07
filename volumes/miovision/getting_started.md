# Getting Started <!-- omit in toc -->

- [Getting Started](#getting-started)
      - [Understanding Legs, Movement and Direction of Travel](#understanding-legs-movement-and-direction-of-travel)
        - [Vehicle Movements](#vehicle-movements)
        - [Pedestrian Movement](#pedestrian-movement)
        - [From Movement Counts to Segment Counts](#from-movement-counts-to-segment-counts)
          - [All the East Leg Crossings!](#all-the-east-leg-crossings)

## Understanding Legs, Movement and Direction of Travel 

The Miovision data table that we receive tracks information about:
1. where the vehicle / pedestrian / cyclist entered the intersection, 
2. mode used (see the classification table), and, 
3. how the vehicle / pedestrian / cyclist moved through the intersection (see the movements table).

Movement is tracked differently for vehicles (including bicycles) and pedestrians.

### Vehicle Movements

Vehicle movement is quite straightforward - upon approaching an intersection, a vehicle may proceed straight through, turn left, turn right or perform a U-turn (aka the Uno Reverse Card of driving).

Here is a diagram that shows the all vehicle movements from the eastern leg of an intersection:
<img src = "img/mio_mvt.png" alt= "Legs and Vehicle Movements from the East Leg" width = "500" height = "500" title = "Legs and Vehicle Movements from the East Leg">

You may have noticed that there are two entries for bicycles in the `classifications` table - one for turning movement counts (aka `classification_uid = 2`) and one for bicycle entrances and exits (aka `classification_uid = 10`). There are also entries in the `movements` table that represent bicycle entries and exits. Th

### Pedestrian Movement

Pedestrian movements are measured less intuitively than vehicle movements. The leg on which pedestrians' movement is tracked is the leg they are crossing. Their movement is tracked using clockwise ("cw") or counterclockwise ("ccw") directions (POV: you're in freefall above the intersection, looking down at it). So, a pedestrian crossing the eastern leg of an intersection in the clockwise direction would be walking south.

Here is a diagram that shows the pedestrian movements at an intersection:
<img src = "img/mio_ped_mvt.png" alt= "Legs and Pedestrian Movements" width = "500" height = "500" title = "Legs and Pedestrian Movements">

### From Movement Counts to Segment Counts

We are able to transform vehicle movements into segment volume counts, which are commonly referred to as "Across The Road" or ATR counts. The full process is described below. Conceptually:
- the movement tables track volume at a point in the centre of an intersection, while,
- the ATR table tracks volume as it crosses a line. A typical "t" intersection has 4 lines - one for each of the road segments that lead to and from an intersection.

To complicate matters even more, the 4 lines across the road segments leading to and from an intersection are further subdivided by direction - so the east leg of an intersection has an eastbound direction and a westbound direction.

Here's a diagram to visualize all of those directions and legs:
<img src = "img/mio_atr.png" alt= "Measuring vehicles as they cross the road" width = "500" height = "500" title = "Measuring vehicles as they cross the road">

Vehicles crossing the line on the east leg, while travelling west, are entering the intersection.

Vehicles crossing the line on the east leg, while travelling east, are exiting the intersection. They may be:
- travelling straight through the intersection from the west leg,
- turning left onto the east leg from the north leg,
- turning right onto the east leg from the south leg,
- u-turning from the east leg right back onto that east leg.

### All the East Leg Crossings!
Still fuzzy? Here's a diagram to help you picture it perfectly:
<img src = "img/mio_atr_zoomin.png" alt= "All The East Leg Crossings" width = "500" height = "500" title = "All The East Leg Crossings">

#### Comparing TMC and ATR Counts

The following example illustrates the differences between the TMC counts found in `volumes_15min_mvt` and `volumes_15min`.

For light vehicles at King / Bay on 2020-10-15 9:00-9:15 (`intersection_uid = 17`, `datetime_bin = '2020-10-15 09:00:00'`, `classification_uid = 1`), the TMC and ATR movements are:

|leg(tmc)|movement_uid|volume|
|--------|------------|------|
| E | 1 | 13  |
| E | 2 | 0   |
| E | 3 | 5   |
| N | 1 | 82  |
| N | 2 | 0   |
| N | 3 | 1   |
| S | 1 | 144 |
| S | 2 | 0   |
| S | 3 | 0   |
| W | 1 | 12  |
| W | 2 | 2   |
| W | 3 | 9   |
| | |total volume = 268|

|leg(atr)|dir|volume|
|--------|---|------|
| E | EB | 12  |
| E | WB | 18  |
| N | NB | 151 |
| N | SB | 83  |
| S | NB | 144 |
| S | SB | 91  |
| W | EB | 23  |
| W | WB | 14  |
| | |total volume = 536|

The ATR table exactly double-counts the number of vehicles travelling through intersections, since it counts vehicles approaching and exiting the intersection. For example, `leg(atr) = E` and `dir = EB` represents vehicles exiting the intersection, and `leg(atr) = E` and `dir = WB` represents vehicles approaching the intersection.

**It is important to note that pedestrian counts (`classification_uid` = 6 and `movement_uid` IN (5, 6)) in the TMC and ATR tables have an equal number of rows and equal total volume - they are not double counted like vehicles (including bicycles).**

**In the TMC table (aka `volumes_15min_mvt`)**
The `leg` represents the side of the intersection that the pedestrian is crossing. Pedestrian movements are tracked using `movement_uid` 5 or 6 (clockwise or counterclockwise, respectively). [This diagram](#Pedestrian-Movement) will help you visualize the clockwise and counterclockwise movements.

**In the ATR table (aka `volumes_15min`)** 
The `leg` represents the side of the intersection that the pedestrian is crossing. The `dir` represents which direction they are walking towards. So, if leg = N and dir = EB means that the pedestrian is at the North crosswalk crossing from the west side to the east side.

### Calculating Volumes on a Segment

To calculate volumes along a segment, use the approach volumes. When exit volumes are used, the vehicles turning into the intersection must be taken into account, and that adds another level of complexity. 

To calculate approach volumes, ensure that the leg and the direction are opposites. A vehicle travelling north (`dir = 'NB'`) approaches an intersection from the south (`leg = 'S'`). Conversely, if a vehicle is travelling north (`dir = 'NB'`) on the north leg of an intersection (`leg = 'N'`) the vehicle has exited the intersection - the leg and the direction are the same!

#### See it in code!
To calculate all vehicle volumes on an East-West street with traffic in both directions, add the west bound traffic on the east leg to the east bound traffic on the west leg. The code snippet below calculates the average, minimum and maximum weekday vehicle volumes for King and Bathurst (`intersection_uid = 10`) and King and Spadina (`intersection_uid = 12`) in October 2023.
```
WITH daily_volumes AS (
    SELECT
        i.intersection_uid,
        i.intersection_name,
        date_trunc('day', datetime_bin) AS dt,
        SUM(volume) AS daily_vol
    FROM miovision_api.intersections AS i
    JOIN mio_staging.volumes_15min AS dv
        USING (intersection_uid)
    WHERE
        i.intersection_uid IN (10, 12)
        AND classification_uid = 1 --vehicles
        AND dv.datetime_bin >= '2023-10-01'::date
        AND dv.datetime_bin < '2023-11-01'::date
        AND date_part('isodow', datetime_bin) <= 5 -- weekdays
        AND date_trunc('day', datetime_bin) <> '2023-10-09'::timestamp --thanksgiving monday
        AND ((dv.leg = 'E' AND dv.dir = 'WB')
            OR (dv.leg = 'W' AND dv.dir = 'EB'))
    GROUP BY
        i.intersection_uid,
        i.intersection_name,
        dt
)
SELECT
    intersection_uid, 
    intersection_name, 
    ROUND(AVG(daily_vol)) AS avg_daily,
    ROUND(MIN(daily_vol)) AS min_daily,
    ROUND(MAX(daily_vol)) AS max_daily
FROM daily_volumes
WHERE daily_vol > 0
GROUP BY 1,2
```
