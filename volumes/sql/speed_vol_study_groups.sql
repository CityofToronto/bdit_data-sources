-- Make a mat view that binds speed volume studies that occur over multiple days on the same arterycode with a unique group id
-- Currently this is only being done for speed volume studies but other count types are a wip

CREATE MATERIALIZED VIEW traffic.speed_vol_study_groups AS (

-- get all the studies from 2012 onward (based on a request - may be revised)
WITH studs AS (
    SELECT
        ci.count_info_id,
        ci.arterycode,
        ci.count_date,
        ci.day_no
    FROM traffic.countinfo AS ci
    WHERE ci.category_id = 4 AND ci.count_date >= '2012-01-01' -- may be revised
    ORDER BY ci.arterycode, ci.count_date 
),

-- identify which dates actually belong to the same study by checking if the dates are consecutive
ids AS (
    SELECT 
        s.*,
        ROW_NUMBER () OVER (ORDER BY arterycode, count_date) AS row_num,
        CASE 
            WHEN s.count_date - LAG(s.count_date, 1) OVER (PARTITION BY s.arterycode ORDER BY count_date) = 1 THEN 0
            ELSE 1
        END AS cnsc_dt
    FROM studs AS s
),

-- assign a group id for studies on consecutive dates
study_groups AS (
    SELECT 
        ids.row_num,
        ids.count_info_id,
        ids.arterycode,
        ids.count_date,
        ids.day_no,
        ids.cnsc_dt,
        SUM (
            CASE 
                WHEN ids.cnsc_dt = 1 THEN 1
            ELSE 0
            END) OVER (ORDER BY ids.row_num) AS study_group
    FROM ids
),

-- find out the min and max dates for each study group and put the count_info_ids in an array in case they're useful later
study_group_dates AS (
    SELECT
        sg.study_group,    
        array_agg(sg.count_info_id) AS count_info_ids,    
        sg.arterycode,
        min(sg.count_date) AS start_date,
        max(sg.count_date) AS end_date
    FROM study_groups AS sg
    GROUP BY
        sg.arterycode,
        sg.study_group
)

-- final table with some extra stats (count days and explicit days of the week) for added fun and usefulness
SELECT 
    sgd.*,
    (sgd.end_date - sgd.start_date) + 1 AS count_days,
    TO_CHAR(sgd.start_date, 'Day') || ' to  ' || TO_CHAR(sgd.end_date, 'Day') AS count_dows
FROM study_group_dates AS sgd
);

ALTER MATERIALIZED VIEW traffic.speed_vol_study_groups
OWNER TO traffic_admins;