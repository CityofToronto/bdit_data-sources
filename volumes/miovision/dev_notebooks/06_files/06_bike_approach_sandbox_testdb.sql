-- RAW VOLUMES TABLE.

CREATE TABLE czhu.miovision_volumes
(
    volume_uid bigint NOT NULL DEFAULT nextval('czhu.miovision_volumes_volume_uid_seq'::regclass),
    intersection_uid integer,
    datetime_bin timestamp without time zone,
    classification_uid integer,
    leg text COLLATE pg_catalog."default",
    movement_uid integer,
    volume integer,
    volume_15min_tmc_uid integer,
    CONSTRAINT volume_uid_pkey PRIMARY KEY (volume_uid),
    CONSTRAINT volumes_intersection_uid_datetime_bin_classification_uid_le_key UNIQUE (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE czhu.miovision_volumes
    OWNER to czhu;

GRANT ALL ON TABLE czhu.miovision_volumes TO bdit_bots;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE czhu.miovision_volumes TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE czhu.miovision_volumes TO dbadmin;

GRANT ALL ON TABLE czhu.miovision_volumes TO miovision_admins;

GRANT ALL ON TABLE czhu.miovision_volumes TO rds_superuser WITH GRANT OPTION;

CREATE INDEX volumes_classification_leg_movement_idx
    ON czhu.miovision_volumes USING btree
    (classification_uid ASC NULLS LAST, leg COLLATE pg_catalog."default" ASC NULLS LAST, movement_uid ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX volumes_datetime_bin_idx
    ON czhu.miovision_volumes USING btree
    (datetime_bin ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX volumes_intersection_uid_idx
    ON czhu.miovision_volumes USING btree
    (intersection_uid ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX volumes_volume_15min_tmc_uid_idx
    ON czhu.miovision_volumes USING btree
    (volume_15min_tmc_uid ASC NULLS LAST)
    TABLESPACE pg_default;


-- 15 MINUTE TMC TABLE.

CREATE TABLE czhu.miovision_volumes_15min_tmc
(
  volume_15min_tmc_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume numeric,
  processed boolean,
  CONSTRAINT volumes_15min_tmc_pkey PRIMARY KEY (volume_15min_tmc_uid),
  CONSTRAINT volumes_15min_tmc_intersection_uid_datetime_bin_classificat_key UNIQUE (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE czhu.miovision_volumes_15min_tmc
  OWNER TO czhu;
GRANT ALL ON TABLE czhu.miovision_volumes_15min_tmc TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE czhu.miovision_volumes_15min_tmc TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE czhu.miovision_volumes_15min_tmc TO bdit_humans WITH GRANT OPTION;

CREATE INDEX volumes_15min_tmc_classification_uid_idx
  ON czhu.miovision_volumes_15min_tmc
  USING btree
  (classification_uid);

CREATE INDEX volumes_15min_tmc_datetime_bin_idx
  ON czhu.miovision_volumes_15min_tmc
  USING btree
  (datetime_bin);

CREATE INDEX volumes_15min_tmc_intersection_uid_idx
  ON czhu.miovision_volumes_15min_tmc
  USING btree
  (intersection_uid);

CREATE INDEX volumes_15min_tmc_leg_movement_uid_idx
  ON czhu.miovision_volumes_15min_tmc
  USING btree
  (leg COLLATE pg_catalog."default", movement_uid);

CREATE INDEX volumes_15min_tmc_volume_15min_tmc_uid_idx
  ON czhu.miovision_volumes_15min_tmc
  USING btree
  (volume_15min_tmc_uid);



-- INTERSECTION MOVEMENTS

CREATE TABLE czhu.intersection_movements AS (
    SELECT * FROM miovision_api.intersection_movements
);

WITH new_movements(movement_uid) AS (
         VALUES (7), (8)
), distinct_intersection_leg AS (
    SELECT DISTINCT intersection_uid,
           leg
    FROM czhu.intersection_movements    
)
INSERT INTO czhu.intersection_movements
SELECT a.intersection_uid,
       10 AS classification_uid,
       a.leg,
       b.movement_uid
FROM distinct_intersection_leg a
CROSS JOIN new_movements b
ORDER BY 1, 2, 3, 4;



-- FUNCTION TO AGGREGATE TO 15 MIN TMC.

CREATE OR REPLACE FUNCTION czhu.aggregate_15_min_tmc(
    start_date date,
    end_date date)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

BEGIN

WITH zero_padding_movements AS (
    -- Cross product of legal movement for cars, bikes, and peds and the bins to aggregate
    SELECT m.*, datetime_bin15
    FROM czhu.intersection_movements m
    CROSS JOIN generate_series(start_date - interval '1 hour', end_date - interval '1 hour 15 minutes', INTERVAL '15 minutes') AS dt(datetime_bin15)
    -- Make sure that the intersection is still active
    JOIN miovision_api.intersections mai USING (intersection_uid)
    -- Only include dates during which intersection is active.
    WHERE datetime_bin15::date > mai.date_installed AND (mai.date_decommissioned IS NULL OR (datetime_bin15::date < mai.date_decommissioned))
), aggregate_insert AS (
    INSERT INTO czhu.miovision_volumes_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
    SELECT pad.intersection_uid,
        pad.datetime_bin15 AS datetime_bin,
        pad.classification_uid,
        pad.leg,
        pad.movement_uid,
        CASE WHEN un.accept = FALSE THEN NULL ELSE (COALESCE(SUM(A.volume), 0)) END AS volume
    FROM zero_padding_movements pad
    --To set unacceptable ones to NULL instead (& only gap fill light vehicles,
    --cyclist/cyclist ATR and pedestrian)
    LEFT JOIN miovision_api.unacceptable_gaps un 
        ON un.intersection_uid = pad.intersection_uid
        AND pad.datetime_bin15 >= DATE_TRUNC('hour', gap_start)
        AND pad.datetime_bin15 < DATE_TRUNC('hour', gap_end) + interval '1 hour' -- may get back to this later on for fear of removing too much data
    --To get 1min bins
    LEFT JOIN czhu.miovision_volumes A
        ON A.datetime_bin >= start_date - INTERVAL '1 hour'
        AND A.datetime_bin < end_date - INTERVAL '1 hour'
        AND A.datetime_bin >= pad.datetime_bin15 
        AND A.datetime_bin < pad.datetime_bin15 + interval '15 minutes'
        AND A.intersection_uid = pad.intersection_uid 
        AND A.classification_uid = pad.classification_uid
        AND A.leg = pad.leg
        AND A.movement_uid = pad.movement_uid
    WHERE A.volume_15min_tmc_uid IS NULL
    GROUP BY pad.intersection_uid, pad.datetime_bin15, pad.classification_uid, pad.leg, pad.movement_uid, un.accept
    HAVING pad.classification_uid IN (1,2,6,10) OR SUM(A.volume) > 0
    RETURNING intersection_uid, volume_15min_tmc_uid, datetime_bin, classification_uid, leg, movement_uid, volume
)
--To update foreign key for 1min bin table
UPDATE czhu.miovision_volumes a
    SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
    FROM aggregate_insert b
    WHERE a.datetime_bin >= start_date - interval '1 hour' AND a.datetime_bin < end_date -  interval '1 hour'
    AND a.volume_15min_tmc_uid IS NULL AND b.volume > 0 
    AND a.intersection_uid  = b.intersection_uid 
    AND a.datetime_bin >= b.datetime_bin AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
    AND a.classification_uid  = b.classification_uid 
    AND a.leg = b.leg
    AND a.movement_uid = b.movement_uid
;

RAISE NOTICE '% Done aggregating to 15min TMC bin', timeofday();
END;

$BODY$;

ALTER FUNCTION czhu.aggregate_15_min_tmc(date, date)
    OWNER TO czhu;

GRANT EXECUTE ON FUNCTION czhu.aggregate_15_min_tmc(date, date) TO PUBLIC;

GRANT EXECUTE ON FUNCTION czhu.aggregate_15_min_tmc(date, date) TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION czhu.aggregate_15_min_tmc(date, date) TO miovision_admins;


-- NEW MOVEMENT MAP.

CREATE TABLE czhu.movement_map AS (
    SELECT * FROM miovision_api.movement_map
);

-- Upload new rows using
-- psql -U czhu -h 10.160.12.47 -d bigdata -c "\COPY czhu.movement_map FROM './movement_map_approach.csv' WITH (DELIMITER ',', FORMAT CSV, HEADER TRUE)";



-- CREATE ATR TABLE.

CREATE TABLE czhu.miovision_volumes_15min
(
  volume_15min_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  dir text,
  volume numeric,
  CONSTRAINT volumes_15min_pkey PRIMARY KEY (volume_15min_uid),
  CONSTRAINT volumes_15min_intersection_uid_datetime_bin_classification__key UNIQUE (intersection_uid, datetime_bin, classification_uid, leg, dir)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE czhu.miovision_volumes_15min
  OWNER TO czhu;
GRANT ALL ON TABLE czhu.miovision_volumes_15min TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE czhu.miovision_volumes_15min TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE czhu.miovision_volumes_15min TO bdit_humans WITH GRANT OPTION;


CREATE INDEX volumes_15min_classification_uid_idx
  ON czhu.miovision_volumes_15min
  USING btree
  (classification_uid);

CREATE INDEX volumes_15min_datetime_bin_idx
  ON czhu.miovision_volumes_15min
  USING btree
  (datetime_bin);

CREATE INDEX volumes_15min_intersection_uid_idx
  ON czhu.miovision_volumes_15min
  USING btree
  (intersection_uid);

CREATE INDEX volumes_15min_intersection_uid_leg_dir_idx
  ON czhu.miovision_volumes_15min
  USING btree
  (intersection_uid, leg COLLATE pg_catalog."default", dir COLLATE pg_catalog."default");

CREATE INDEX volumes_15min_volume_15min_uid_idx
  ON czhu.miovision_volumes_15min
  USING btree
  (volume_15min_uid);



-- CREATE XOVER TABLE

CREATE TABLE czhu.miovision_volumes_tmc_atr_xover
(
    volume_15min_tmc_uid integer,
    volume_15min_uid integer,
    CONSTRAINT atr_tmc_uid_volume_15min_tmc_uid_fkey FOREIGN KEY (volume_15min_tmc_uid)
        REFERENCES czhu.miovision_volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE CASCADE,
    CONSTRAINT atr_tmc_uid_volume_15min_uid_fkey FOREIGN KEY (volume_15min_uid)
        REFERENCES czhu.miovision_volumes_15min (volume_15min_uid) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE czhu.miovision_volumes_tmc_atr_xover
    OWNER to czhu;



-- CREATE 15 MIN ATR AGGREGATION FUNCTION

CREATE OR REPLACE FUNCTION czhu.aggregate_15_min(
    start_date date,
    end_date date)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
BEGIN
--Creates the ATR bins
    WITH transformed AS (
        SELECT     A.intersection_uid,
            A.datetime_bin,
            A.classification_uid,
            B.leg_new AS leg,
            B.dir,
            SUM(A.volume) AS volume,
            array_agg(volume_15min_tmc_uid) AS uids

        FROM czhu.miovision_volumes_15min_tmc A
        INNER JOIN czhu.movement_map B -- TMC to ATR crossover table.
        ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid 
        WHERE A.processed IS NULL
        AND datetime_bin >= start_date - INTERVAL '1 hour' AND datetime_bin < end_date - INTERVAL '1 hour' 
        -- each day is aggregated from 23:00 the day before to 23:00 of that day
        GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
    ),
    --Inserts the ATR bins to the ATR table
    insert_atr AS (
        INSERT INTO czhu.miovision_volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
        SELECT intersection_uid, datetime_bin, classification_uid, leg, dir, volume
        FROM transformed
        RETURNING volume_15min_uid, intersection_uid, datetime_bin, classification_uid, leg, dir)
    --Updates crossover table with new IDs
    , insert_crossover AS(
        INSERT INTO czhu.miovision_volumes_tmc_atr_xover (volume_15min_tmc_uid, volume_15min_uid)
        SELECT volume_15min_tmc_uid, volume_15min_uid
        FROM insert_atr A
        INNER JOIN (SELECT intersection_uid, datetime_bin, classification_uid, leg, dir, unnest(uids) AS volume_15min_tmc_uid FROM transformed) B
            ON A.intersection_uid=B.intersection_uid 
            AND A.datetime_bin=B.datetime_bin
            AND A.classification_uid=B.classification_uid 
            AND A.leg=B.leg
            AND A.dir=B.dir
        ORDER BY volume_15min_uid
        RETURNING volume_15min_tmc_uid
    )
    --Sets processed column to TRUE
    UPDATE czhu.miovision_volumes_15min_tmc a
    SET processed = TRUE
    FROM insert_crossover b 
    WHERE a.volume_15min_tmc_uid=b.volume_15min_tmc_uid;
    
    RETURN NULL;
EXCEPTION
    WHEN unique_violation THEN 
        RAISE EXCEPTION 'Attempting to aggregate data that has already been aggregated but not deleted';
        RETURN 0;
END;
$BODY$;


