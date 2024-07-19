DROP TABLE IF EXISTS gis.traffic_signals;
CREATE TABLE gis.traffic_signals (
PX int primary key ,
Main text ,
Mid_Block text ,
Side_1 text ,
Side_2 text ,
Private_Access text ,
Additional_info text ,
Geo_id bigint not null,
Node_id bigint not null,
X numeric,
Y numeric ,
Latitude text ,
Longitude text ,
Activation_date DATE,
Signal_System text ,
Non_system text ,
Mode_of_Control text ,
Pedestrian_Walk_speed text ,
APS_Signal smallint,
APS_Operation text ,
APS_Activation_date DATe,
Transit_Preempt BOOLEAN,
Fire_preempt BOOLEAN,
Rail_preempt BOOLEAN,
Signalized_Approaches smallint ,
UPS BOOLEAN,
LED_Blankout BOOLEAN,
lpi BOOLEAN ,
Bicycle_Signal BOOLEAN
);
GRANT SELECT ON gis.traffic_signals TO bdit_humans;

ALTER TABLE gis.traffic_signals ADD COLUMN geom geometry(Point, 4326);
UPDATE gis.traffic_signals SET geom = ST_SetSRID(ST_MakePoint(latitude::numeric, longitude::numeric), 4326);