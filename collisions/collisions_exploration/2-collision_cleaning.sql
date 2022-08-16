--CLEANING TEXTUAL INFORMATION

---------------------------------------------------------------------------------------------------------------------------------------------------
----EXPLORE
---------------------------------------------------------------------------------------------------------------------------------------------------
--EXPLORE distinct streetypes
SELECT DISTINCT st FROM (
	SELECT streetype1 as st FROM rbahreh.col_orig 
	UNION 
	SELECT streetype2 as st FROM rbahreh.col_orig 
	UNION 
	SELECT streetype3 as st FROM rbahreh.col_orig ) AS res
	ORDER BY st;--179 unique streetype exist in the dataset
    
--EXPLORE distinct directions
SELECT array_agg(array[dir,frequency::text])
FROM (
	SELECT DISTINCT dir, count(dir) as frequency
	FROM (
		SELECT dir1 as dir FROM rbahreh.col_orig 
		UNION ALL
		SELECT dir2 as dir FROM rbahreh.col_orig 
		UNION ALL 
		SELECT dir3 as dir FROM rbahreh.col_orig) AS res
	GROUP BY dir
	ORDER BY dir) AS dir_freq;
	--8 different directions{{D,1},{e,17},{E,58534},{N,7494},{s,2},{S,5242},{w,8},{W,63632},{NULL,0}}
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
----INITIAL CAPITALISD
---------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE  rbahreh.col_orig 
SET stname1= initcap(trim(stname1)), streetype1 = initcap(trim(streetype1)), dir1 = initcap(trim(dir1)),
stname2 = initcap(trim(stname2)), streetype2 = initcap(trim(streetype2)), dir2 = initcap(trim(dir2)),
stname3 = initcap(trim(stname3)), streetype3 = initcap(trim(streetype3)), dir3 = initcap(trim(dir3));

---------------------------------------------------------------------------------------------------------------------------------------------------
----EXPLORE THE IMPACT
---------------------------------------------------------------------------------------------------------------------------------------------------
--Impact of the initcap on streetype
SELECT DISTINCT st FROM (
	SELECT streetype1 as st FROM rbahreh.col_orig 
	UNION 
	SELECT streetype2 as st FROM rbahreh.col_orig 
	UNION 
	SELECT streetype3 as st FROM rbahreh.col_orig ) AS res
	ORDER BY st;--decreased from 179 to 121 streetypes; it seems that some differences just was beacuse of formatting


--Impact of the initcap on dir
SELECT array_agg(array[dir,frequency::text])
FROM (
	SELECT DISTINCT dir, count(dir) as frequency
	FROM (
		SELECT dir1 as dir FROM rbahreh.col_orig 
		UNION ALL
		SELECT dir2 as dir FROM rbahreh.col_orig 
		UNION ALL 
		SELECT dir3 as dir FROM rbahreh.col_orig) AS res
	GROUP BY dir
	ORDER BY dir) AS dir_freq;--5 different directions {{D,1},{E,58551},{N,7494},{S,5244},{W,63640},{NULL,0}}
---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
----ABBREVIATION FUNCTION
---------------------------------------------------------------------------------------------------------------------------------------------------
--Some of the differences between streetypes are only beacuse of different ways of abbreviating.
--So I used an explorative approach, searched the streetypes and used a function to make them similar. 
--Some were not easy for me to diffrentiate, so I kept it as it is!

--I needed to change the Column Types, since some of abbreviation are bigger than character varying(4)
ALTER TABLE rbahreh.col_orig
ALTER COLUMN streetype1 TYPE TEXT;
ALTER TABLE rbahreh.col_orig
ALTER COLUMN streetype2 TYPE TEXT;
ALTER TABLE rbahreh.col_orig
ALTER COLUMN streetype3 TYPE TEXT;

--DROP TABLE rbahreh.col_orig_abbr;
CREATE TABLE rbahreh.col_orig_abbr As (
	SELECT collision_no, accnb, accyear, accdate, acctime, longitude, latitude, geom,
	stname1, rbahreh.abbr_streetype(streetype1) as streetype1, dir1,
	stname2, rbahreh.abbr_streetype(streetype2) as streetype2, dir2,
	stname3, rbahreh.abbr_streetype(streetype3) as streetype3, dir3, road_class,
	location_type, location_class, collision_type, impact_type, visibility, light,
	road_surface_cond, px, traffic_control, traffic_control_cond, on_private_property, description, data_source, ksi
	FROM rbahreh.col_orig);--runs in 4 min 19 secs.
---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
----EXPLORE THE IMPACT
---------------------------------------------------------------------------------------------------------------------------------------------------
--IMPACT on streetype
SELECT DISTINCT st FROM (
	SELECT streetype1 as st FROM rbahreh.col_orig_abbr 
	UNION 
	SELECT streetype2 as st FROM rbahreh.col_orig_abbr 
	UNION 
	SELECT streetype3 as st FROM rbahreh.col_orig_abbr) AS res
	ORDER BY st;
--The number of unique streetypes were down to 77 (from 121). 


SELECT array_agg(array[stype,frequency::text])
FROM (
	SELECT DISTINCT stype, COUNT(stype) as frequency
	FROM (
	SELECT streetype1 as stype FROM rbahreh.col_orig_abbr 
	UNION ALL
	SELECT streetype2 as stype FROM rbahreh.col_orig_abbr 
	UNION ALL
	SELECT streetype3 as stype FROM rbahreh.col_orig_abbr) AS res
	GROUP BY stype
	ORDER BY frequency
) AS stype_freq;
	/*{{NULL,0},{27,1},{Abe,1},{Ava,1},{Avr,1},{Ce,1},{Cir,1},{Ddr,1},{Dnw,1},{Gr,1},{Gs,1},
	{Hl,1},{Lawn,1},{Lwn,1},{Mill,1},{Mils,1},{Qy,1},{Rdst,1},{Rdwy,1},{Res,1},{Rve,1},{Sve,1},
	{Sy,1},{Tl,1},{Vast,1},{Vi,1},{Via,1},{West,1},{Wky,1},{Acre,2},{Dd,2},{Gree,2},{High,2},
	{R,2},{Sr,2},{"St W",2},{Ve,2},{Avd,3},{Ml,3},{Frnt,4},{Cr,5},{N,6},{Mall,8},{S,9},{Ct,10},
	{Brdg,13},{Pthwy,15},{Hwy,26},{E,36},{W,51},{Mews,55},{Crct,57},{Hts,72},{Grv,166},{Ter,185},
	{Ln,193},{Gdns,253},{Pk,308},{Quay,358},{Pkwy,368},{Lane,618},{Sq,752},{Crcl,781},{Way,850},
	{Circ,887},{Trl,1160},{Gt,1240},{Pl,1333},{Exp,1516},{Crt,1832},{Cres,5232},{Ramp,7431},
	{Blvd,18941},{Dr,30789},{Rd,100229},{St,116810},{Ave,180709}}*/
    
***************************************************************************************************************************************************    
--*****NOTE: *****
--The streetype with only less than 10 occourences require review,
--Some of other types also requires to be reviewed and formatted as single type. 
--There are also some directions in types that can be corrected
***************************************************************************************************************************************************    

---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
----CHECK THE Collision with dir=D
---------------------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM rbahreh.col_orig_abbr WHERE dir1 ='D' or dir2 ='D' or dir3 ='D';--collision_no=1721464
--It seems the problem is, Rd from streetype was imported as R in streetype and D as dir2

UPDATE rbahreh.col_orig_abbr
SET streetype2 = 'Rd', dir2 = NULL
WHERE collision_no=1721464;
---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
----CHECK THE st as digit
---------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *  FROM rbahreh.col_orig_abbr WHERE streetype1='27' OR streetype2='27' OR streetype3='27'; --collision_no = 1517737

UPDATE rbahreh.col_orig_abbr
SET stname2 = 'Highway 27', streetype2='Hwy'
WHERE collision_no=1517737;
---------------------------------------------------------------------------------------------------------------------------------------------------
