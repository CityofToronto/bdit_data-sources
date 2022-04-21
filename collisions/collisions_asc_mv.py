#This script should run after the MOVE dag dumps data into collisions_replicator."ACC"

# Operators; we need this to operate!
from airflow.operators.postgres_operator import PostgresOperator 
from psycopg2 import sql

#what follows is a stab in the dark and also preserving sql that...
#1) uses except to find the differences between this update and the last update
#2) stores the different records in a table called collisions_replicator.acc_upd, along with the update date (so you can see what records got updated when)
#3) uses truncate to delete all records from acc_safe_copy (it's hard to find fields without nulls for pk or unique combos so whaddya gonna do?)
#4) puts all the records from the ACC table into acc_safe_copy

update = sql.SQL('''insert into collisions_replicator.acc_safe_copy (
	select * from collisions_replicator."ACC"
	except
	select * from collisions_replicator.acc_safe_copy)
	on conflict ("REC_ID") 
	DO UPDATE 
		SET "ACCNB" = EXCLUDED."ACCNB",
			"RELACCNB" = EXCLUDED."RELACCNB",
    		"ACCDATE" = EXCLUDED."ACCDATE",	
    		"DAY_NO" = EXCLUDED."DAY_NO",
    		"ACCTIME" = EXCLUDED."ACCTIME",
   			"PATAREA" = EXCLUDED."PATAREA",
		    "STNAME1" = EXCLUDED."STNAME1",
		    "STREETYPE1" = EXCLUDED."STREETYPE1",
		    "DIR1" = EXCLUDED."DIR1",
    		"STNAME2" = EXCLUDED."STNAME2",
    		"STREETYPE2" = EXCLUDED."STREETYPE2",
    		"DIR2" = EXCLUDED."DIR2",
		    "STNAME3" = EXCLUDED."STNAME3",
		    "STREETYPE3" = EXCLUDED."STREETYPE3",
		    "DIR3" = EXCLUDED."DIR3",
		    "PER_INV" = EXCLUDED."PER_INV",
		    "VEH_INV" = EXCLUDED."VEH_INV",
		    "MUNICIPAL" = EXCLUDED."MUNICIPAL",
		    "LOCCOORD" = EXCLUDED."LOCCOORD",
		    "IMPCTAREA" = EXCLUDED."IMPCTAREA",
		    "ACCLASS" = EXCLUDED."ACCLASS",
		    "ACCLOC" = EXCLUDED."ACCLOC",
		    "TRAFFICTL" = EXCLUDED."TRAFFICTL",
		    "DRIVAGE" = EXCLUDED."DRIVAGE",
		    "VEH_NO" = EXCLUDED."VEH_NO",
		    "VEHTYPE" = EXCLUDED."VEHTYPE",
		    "TOWEDVEH" = EXCLUDED."TOWEDVEH",
		    "INITDIR" = EXCLUDED."INITDIR",
		    "IMPACTYPE" = EXCLUDED."IMPACTYPE",
		    "IMPLOC" = EXCLUDED."IMPLOC",
		    "EVENT1" = EXCLUDED."EVENT1",
		    "EVENT2" = EXCLUDED."EVENT2",
		    "EVENT3" = EXCLUDED."EVENT3",
		    "PER_NO" = EXCLUDED."PER_NO",
		    "INVTYPE" = EXCLUDED."INVTYPE",
		    "INVAGE" = EXCLUDED."INVAGE",
		    "INJURY" = EXCLUDED."INJURY",
		    "SAFEQUIP" = EXCLUDED."SAFEQUIP",
		    "DRIVACT" = EXCLUDED."DRIVACT",
		    "DRIVCOND" = EXCLUDED."DRIVCOND",
		    "PEDCOND" = EXCLUDED."PEDCOND",
		    "PEDACT" = EXCLUDED."PEDACT",
		    "CHARGE" = EXCLUDED."CHARGE",
			"CHARGE2" = EXCLUDED."CHARGE2",
		    "CHARGE3" = EXCLUDED."CHARGE3",
		    "CHARGE4" = EXCLUDED."CHARGE4",
		    "VISIBLE" = EXCLUDED."VISIBLE",
		    "LIGHT" = EXCLUDED."LIGHT",
		    "RDSFCOND" = EXCLUDED."RDSFCOND",
		    "VEHIMPTYPE" = EXCLUDED."VEHIMPTYPE",
		    "MANOEUVER" = EXCLUDED."MANOEUVER",
		    "ENTRY" = EXCLUDED."ENTRY",
		    "GEOCODE" = EXCLUDED."GEOCODE",
		    "FACTOR_ERR" = EXCLUDED."FACTOR_ERR",
		    "REP_TYPE" = EXCLUDED."REP_TYPE",
		    "BADGE_NO" = EXCLUDED."BADGE_NO",
		    "POSTAL" = EXCLUDED."POSTAL",
		    "XCOORD" = EXCLUDED."XCOORD",
		    "YCOORD" = EXCLUDED."YCOORD",
		    "PRECISE_XY" = EXCLUDED."PRECISE_XY",
		    "LONGITUDE" = EXCLUDED."LONGITUDE",
		    "LATITUDE" = EXCLUDED."LATITUDE",
		    "CHANGED" = EXCLUDED."CHANGED" ,
		    "SENT_UNIT" = EXCLUDED."SENT_UNIT",
		    "SENT_DATE" = EXCLUDED."SENT_DATE",
		    "STATUS" = EXCLUDED."STATUS",
		    "CITY_AREA" = EXCLUDED."CITY_AREA",
		    "USER_ID" = EXCLUDED."USER_ID",
		    "CRC_UNIT" = EXCLUDED."CRC_UNIT",
		    "COMMENTS" = EXCLUDED."COMMENTS",
		    "MTP_DIVISION" = EXCLUDED."MTP_DIVISION",
		    "POLICE_AGENCY" = EXCLUDED."POLICE_AGENCY",
		    "SUBMIT_BADGE_NUMBER" = EXCLUDED."SUBMIT_BADGE_NUMBER",
		    "SUBMIT_DATE" = EXCLUDED."SUBMIT_DATE",
		    "BIRTHDATE" = EXCLUDED."BIRTHDATE",
		    "PRIVATE_PROPERTY" = EXCLUDED."PRIVATE_PROPERTY",
		    "PERSON_ID" = EXCLUDED."PERSON_ID",
		    "USERID" = EXCLUDED."USERID",
		    "TS" = EXCLUDED."TS",
		    "ROAD_CLASS" = EXCLUDED."ROAD_CLASS",
		    "SYMBOL_NUM" = EXCLUDED."SYMBOL_NUM",
		    "ROTATION_NUM" = EXCLUDED."ROTATION_NUM",
		    "PX" = EXCLUDED."PX",
		    "DISTRICT" = EXCLUDED."DISTRICT",
		    "QUADRANT" = EXCLUDED."QUADRANT",
		    "FAILTOREM" = EXCLUDED."FAILTOREM",
		    "YEAR" = EXCLUDED."YEAR",
		    "REC_ID" = EXCLUDED."REC_ID",
		    "PEDTYPE" = EXCLUDED."PEDTYPE",
		    "CYCLISTYPE" = EXCLUDED."CYCLISTYPE",
		    "SIDEWALKCYCLE" = EXCLUDED."SIDEWALKCYCLE",
		    "CYCACT" = EXCLUDED."CYCACT",
		    "CYCCOND" = EXCLUDED."CYCCOND",
		    "MVAIMG" = EXCLUDED."MVAIMG",
		    "WARDNUM" = EXCLUDED."WARDNUM",
		    "FATAL_NO" = EXCLUDED."FATAL_NO",
		    "DESCRIPTION" = EXCLUDED."DESCRIPTION",
		    "TAB_REPORT" = EXCLUDED."TAB_REPORT",
		    "ACTUAL_SPEED" = EXCLUDED."ACTUAL_SPEED",
		    "POSTED_SPEED" = EXCLUDED."POSTED_SPEED",
		    "TRAFCTLCOND" = EXCLUDED."TRAFCTLCOND";

with delrec as (select * from collisions_replicator.acc_safe_copy 
	except 
	select * from collisions_replicator."ACC")

Delete from collisions_replicator.acc_safe_copy where "REC_ID" in (SELECT "REC_ID" from delrec)''')



refresh_col_no = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_mat_view_collisions_no()'
				task_id = 'refresh_col_no'
				postgres_conn_id = 'replicator_bot'
				autocommit = True
				retries = 0
				dag = 'BDITTO_ACC_REPLICATOR' #how does this even work if it's separated from the BDITTO_ACC_REPLICATOR definition? Clearly I'm missing something...
    )
refresh_events_involved = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_mat_view_collisions_no()'
				task_id = 'refresh_col_no'
				postgres_conn_id = 'replicator_bot'
				autocommit = True
				retries = 0
				dag = 'BDITTO_ACC_REPLICATOR' #ibid
    )     

