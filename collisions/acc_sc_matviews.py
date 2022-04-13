#This script should run after the MOVE dag dumps data into collisions_replicator."ACC"

# Operators; we need this to operate!
from airflow.operators.postgres_operator import PostgresOperator 
from psycopg2 import sql

#what follows is a stab in the dark and also preserving sql that...
#1) uses except to find the differences between this update and the last update
#2) stores the different records in a table called collisions_replicator.acc_upd, along with the update date (so you can see what records got updated when)
#3) uses truncate to delete all records from acc_safe_copy (it's hard to find fields without nulls for pk or unique combos so whaddya gonna do?)
#4) puts all the records from the ACC table into acc_safe_copy

update = sql.SQL('''with col_delta as (
	select * from collisions_replicator.acc_safe_copy
	except
	select * from collisions_replicator."ACC")

insert into collisions_replicator.acc_upd 
	(select 
	now()::date as update_date,
	* 
	from col_delta);

truncate table collisions_replicator.acc_safe_copy;

insert into collisions_replicator.acc_safe_copy (select * from collisions_replicator."ACC"''')



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

