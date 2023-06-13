from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('data_processing', default_args=default_args, schedule_interval='0 4 * * *') #daily at 4am

def pull_raw_data():
    # Pull raw data from Postgres database
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    raw_sql = f'''SELECT 
        d.divisionid,
        d.vdsid,
        d.timestamputc,
        d.lanedata
    FROM vdsdata AS d
    JOIN vdsconfig AS c ON
        d.vdsid = c.vdsid
        AND d.divisionid = c.divisionid
        AND to_timestamp(d.timestamputc) >= c.starttimestamputc
        AND (
            to_timestamp(d.timestamputc) <= c.endtimestamputc
            OR c.endtimestamputc IS NULL) --no end date
    JOIN EntityLocationLatest AS e ON
        c.vdsid = e.entityid
        AND c.divisionid = e.divisionid
    WHERE 
        timestamputc >= extract(epoch from timestamptz '{{ds}}')
        AND timestamputc < extract(epoch from timestamptz '{{ds}}') + 86400
        AND e.entityid IS NOT NULL --we only have locations for these ids
        AND d.divisionid = 2 --other is 8001 which are traffic signal detectors
        AND substring(sourceid, 1, 3) <> 'BCT' --bluecity.ai sensors; '''

    raw_data = pg_hook.get_records(raw_sql)
    
    # Transform raw data
    transformed_data = transform_function(raw_data)
        
    # Drop records for the current date
    drop_query = f"DELETE FROM rescu.raw_20sec WHERE date = '{{ds}}'"
    pg_hook.run(drop_query)
    
    # Insert cleaned data into the database
    insert_query = f"INSERT INTO rescu.raw_20sec VALUES {transformed_data}"
    pg_hook.run(insert_query)

pull_raw_data_task = PythonOperator(
    task_id='pull_raw_data',
    python_callable=pull_raw_data,
    dag=dag
)

def summarize_data():
    # Perform summarization logic on raw_20sec table
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    summarize_query = '''
        INSERT INTO rescu.volumes_15min (detector_id, datetime_bin, volume_15min)
        SELECT detector_id, datetime_bin, SUM(volume) AS volume_15min
        FROM rescu.raw_20sec
        WHERE datetime_bin::date = {{ds}}
        GROUP BY detector_id, datetime_bin
    '''
    pg_hook.run(summarize_query)

summarize_data_task = PythonOperator(
    task_id='summarize_data',
    python_callable=summarize_data,
    dag=dag
)

def pull_detector_inventory():
    # Pull data from the detector_inventory table
    detector_sql = '''SELECT 
        c.divisionid,
        c.vdsid,
        upper(c.sourceid) AS detector_id,
        c.starttimestamputc,
        c.endtimestamputc,
        c.lanes,
        c.hasgpsunit,
        c.managementurl,
        c.description,
        c.fssdivisionid,
        c.fssid,
        c.rtmsfromzone,
        c.rtmstozone,
        c.detectortype,
        c.createdby,
        c.createdbystaffid,
        c.signalid,
        c.signaldivisionid,
        c.movement,
        e.entitytype,                  
        e.locationtimestamputc,        
        e.latitude,                    
        e.longitude,                   
        e.altitudemetersasl,           
        e.headingdegrees,              
        e.speedkmh,                    
        e.numsatellites,               
        e.dilutionofprecision,         
        e.mainroadid,                  
        e.crossroadid,                 
        e.secondcrossroadid,           
        e.mainroadname,                
        e.crossroadname,               
        e.secondcrossroadname,         
        e.streetnumber,                
        e.offsetdistancemeters,        
        e.offsetdirectiondegrees,      
        e.locationsource,              
        e.locationdescriptionoverwrite
    FROM vdsconfig AS c 
    JOIN EntityLocationLatest AS e ON 
        c.vdsid = e.entityid 
        AND c.divisionid = e.divisionid 
    ORDER BY c.vdsid'''

    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    detector_data = pg_hook.get_records(detector_sql)
    
    # Transform and upsert data into the detector_inventory table
    transformed_data = detector_transform_function(detector_data)
    
    # Generate the upsert query
    upsert_query = '''
        INSERT INTO detector_inventory (column1, column2, column3)
        VALUES %s
        ON CONFLICT (column1) DO UPDATE
        SET column2 = EXCLUDED.column2, column3 = EXCLUDED.column3
    '''
    pg_hook.run(upsert_query, parameters=transformed_data)

pull_detector_inventory_task = PythonOperator(
    task_id='pull_and_upsert_detector_inventory',
    python_callable=pull_detector_inventory,
    dag=dag
)

pull_raw_data_task >> summarize_data_task >> pull_detector_inventory_task
