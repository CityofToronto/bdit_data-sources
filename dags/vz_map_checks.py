'''DAG to compare row counts for Vision Zero Map to staging tables to identify missing rows.'''

import os
import sys
import requests
import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

# DAG Information
DAG_NAME = 'vz_map_checks'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

#import python scripts
try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("script import failed")

def vz_row_count(schema, table_name):
    return SQLCheckOperatorWithReturnValue(
        task_id=f"{table_name}_row_count",
        sql=f"SELECT COUNT(1) FROM {schema}.{table_name};",
        conn_id="vz_api_bot",
    )

def get_arcserver_row_count(layer = None, where_clause = None):
    """
    Function to retreive row count from GCCView rest api.

    Parameters
    -----------
    layer: text
    where_clause: text
    
    Returns
    --------
    return_json : json
        Resulted json response from calling the GCCView rest api
    """    
    base_url = f"https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/{layer}/FeatureServer/0/query"
    query = {"where":f"safety_program_type='{where_clause}'",
             "outFields": "*",
             "outSR": '4326', 
             "returnGeometry": "false",
             "returnTrueCurves": "false",
             "returnIdsOnly": "false",
             "returnCountOnly": "true",
             "returnZ": "false",
             "returnM": "false",
             "orderByFields": "OBJECTID", 
             "returnDistinctValues": "false",
             "returnExtentsOnly": "false",
             "f":"json"}
    r = requests.get(base_url, params = query, verify = False)
    return_json = r.json() 
    return return_json

#DAG
default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2024, 6, 3, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    #'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    #on demand for now
    #schedule='0 8 * * *',
    catchup=False,
    tags=['Vision Zero'],
    doc_md=__doc__
)
def vz_checks_dag():
    @task()
    def speed_limits_gcc_row_count(upstream_row_count = None):
        return_json = get_arcserver_row_count('COTGEO_SAFETY_MEASURE_LINE', 'Approved Speed Limit Reductions')
        return return_json
    
    @task()
    def csz_gcc_row_count(upstream_row_count = None):
        return_json = get_arcserver_row_count('COTGEO_SAFETY_MEASURE_LINE', 'Approved Speed Limit Reductions')
        return return_json
    
    #get both row counts, then compare and notify
    csz_rc = vz_row_count('vz_safety_programs_staging', 'csz_signs_and_markings_tracking')
    csz_gcc_row_count(csz_rc)
    
    speed_limit_rc = vz_row_count('vz_safety_programs_staging', 'speed_limit_reductions_master')
    speed_limits_gcc_row_count(speed_limit_rc)

vz_checks_dag()