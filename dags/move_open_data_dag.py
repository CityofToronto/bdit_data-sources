from datetime import datetime, timedelta
from textwrap import dedent
import pendulum
import os

from airflow.models import Variable
from airflow.decorators import dag, task

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

# Toronto timezone - important to make sure cron schedules are in the right timezone!
LOCAL_TZ = pendulum.timezone('America/Toronto')
START_DATE = datetime(2022, 3, 1)
start_date_local = LOCAL_TZ.convert(START_DATE)

SCRIPT_DIR =  os.path.dirname(os.path.realpath(__file__))
REPOSITORY = SCRIPT_DIR.split('/')[-2]

# prod and dev env will have this folder structure qa has its own
if(REPOSITORY == 'airflow-dags'):
  ENV_CONFIGS = Variable.get('opendata_config')
  env_dict: dict = eval(str(ENV_CONFIGS))
else:
  ENV_CONFIGS = Variable.get('opendata_config_qa')
  env_dict: dict = eval(str(ENV_CONFIGS))

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
  'owner': 'move',
  'depends_on_past': False,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=5),
  'wait_for_downstream': True,
}
def create_dag(dag_id, schedule, server, endpoint, tags):
  @dag(
    dag_id = dag_id,
    default_args=default_args,
    description='Copies data from AWS endpoint and places it in the OpenData team directory',
    schedule=schedule,
    start_date=start_date_local,
    catchup=False,
    tags=tags,
    max_active_runs = 1,
    max_active_tasks = 1
  )
  def open_data_sync():
    rsync_output = BashOperator(
      task_id='rsync_output',
      depends_on_past=True,
      bash_command='$HOME/{}/scripts/open_data_sync.sh {} {}'.format(REPOSITORY, server, endpoint),
      retries=3,
    )

    rsync_output
  return open_data_sync()

for env, values in env_dict.items():
  job = values.get("job")
  dag_id = "opendata_{}_{}".format(job, env)
  schedule = values.get("schedule")
  server = values.get("server")
  endpoint = values.get("endpoint")
  tags = [job, env]
  create_dag(dag_id, schedule, server, endpoint, tags)