"""
airflow_log_cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs
to avoid those getting too big.
"""
# pylint: disable=pointless-statement,anomalous-backslash-in-string
import os
import sys
import pendulum
from airflow import DAG

AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'tasks')
AIRFLOW_TASKS_LIB = os.path.join(AIRFLOW_TASKS, 'lib')

from airflow.configuration import conf
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable 

dag_name = 'log_cleanup'

# Slack alert
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    


def create_dag(filepath, doc, start_date, schedule_interval):
    """
    Creates an Airflow DAG with our default parameters.
    You should call this from within your DAG file as follows:
        import airflow_utils
        dag = airflow_utils.create_dag(__file__, __doc__, start_date,   schedule_interval)
        with dag:
          # initialize tasks
          pass
    This function imposes the convention that a DAG file with name
    `my_dag.py` has ID `my_dag`.
    """
    default_args = {
      # When on, `depends_on_past` freezes progress if a previous run failed.
      # This isn't ideal for our use case, so we disable it here.
      'depends_on_past': False,
      'owner': ','.join(names),
      'start_date': start_date,
      'on_failure_callback': task_fail_slack_alert
    }
  
    dag = DAG(
      dag_id = dag_name,
      default_args=default_args,
      # This avoids Airflow's default catchup behavior, which can be surprising.
      # Since our pipelines tend to operate non-incrementally, turning this off
      # makes more sense.
      catchup=False,
      # Prevent the same DAG from running concurrently more than once.
      max_active_runs=1,
      schedule=schedule_interval,
      # This allows us to simplify `create_bash_task` below.
      template_searchpath=AIRFLOW_TASKS
    )
    # Use the module docstring to generate documentation for the Airflow DAG.
    dag.doc_md = doc
    return dag
    
START_DATE = pendulum.datetime(2020, 2, 25, tz="America/Toronto")
SCHEDULE_INTERVAL = '@daily'
DAG = create_dag(__file__, __doc__, START_DATE, SCHEDULE_INTERVAL)

BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER")
MAX_LOG_AGE_IN_DAYS = 30    # Number of days to retain the log files
ENABLE_DELETE = True

DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]

LOG_CLEANUP = """
echo "Getting Configurations..."
BASE_LOG_FOLDER="{{params.directory}}"
TYPE="{{params.type}}"
MAX_LOG_AGE_IN_DAYS='""" + str(MAX_LOG_AGE_IN_DAYS) + """'
ENABLE_DELETE=""" + str("true" if ENABLE_DELETE else "false") + """
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"
echo "TYPE:                 '${TYPE}'"

echo ""
echo "Running Cleanup Process..."
if [ $TYPE == file ];
then
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
    DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"
else
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"
fi
echo "Executing Find Statement: ${FIND_STATEMENT}"
FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`
echo "Process will be Deleting the following File(s)/Directory(s):"
echo "${FILES_MARKED_FOR_DELETE}"
echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l` File(s)/Directory(s)"     # "grep -v '^$'" - removes empty lines. "wc -l" - Counts the number of lines
echo ""
if [ "${ENABLE_DELETE}" == "true" ];
then
    if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
    then
        echo "Executing Delete Statement: ${DELETE_STMT}"
        eval ${DELETE_STMT}
        DELETE_STMT_EXIT_CODE=$?
        if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
            echo "Delete process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
            exit ${DELETE_STMT_EXIT_CODE}
        fi
    else
        echo "WARN: No File(s)/Directory(s) to Delete"
    fi
else
    echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
fi
echo "Finished Running Cleanup Process"
"""
for i, directory in enumerate(DIRECTORIES_TO_DELETE):
  log_cleanup_file_op = BashOperator(
    task_id='log_cleanup_file_' + str(i),
    bash_command=LOG_CLEANUP,
    params={"directory": str(directory), "type": "file"},
    dag=DAG
  )

  log_cleanup_dir_op = BashOperator(
    task_id='log_cleanup_directory_' + str(i),
    bash_command=LOG_CLEANUP,
    params={"directory": str(directory), "type": "directory"},
    dag=DAG
  )

  log_cleanup_file_op >> log_cleanup_dir_op