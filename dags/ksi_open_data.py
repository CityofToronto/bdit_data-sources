r"""### Daily KSI Open Data Refresh DAG
Pipeline to refresh KSI data for open data and run data checks. 
"""
import os
import sys
import logging
import pendulum
from functools import partial
from datetime import datetime, timedelta

from airflow.sdk import dag, task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import Variable

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_owners import owners
    # import custom operators and helper functions
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, send_slack_msg
    from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'ksi_open_data'
DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"])

default_args = {'owner': ','.join(DAG_OWNERS),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2026, 1, 18, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': partial(task_fail_slack_alert, channel="slack_data_pipeline_dev")
                }

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["collision", "open_data"]
)

def ksi_opan_data():
    @task_group()
    def data_checks():

        check_deleted_collision = SQLExecuteQueryOperator(
            task_id="check_deleted_collision",
            sql='''
                    SELECT COUNT(*) = 0 AS _check, 'There are '|| count(*) ||' deleted collision_id comparing to last updated.  '||
                    '\n```'||ARRAY_TO_STRING(array_agg(collision_id), ', ')||'```' AS missing
                    FROM(
                    SELECT DISTINCT collision_id FROM open_data.ksi
                    except all
                    SELECT DISTINCT collision_id FROM natalie.ksi_open_data_Test) AS diff;
                    ''',
            conn_id="collisions_bot",
            do_xcom_push=True,
        )
        check_dup_collision = SQLExecuteQueryOperator(
            task_id="check_dup_collision",
            sql='''
                    SELECT COUNT(*) = 0 AS _check, 'There are '|| count(*) ||' deleted collision person pair comparing to last updated.  '||
                    '\n```'||ARRAY_TO_STRING(array_agg(collision_id), ', ')||'```' AS missing
                    FROM(
                    SELECT DISTINCT collision_id, per_no FROM open_data.ksi
                    except all
                    SELECT DISTINCT collision_id, per_no FROM natalie.ksi_open_data_Test) AS diff;
                    ''',
            conn_id="collisions_bot",
            do_xcom_push=True,
        )
        check_null_fatal_no = SQLExecuteQueryOperator(
            task_id="check_null_fatal_no",
            sql='''
                    SELECT COUNT(*) = 0 AS _check, 'There are '|| count(*) ||' of NULL fatal_no for fatals.  '||
                    '\n```'||ARRAY_TO_STRING(array_agg(collision_id), ', ')||'```' AS missing
                    FROM(
                    SELECT collision_id FROM natalie.ksi_open_data_Test WHERE injury = 'Fatal' AND fatal_no IS NULL) AS diff;
                    ''',
            conn_id="collisions_bot",
            do_xcom_push=True,
        )
        return check_deleted_collision, check_dup_collision, check_null_fatal_no
    
    check_deleted_collision, check_dup_collision, check_null_fatal_no = data_checks()

    @task
    def summarize_checks(*check_results):
        errors = []

        for results in check_results:
            ok, missing = results[0]
            # grab list of errored collisions
            if ok is False: 
                errors.append(missing)

        return {
            "has_errors": bool(errors),
            "details": "\n".join(errors) if errors else "No issues found."
        }

    checks_summary = summarize_checks(
        check_deleted_collision.output,
        check_dup_collision.output,
        check_null_fatal_no.output
    )

    refresh_ksi_staging = SQLExecuteQueryOperator(
        sql='''
            REFRESH MATERIALIZED VIEW CONCURRENTLY open_data_staging.ksi;
            ''',
        task_id='refresh_ksi_staging',
        conn_id='collisions_bot',
        autocommit=True,
        retries = 0
    )

    truncate_and_copy = SQLExecuteQueryOperator(
        sql='''
            TRUNCATE open_data.ksi;
            INSERT INTO open_data.ksi 
            SELECT * FROM open_data_staging.ksi;
            ''',
        task_id='truncate_and_copy',
        conn_id='collisions_bot',
        autocommit=True,
        retries = 0,
        # so it doesnt get skipped
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    @task()
    def checks_failed_message(summary: dict,  **context):
        slack_ids = Variable.get("slack_member_id", deserialize_json=True)
        owners = context.get('dag').owner.split(',')
        list_names = " ".join([slack_ids.get(name, name) for name in owners])
        ti = context["ti"]
        url = ti.log_url
        url = url.replace(
                'checks_failed_message', "approve_refresh/required_actions"
            )
        send_slack_msg(
            context=context,
            msg=(
                f"{list_names}:cat_yell: KSI open data checks failed, refresh paused for approval. Table will not be refreshed until manual approval.\n"
                f"{summary['details']}\n"
                f"Approve/reject *<{url}|here>*."
            ),
            channel="slack_data_pipeline_dev",
        )
    @task.branch
    def decide_approval(summary: dict):
        return "checks_failed_message" if summary["has_errors"] else "skip_approval"

    skip_approval = EmptyOperator(task_id="skip_approval")

    approve_refresh = ApprovalOperator(
        task_id="approve_refresh",
        subject="Refresh has been paused because of the following errors:",
        body=(
            "The following data checks failed:\n\n"
            "{{ ti.xcom_pull(task_ids='summarize_checks')['details'] }}"
            ),
        defaults="Approve",
        assigned_users=[
                {"id": "1", "name": "admin"},
                {"id": "4", "name": "natalie"},
                {"id": "44", "name": "dmcelroy"},
            ])


    @task()
    def status_message(**context):
        send_slack_msg(
            context=context,
            msg=f"KSI table successfully refreshed :white_check_mark:. ",
            channel='slack_data_pipeline_dev'
            )
        
    branch = decide_approval(checks_summary)

    refresh_ksi_staging >> [check_deleted_collision, check_dup_collision, check_null_fatal_no]  >> checks_summary >> branch

    # if no data checks failed
    # continue with refresh
    branch >> skip_approval >> truncate_and_copy
    # If data checks failed, prompt for approval
    # Then only refresh when approved
    branch >> checks_failed_message(checks_summary) >> approve_refresh >> truncate_and_copy

    truncate_and_copy >> status_message()


ksi_opan_data()


