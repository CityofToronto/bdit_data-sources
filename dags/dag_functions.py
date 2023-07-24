#!
# -*- coding: utf-8 -*-
"""Common functions used in most of the DAGs."""
from typing import Optional, Callable, Any, Union
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = "slack_data_pipeline"

def task_fail_slack_alert(
    context: dict, extra_msg: Optional[Union[str, Callable[..., str]]] = ""
) -> Any:
    """Sends Slack task-failure notifications.

    Failure callback function to send notifications to Slack upon the failure
    of an Airflow task.

    Example:
        This function can be passed as a failure callback to DAG's default_args
        like this::

            import sys
            import os
            from functools import partial
            from airflow import DAG
            from airflow.operators.bash import BashOperator
            
            repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
            sys.path.insert(0, repo_path)
            from dags.dag_functions import task_fail_slack_alert
            
            with DAG(dag_id = dag_name) as dag:
                t = BashOperator(
                    task_id="failing_task",
                    bash_command="exit 1",
                    on_failure_callback= partial(
                                task_fail_slack_alert, extra_msg="extra_failure_msg"
                            )
                )

    Args:
        context: The calling Airflow task's context
        extra_msg: An extra string message or a function that
            generates an extra message to be appended to the default
            notification (default '').
    
    Returns:
        Any: The result of executing the SlackWebhookOperator.
    """
    slack_ids = Variable.get("slack_member_id", deserialize_json=True)
    owners = context.get('dag').owner.split(',')
    list_names = " ".join([slack_ids.get(name, name) for name in owners])

    if callable(extra_msg):
        extra_msg_str = extra_msg(context)
    else:
        extra_msg_str = extra_msg

    # Slack failure message
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    log_url = context.get("task_instance").log_url
    slack_msg = (
        f":red_circle: {context.get('task_instance').dag_id}."
        f"{context.get('task_instance').task_id} "
        f"({context.get('ts_nodash_with_tz')}) FAILED.\n"
        f"{list_names}, please, check the <{log_url}|logs>\n"
        f"{extra_msg_str}"
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return failed_alert.execute(context=context)
