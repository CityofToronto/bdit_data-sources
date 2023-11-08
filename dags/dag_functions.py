#!
# -*- coding: utf-8 -*-
"""Common functions used in most of the DAGs."""
import os
from typing import Optional, Callable, Any, Union
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def is_prod_mode() -> bool:
    """Returns True if the code is running from the PROD ENV directory."""
    PROD_ENV_PATH = Variable.get("prod_env_path")
    dags_folder = os.path.dirname(os.path.realpath(__file__))
    repo_folder = os.path.abspath(os.path.dirname(dags_folder))
    return os.path.normpath(repo_folder) == os.path.normpath(PROD_ENV_PATH)

def task_fail_slack_alert(
    context: dict,
    extra_msg: Optional[Union[str, Callable[..., str]]] = "",
    use_proxy: Optional[bool] = False,
    dev_mode: Optional[bool] = None
) -> Any:
    """Sends Slack task-failure notifications.

    Failure callback function to send notifications to Slack upon the failure
    of an Airflow task.

    Example:
        This function can be passed as a failure callback to DAG's default_args
        like this::

            import sys
            import os
            import pendulum
            from functools import partial
            from airflow import DAG
            from airflow.operators.bash import BashOperator
            
            repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
            sys.path.insert(0, repo_path)
            from dags.dag_functions import task_fail_slack_alert
            
            with DAG(
                dag_id = dag_name,
                start_date=pendulum.datetime(2023, 8, 28, tz="America/Toronto")
            ) as dag:
                t = BashOperator(
                    task_id="failing_task",
                    bash_command="exit 1",
                    on_failure_callback= partial(
                        task_fail_slack_alert, extra_msg="My custom message"
                    )
                )

    Args:
        context: The calling Airflow task's context
        extra_msg: An extra string message or a function that
            generates an extra message to be appended to the default
            notification (default '').
        use_proxy: A boolean to indicate whether to use a proxy or not. Proxy
            usage is required to make the Slack webhook call on on-premises
            servers (default False).
        dev_mode: A boolean to indicate if working in development mode to send
            Slack alerts to data_pipeline_dev instead of the regular 
            data_pipeline (default None, to be determined based on the location
            of the file).
    
    Returns:
        Any: The result of executing the SlackWebhookOperator.
    """
    if dev_mode or (dev_mode is None and not is_prod_mode()):
        SLACK_CONN_ID = "slack_data_pipeline_dev"
    else:
        SLACK_CONN_ID = "slack_data_pipeline"

    slack_ids = Variable.get("slack_member_id", deserialize_json=True)
    owners = context.get('dag').owner.split(',')
    list_names = " ".join([slack_ids.get(name, name) for name in owners])

    if callable(extra_msg):
        extra_msg_str = extra_msg(context)
    else:
        extra_msg_str = extra_msg

    # Slack failure message
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    if use_proxy:
        # Temporarily accessing Airflow on Morbius through 8080 instead of Nginx
        # Its hould be eventually removed
        log_url = context.get("task_instance").log_url.replace(
            "localhost", context.get("task_instance").hostname + ":8080"
        )
        # get the proxy credentials from the Airflow connection ``slack``. It
        # contains username and password to set the proxy <username>:<password>
        proxy=(
            f"http://{BaseHook.get_connection('slack').password}"
            f"@{json.loads(BaseHook.get_connection('slack').extra)['url']}"
        )
    else:
        log_url = context.get("task_instance").log_url.replace(
            "localhost", context.get("task_instance").hostname
        )
        proxy = None
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
        proxy=proxy,
    )
    return failed_alert.execute(context=context)
