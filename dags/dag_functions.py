#!
# -*- coding: utf-8 -*-
"""Common functions used in most of the DAGs."""
import os
import re
import json
from typing import Optional, Callable, Any, Union
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def is_prod_mode() -> bool:
    """Returns True if the code is running from the PROD ENV directory."""
    PROD_ENV_PATH = Variable.get("prod_env_path")
    dags_folder = os.path.dirname(os.path.realpath(__file__))
    repo_folder = os.path.basename(os.path.dirname(dags_folder))
    return repo_folder == PROD_ENV_PATH

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
            notification (default '', which forces the function to look for any
            XCom with key ``extra_msg`` returned from the failing task;
            otherwise, no extra message is added to the standard one).
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

    task_instance = context["task_instance"]
    slack_ids = Variable.get("slack_member_id", deserialize_json=True)
    owners = context.get('dag').owner.split(',')
    list_names = " ".join([slack_ids.get(name, name) for name in owners])
    # get the extra message from the calling task, if provided
    extra_msg_from_task = task_instance.xcom_pull(
            task_ids=task_instance.task_id,
            map_indexes=task_instance.map_index,
            key="extra_msg"
        )

    if callable(extra_msg):
        # in case of function
        extra_msg_str = extra_msg(context)
    elif extra_msg == "" and extra_msg_from_task is not None:
        # in case the the extra message is passed from inside the task via xcom
        extra_msg_str = extra_msg_from_task
    else:
        # in case of a string (or the default empty string)
        extra_msg_str = extra_msg

    if isinstance(extra_msg_str, tuple) or isinstance(extra_msg_str, list):
        #recursively collapse extra_msg_str's which are in the form of a list with new lines.
        extra_msg_str = '\n'.join(
            ['\n'.join(item) if isinstance(item, list) else item for item in extra_msg_str]
        )

    # Slack failure message
    if use_proxy:
        # Temporarily accessing Airflow on Morbius through 8080 instead of Nginx
        # Its hould be eventually removed
        log_url = task_instance.log_url.replace(
            "localhost", task_instance.hostname + ":8080"
        )
        # get the proxy credentials from the Airflow connection ``slack``. It
        # contains username and password to set the proxy <username>:<password>
        proxy=(
            f"http://{BaseHook.get_connection('slack').password}"
            f"@{json.loads(BaseHook.get_connection('slack').extra)['url']}"
        )
    else:
        log_url = task_instance.log_url.replace(
            "localhost", task_instance.hostname
        )
        proxy = None
    slack_msg = (
        f":red_circle: {task_instance.dag_id}."
        f"{task_instance.task_id} "
        f"({context.get('ts_nodash_with_tz')}) FAILED.\n"
        f"{list_names}, please, check the <{log_url}|logs>\n"
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
        attachments=[{"text": str(extra_msg_str)}],
        proxy=proxy,
    )
    return failed_alert.execute(context=context)

def get_readme_docmd(readme_path, dag_name):
    """Extracts a DAG doc_md from a .md file using html comments tags.

    Args:
        readme_path: An aboslute path to the .md file to extract from. 
        dag_name: The name of the DAG, matching the html comment in the .md
        file to extract from. The html comment should be in the format
        '<!-- dag_name_doc_md -->' before and after the relevant section. 
    """

    contents = open(readme_path, 'r').read()
    doc_md_key = '<!-- ' + dag_name + '_doc_md -->'
    doc_md_regex = '(?<=' + doc_md_key + '\n)[\s\S]+(?=\n' + doc_md_key + ')'
    return re.findall(doc_md_regex, contents)[0]

def send_slack_msg(
    context: dict,
    msg: str,
    attachments: Optional[list] = None,
    blocks: Optional[list] = None,
    use_proxy: Optional[bool] = False,
    dev_mode: Optional[bool] = None
) -> Any:
    """Sends a message to Slack.

    Args:
        context: The calling Airflow task's context.
        msg : A string message be sent to Slack.
        slack_conn_id: ID of the Airflow connection with the details of the
            Slack channel to send messages to.
        attachments: List of dictionaries representing Slack attachments.
        blocks: List of dictionaries representing Slack blocks.
        use_proxy: A boolean to indicate whether to use a proxy or not. Proxy
            usage is required to make the Slack webhook call on on-premises
            servers (default False).
        dev_mode: A boolean to indicate if working in development mode to send
            Slack alerts to data_pipeline_dev instead of the regular 
            data_pipeline (default None, to be determined based on the location
            of the file).
    """
    if dev_mode or (dev_mode is None and not is_prod_mode()):
        SLACK_CONN_ID = "slack_data_pipeline_dev"
    else:
        SLACK_CONN_ID = "slack_data_pipeline"

    if use_proxy:
        # get the proxy credentials from the Airflow connection ``slack``. It
        # contains username and password to set the proxy <username>:<password>
        proxy=(
            f"http://{BaseHook.get_connection('slack').password}"
            f"@{json.loads(BaseHook.get_connection('slack').extra)['url']}"
        )
    else:
        proxy = None

    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=msg,
        username="airflow",
        attachments=attachments,
        blocks=blocks,
        proxy=proxy,
    )
    return slack_alert.execute(context=context)