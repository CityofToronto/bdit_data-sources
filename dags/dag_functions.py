#!
# -*- coding: utf-8 -*-
"""Common functions used in most of the DAGs."""
import os
import re
import json
import logging
from typing import Optional, Callable, Any, Union
from functools import partial
from psycopg2 import sql, Error

from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def is_prod_mode() -> bool:
    """Returns True if the code is running from the PROD ENV directory."""
    PROD_ENV_PATH = Variable.get("prod_env_path")
    dags_folder = os.path.dirname(os.path.realpath(__file__))
    repo_folder = os.path.basename(os.path.dirname(dags_folder))
    return repo_folder == PROD_ENV_PATH

def slack_channel(channel: Optional[str] = None) -> str:
    "Returns a slack channel ID"
    if not is_prod_mode(): #always send to dev
        return "slack_data_pipeline_dev"
    
    valid_channels = [
        'slack_data_pipeline', 'slack_data_pipeline_dev', 'slack_data_pipeline_data_quality'
    ]
    if channel in valid_channels:
        return channel
    if channel is not None and channel not in valid_channels:
        LOGGER.warning(f"Channel {channel} is not valid. Defaulting to `slack_data_pipeline`.")
    return "slack_data_pipeline"

def task_fail_slack_alert(
    context: dict,
    extra_msg: Optional[Union[str, Callable[..., str]]] = "",
    use_proxy: Optional[bool] = False,
    channel: Optional[str] = None,
    emoji: Optional[str] = ':large_red_square:'
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
            from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
            
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
        channel: ID of the Airflow connection with the details of the
            Slack channel to send messages to.
    
    Returns:
        Any: The result of executing the SlackWebhookNotifier.
    """
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

    #recursively join list/tuple extra_msg_str into string
    if isinstance(extra_msg_str, (list, tuple)):
        extra_msg_str = '\n> '.join(
            ['\n> '.join(item) if isinstance(item, (list, tuple)) else str(item) for item in extra_msg_str]
        )

    # Slack failure message
    proxy = None
    if use_proxy:
        # get the proxy credentials from the Airflow connection ``slack``. It
        # contains username and password to set the proxy <username>:<password>
        proxy=(
            f"http://{BaseHook.get_connection('slack').password}"
            f"@{json.loads(BaseHook.get_connection('slack').extra)['url']}"
        )
    log_url = task_instance.log_url.replace(
        "localhost", task_instance.hostname
    )
    
    slack_msg = (
        f"{emoji} {task_instance.dag_id}."
        f"{task_instance.task_id} "
        f"({context.get('ts_nodash_with_tz')}) FAILED.\n"
        f"{list_names}, please, check the <{log_url}|logs>\n"
    )
    
    if extra_msg_str != "":
        slack_msg = slack_msg + extra_msg_str

    notifier = SlackWebhookNotifier(
        slack_webhook_conn_id=slack_channel(channel),
        text=slack_msg,
        proxy=proxy,
    )
    notifier.notify(context=context)

slack_alert_data_quality = partial(
    task_fail_slack_alert,
    channel="slack_data_pipeline_data_quality",
    emoji=":large_yellow_square:"
)

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
    doc_md_regex = '(?<=' + doc_md_key + '\\n)[\\s\\S]+(?=\\n' + doc_md_key + ')'
    try:
        doc_md = re.findall(doc_md_regex, contents)[0]
    except IndexError: #soft fail without breaking DAG.
        doc_md = "doc_md not found in {readme_path}. Looking between {doc_md_key} tags."
    return doc_md

def send_slack_msg(
    context: dict,
    msg: str,
    attachments: Optional[list] = None,
    blocks: Optional[list] = None,
    use_proxy: Optional[bool] = False,
    channel: Optional[str] = None
) -> Any:
    """Sends a message to Slack.

    Args:
        context: The calling Airflow task's context.
        msg : A string message be sent to Slack.
        attachments: List of dictionaries representing Slack attachments.
        blocks: List of dictionaries representing Slack blocks.
        use_proxy: A boolean to indicate whether to use a proxy or not. Proxy
            usage is required to make the Slack webhook call on on-premises
            servers (default False).
        channel: ID of the Airflow connection with the details of the
            Slack channel to send messages to.
    """
    if use_proxy:
        # get the proxy credentials from the Airflow connection ``slack``. It
        # contains username and password to set the proxy <username>:<password>
        proxy=(
            f"http://{BaseHook.get_connection('slack').password}"
            f"@{json.loads(BaseHook.get_connection('slack').extra)['url']}"
        )
    else:
        proxy = None

    notifier = SlackWebhookNotifier(
        slack_webhook_conn_id=slack_channel(channel),
        text=msg,
        attachments=attachments,
        blocks=blocks,
        proxy=proxy,
    )
    notifier.notify(context=context)

def check_not_empty(context: dict, conn_id:str, table:str) -> None:
    con = PostgresHook(conn_id).get_conn()
    sch, tbl = table.split(".")
    check_query = sql.SQL("SELECT True FROM {}.{} LIMIT 1;").format(sql.Identifier(sch), sql.Identifier(tbl))
    try:
        with con.cursor() as cur:
            # check for non-empty table
            LOGGER.info(f"Checking for rows in {table}.")
            cur.execute(check_query)
            check = cur.fetchone()
            if check is None:
                context["task_instance"].xcom_push(
                    key="extra_msg",
                    value=f"`{table}` is empty. Copying not completed."
                )
                raise AirflowFailException(f"`{table}` is empty. Copying not completed.")
    #catch psycopg2 errors:
    except Error as e:
        # push an extra failure message to be sent to Slack in case of failing
        context["task_instance"].xcom_push(
            key="extra_msg",
            value=f"Failed to check `{table}` non-empty: `{str(e).strip()}`."
        )
        raise AirflowFailException(e)