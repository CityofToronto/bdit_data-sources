#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
r"""### The School Safety Zones DAG

This DAG runs daily to pull School Safety Zones (SSZ) from multiple google
sheets. Each sheet contains data of a single year.

> This is part of VZ DAGs.
"""
import os
import sys
import pendulum
from datetime import timedelta
from typing import Any
from sqlalchemy.engine import Engine
from googleapiclient.discovery import build
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# import custom operators and helper functions
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
# pylint: disable=wrong-import-position
from dags.dag_functions import task_fail_slack_alert
from dags.common_tasks import get_variable
from gis.school_safety_zones.schools import pull_from_sheet
# pylint: enable=wrong-import-position

DAG_NAME = 'vz_google_sheets'
DAG_OWNERS = Variable.get("dag_owners", deserialize_json=True).get(DAG_NAME, ["Unknown"])

DEFAULT_ARGS = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past' : False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': pendulum.datetime(2019, 9, 30, tz="America/Toronto"),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context':True,
    "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=5,
    max_active_tasks=5,
    schedule="@daily",
    doc_md=__doc__,
    tags=["Vision Zero"]
)
def get_vz_data():
    """The main function of the SSZ DAG."""
    
    @task()
    def pull_data(
        engine: Engine,
        service: Any,
        schema: str,
        spreadsheet: dict,
        **context
    ) -> None:
        """Loads SSZ data from Google Spreadsheets into the database.

        Args:
            engine: Connection to the PostgreSQL database.
            service: Resource for interacting with the Google API.
            schema: Name of schema to load the data into.
            spreadsheet: Dictionary of the year to be loaded, the spreadsheet
                ID, and the range of data within the spreadsheet.

        Returns:
            None.

        Raises:
            AirflowFailException: If any invalid record is found.
        """
        year = spreadsheet["year"]
        context["task_instance"].xcom_push(
            "extra_msg",
            f"Failed to pull {year} data. "
        )
        if not pull_from_sheet(
            engine=engine,
            service=service,
            year=year,
            spreadsheet_id=spreadsheet["spreadsheet_id"],
            spreadsheet_range=spreadsheet["spreadsheet_range"],
            schema=schema,
            table=f"school_safety_zone_{year}_raw",
        ):
            # push an extra failure message to be sent to Slack in case of failing
            context["task_instance"].xcom_push(
                "extra_msg",
                f"Found one or more invalid records in {year} data. "
                "Please, report this issue to the <mailto:VisionZeroTO@toronto.ca|Vision Zero Team>."
            )

            raise AirflowFailException('Invalid rows found. See errors documented above.')

    #to get credentials to access google sheets
    google_cred = GoogleBaseHook('vz_api_google').get_credentials()

    spreadsheets = get_variable.override(task_id="get_list_of_years")("ssz_spreadsheets")
    pull_data.partial(
        engine=PostgresHook("vz_api_bot").get_sqlalchemy_engine(),
        service=build('sheets', 'v4', credentials=google_cred, cache_discovery=False),
        schema="vz_safety_programs_staging"
    ).expand(
        spreadsheet=spreadsheets
    )

get_vz_data()
