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
from dateutil.parser import parse
from googleapiclient.discovery import build
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 
from airflow.models.param import Param
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# import custom operators and helper functions
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
# pylint: disable=wrong-import-position
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
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
    params={
        "years": Param(
            default=[0],
            type="array",
            title="An array of years to pull.",
            description="Override the default years to pull (current and last) by passing an array of years. ie. [2019, 2020, 2021]",
            items={"type": "number"},
        )
    },
    tags=["Vision Zero", "google_sheets"]
)
def get_vz_data():
    """The main function of the SSZ DAG."""
    
    @task(map_index_template="{{ task_year }}")
    def pull_data(
        schema: str,
        spreadsheet: dict,
        **context
    ) -> None:
        """Loads SSZ data from Google Spreadsheets into the database.

        Args:
            schema: Name of schema to load the data into.
            spreadsheet: Dictionary of the year to be loaded, the spreadsheet
                ID, and the range of data within the spreadsheet.

        Returns:
            None.

        Raises:
            AirflowFailException: If any invalid record is found.
        """
        #get name for mapped task 
        from airflow.sdk import get_current_context
        context = get_current_context()
        context["task_year"] = spreadsheet["year"]
        
        #to get credentials to access google sheets, database
        google_cred = GoogleBaseHook('google_sheets_api').get_credentials()
        service=build('sheets', 'v4', credentials=google_cred, cache_discovery=False)
        engine=PostgresHook("vz_api_bot").get_sqlalchemy_engine()
        
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
    
    """Gets list of spreadsheets to process.
    
    Pulls from Airflow variable `ssz_spreadsheets`. 
    By default filters to only current year and previous year based on execution date.
    Can override by triggering with DAG Parameters."""
    @task(task_id = "get_list_of_years")
    def filter_spreadsheets(ds = None, **context):
        spreadsheets = Variable.get("ssz_spreadsheets", deserialize_json=True)
        
        YR = parse(str(ds)).year
        if context["params"]["years"] == [0]:
            #defaults to current year and previous year if not specified as a param
            years = (YR-1, YR)
        else:
            years = tuple(context["params"]["years"])
        
        sheets = []
        for item in spreadsheets:
            try:
                if item['year'] in years:
                    sheets.append(item)
            except KeyError:
                raise AirflowFailException('Year key not found.')
        if sheets == []:
            raise AirflowFailException('No sheets found.')
        
        return sheets
    
    pull_data.partial(
        schema="vz_safety_programs_staging"
    ).expand(
        spreadsheet=filter_spreadsheets()
    )

get_vz_data()
