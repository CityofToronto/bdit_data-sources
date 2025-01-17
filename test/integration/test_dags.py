#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
"""This file contains basic integration tests for DAGs.
Example:
    pytest -q test/integration/test_dags.py
    pytest -v test/integration/test_dags.py::test_file_imports
"""
import os
from typing import Tuple
import re
from datetime import datetime
import pytest
from unittest import mock
from airflow.models import DagBag
from airflow.models.connection import Connection


# CONSTANTS
DAG_FOLDER = "dags"
IGNORED_DAGS = "(?:{})".format("|".join(["^test.*"]))

# Mocking Airflow variables & connections
AIRFLOW_VARIABLES = {
    'AIRFLOW_VAR_DAG_OWNERS': '{"var": "value"}',
    'AIRFLOW_VAR_SLACK_MEMBER_ID': '{"var": "value"}',
    'AIRFLOW_VAR_SSZ_SPREADSHEET_IDS': "{"+",".join([f'"ssz{year}": "value"' for year in range(2018, datetime.now().year+1)])+"}",
    'AIRFLOW_VAR_COLLISIONS_TABLES': "["+",".join([f'["src_schema.table_{i}", "dst_schema.table_{i}"]' for i in range(0, 2)])+"]",
    'AIRFLOW_VAR_COUNTS_TABLES': "["+",".join([f'["src_schema.table_{i}", "dst_schema.table_{i}"]' for i in range(0, 3)])+"]",
    'AIRFLOW_VAR_HERE_DAG_TRIGGERS': "["+",".join([f'"dag_{i}"' for i in range(0, 3)])+"]",
    'AIRFLOW_VAR_REPLICATORS': '{"dag": {"dag_name": "value", "tables": "value", "conn": "value"}}',
    'AIRFLOW_VAR_TEST_DAG_TRIGGERS': "["+",".join([f'"dag_{i}"' for i in range(0, 3)])+"]",
    'AIRFLOW_VAR_GCC_DAGS': '{"dag": {"conn": "value", "deployments": ["value"]}}',
}
SAMPLE_CONN = Connection(
    conn_type="sample_type",
    login="cat",
    host="localhost",
)
AIRFLOW_CONNECTIONS = {
    'AIRFLOW_CONN_MIOVISION_API_BOT': SAMPLE_CONN.get_uri(),
    'AIRFLOW_CONN_HERE_BOT': SAMPLE_CONN.get_uri(),
    'AIRFLOW_CONN_RESCU_BOT': SAMPLE_CONN.get_uri(),
    'AIRFLOW_CONN_VZ_API_BOT': SAMPLE_CONN.get_uri(),
    'AIRFLOW_CONN_GOOGLE_SHEETS_API': SAMPLE_CONN.get_uri(),
    'AIRFLOW_CONN_ECOCOUNTER_BOT': SAMPLE_CONN.get_uri(),
}


@mock.patch.dict(
    'os.environ',
    AIRFLOW_VARIABLES | AIRFLOW_CONNECTIONS
)
@mock.patch("psycopg2.connect")
@mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials")
def get_dagbag(mock_connect, mock_google_hook):
    return DagBag(include_examples=False, dag_folder=DAG_FOLDER)


dag_bag = get_dagbag()


def get_dags() -> Tuple[str, str]:
    """Returns all DAG IDs and objects.
    Returns:
        A tuple of DAG ID and the DAG object.
    """
    return [
        (k, v) for k, v in dag_bag.dags.items() if not re.match(IGNORED_DAGS, k)
    ]


def get_import_errors() -> Tuple[str, str]:
    """Returns import errors in the dag bag.
    Returns:
        A tuple of DAG file name and the import error (if any exists).
    """
    return [(None, None)] + [
        (os.path.realpath(k), v.strip())
        for k, v in dag_bag.import_errors.items()
    ]


@pytest.mark.parametrize(
    "dag_path,msg", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(dag_path: str, msg: str) -> None:
    """Looks for import errors in a DAG file."""
    if dag_path and msg:
        raise Exception(f"{dag_path} failed to import with message \n {msg}")


@pytest.mark.parametrize(
    "dag_id,dag", get_dags(), ids=[x[0] for x in get_dags()]
)
def test_empty_dags(dag_id, dag):
    """Checks if the DAG ``dag_id`` has at least one task."""
    assert len(dag.tasks) > 0, f"{dag_id} has no tags."