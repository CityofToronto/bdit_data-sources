#!
# -*- coding: utf-8 -*-
"""This file contains custom Airflow SQL operators.

This file contains the following custom operators:

1. SQLCheckOperatorWithReturnValue: extends the functionalities of the original
   ``SQLCheckOperator``.
"""
import psycopg2
import os
import sys
from functools import partial

from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

dags_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, dags_path)
from dag_functions import task_fail_slack_alert

class SQLCheckOperatorWithReturnValue(SQLCheckOperator):
    """A custom Airflow SQLCheckOperator that extends the original operator.

    This custom operator Overrides the ``execute`` function of the
    ``SQLCheckOperator`` to:

    1. Only fail if the first element (not any element) in the returning result
       is False,
    2. Return the result of the final query, and
    3. Enable autocommit

    """
    def __init__(self, *args, on_failure_callback=None, **kwargs):
        # By default, send slack alerts from this operator to data quality channel
        default_failure_callback = partial(
            task_fail_slack_alert, channel='slack_data_pipeline_data_quality'
        )
        # Use the default callback if none is provided
        super().__init__(*args,
            on_failure_callback=on_failure_callback or default_failure_callback, **kwargs)
    def execute(self, context: Context = None):
        """Executes the Airflow operator.

        It runs the given sql query to check its result. It also returns the
        result of that query and commit any changes this query might have
        enforced.

        Args:
          context:
            The execution context as inherited from the parent class.

        Returns:
          A list of boolean if any records were found and an integer indicating
          the number of records resulting from the SQL query. For example:

          [True, 5]

        Raises:
          AirflowException: An error occurred during running the SQL query.
        """
        self.log.info("Executing SQL check: %s", self.sql)
        # Fetch the first row of the sql's output and commit changes
        try:
            hook = self.get_db_hook()
            records = hook.run(
                sql=self.sql,
                handler=lambda cursor: cursor.fetchone(),
                autocommit=True,
            )
        except psycopg2.Error as e:
            context.get("task_instance").xcom_push(key="extra_msg", value=str(e))
            raise AirflowException(e)
            
        self.log.info("Record: %s", records)
        if not records:
            raise AirflowException("The query returned None")
        if not bool(records[0]):  # checking first element only
            context.get("task_instance").xcom_push(key="extra_msg", value=records[1:])
            raise AirflowException(
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}"
            )

        self.log.info("Success.")
        return records
