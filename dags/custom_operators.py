from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.exceptions import AirflowException


class SQLCheckOperatorWithReturnValue(SQLCheckOperator):
    """
    Overrides the `execute` function of `SQLCheckOperator` to:
    1. Only fail if the first element (not any element) in the returning result is False
    2. return the result of the final query
    3. enable autocommit
    """

    def execute(self, context=None):
        self.log.info("Executing SQL check: %s", self.sql)
        # Fetch the first row of the sql's output and commit changes
        hook = self.get_db_hook()
        records = hook.run(
            sql=self.sql, handler=lambda cursor: cursor.fetchone(), autocommit=True
        )
        # records = self.get_db_hook().get_first(self.sql)

        self.log.info("Record: %s", records)
        if not records:
            raise AirflowException("The query returned None")
        elif not bool(records[0]):  # checking first element only
            raise AirflowException(
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}"
            )

        self.log.info("Success.")
        return records