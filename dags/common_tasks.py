
from psycopg2 import sql
from typing import Tuple
# pylint: disable=import-error
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException

@task.sensor(poke_interval=3600, timeout=3600*24, mode="reschedule")
def wait_for_upstream(**kwargs) -> PokeReturnValue:
    """Waits for an external trigger."""
    return PokeReturnValue(
        is_done=kwargs["task_instance"].dag_run.external_trigger
    )

@task()
def copy_table(table:Tuple[str, str]) -> None:
    """Copies ``table[0]`` table into ``table[1]`` after truncating it.

    Args:
        table: A tuple containing the source table to be copied in the format
            ``schema.table``, and the destination table in the same format
            ``schema.table``.
    """
    try:
        src_schema, src_table = table[0].split(".")
    except ValueError:
        raise AirflowFailException(
            f"Invalid source table (expected schema.table, got {table[0]})"
        )
    try:
        dst_schema, dst_table = table[1].split(".")
    except ValueError:
        raise AirflowFailException(
            f"Invalid destination table (expected schema.table, got {table[1]})"
        )

    con = PostgresHook("collisions_bot").get_conn()
    truncate_query = sql.SQL(
        "TRUNCATE {}.{}"
        ).format(
            sql.Identifier(dst_schema), sql.Identifier(dst_table)
        )
    insert_query = sql.SQL(
        "INSERT INTO {}.{} SELECT * FROM {}.{}"
        ).format(
            sql.Identifier(dst_schema), sql.Identifier(dst_table),
            sql.Identifier(src_schema), sql.Identifier(src_table)
        )
    with con, con.cursor() as cur:
        cur.execute(truncate_query)
        cur.execute(insert_query)
