
from psycopg2 import sql
from typing import Tuple
import logging 
# pylint: disable=import-error
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@task.sensor(poke_interval=3600, timeout=3600*24, mode="reschedule")
def wait_for_external_trigger(**kwargs) -> PokeReturnValue:
    """Waits for an external trigger.
    
    A sensor waiting to be triggered by an external trigger. Its default poke
        interval is 1 hour and timeout is 1 day. The sensor's mode is set to
        reschedule by default to free the resources while idle.
    """
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
    
    LOGGER.info(f"Successfully copied {table[0]} to {table[1]}.")