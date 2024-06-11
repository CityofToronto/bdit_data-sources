
from psycopg2 import sql, Error
from typing import Tuple
import logging
import datetime
# pylint: disable=import-error
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.sensors.time_sensor import TimeSensor

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
def get_variable(var_name:str) -> list:
    """Returns an Airflow variable.
    
    Args:
        var_name: The name of the Airflow variable.
    
    Returns:
        A list of two-element lists. Each list consists of a full name of the
            source table to be copied and the destination table.
    """
    return Variable.get(var_name, deserialize_json=True)

@task()
def copy_table(conn_id:str, table:Tuple[str, str], **context) -> None:
    """Copies ``table[0]`` table into ``table[1]`` after truncating it.

    Args:
        conn_id: The name of Airflow connection to the database
        table: A tuple containing the source table to be copied in the format
            ``schema.table``, and the destination table in the same format
            ``schema.table``.
    """
    # separate tables and schemas
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

    LOGGER.info(f"Copying {table[0]} to {table[1]}.")

    con = PostgresHook(conn_id).get_conn()
    # truncate the destination table
    truncate_query = sql.SQL(
        "TRUNCATE {}.{}"
        ).format(
            sql.Identifier(dst_schema), sql.Identifier(dst_table)
        )
    # get the column names of the source table
    source_columns_query = sql.SQL(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s;"
        )
    # copy the table's comment, extended with additional info (source, time)
    comment_query = sql.SQL(
        r"""
            DO $$
            DECLARE comment_ text;
            BEGIN
                SELECT obj_description('{}.{}'::regclass)
                    || 'Copied from {}.{} by bigdata repliactor DAG at '
                    || to_char(now() AT TIME ZONE 'EST5EDT', 'yyyy-mm-dd HH24:MI') || '.' INTO comment_;
                EXECUTE format('COMMENT ON TABLE {}.{} IS %L', comment_);
            END $$;
        """
        ).format(
            sql.Identifier(src_schema), sql.Identifier(src_table),
            sql.Identifier(src_schema), sql.Identifier(src_table),
            sql.Identifier(dst_schema), sql.Identifier(dst_table),
        )
    
    try:
        with con, con.cursor() as cur:
            # truncate the destination table
            cur.execute(truncate_query)
            # get the column names of the source table
            cur.execute(source_columns_query, [src_schema, src_table])
            src_columns = [r[0] for r in cur.fetchall()]
            # copy all the data
            insert_query = sql.SQL(
                "INSERT INTO {}.{} ({}) SELECT {} FROM {}.{}"
                ).format(
                    sql.Identifier(dst_schema), sql.Identifier(dst_table),
                    sql.SQL(', ').join(map(sql.Identifier, src_columns)),
                    sql.SQL(', ').join(map(sql.Identifier, src_columns)),
                    sql.Identifier(src_schema), sql.Identifier(src_table)
                )
            cur.execute(insert_query)
            # copy the table's comment
            cur.execute(comment_query)
    #catch psycopg2 errors:
    except Error as e:
        # push an extra failure message to be sent to Slack in case of failing
        context["task_instance"].xcom_push(
            key="extra_msg",
            value=f"Failed to copy `{table[0]}` to `{table[1]}`: `{str(e).strip()}`."
        )
        raise AirflowFailException(e)

    LOGGER.info(f"Successfully copied {table[0]} to {table[1]}.")

@task.short_circuit(ignore_downstream_trigger_rules=False, retries=0) #only skip immediately downstream task
def check_jan_1st(ds=None): #check if Jan 1 to trigger partition creates. 
    from datetime import datetime
    start_date = datetime.strptime(ds, '%Y-%m-%d')
    if start_date.month == 1 and start_date.day == 1:
        return True
    return False

@task.short_circuit(ignore_downstream_trigger_rules=False, retries=0) #only skip immediately downstream task
def check_1st_of_month(ds=None): #check if 1st of Month to trigger partition creates. 
    from datetime import datetime
    start_date = datetime.strptime(ds, '%Y-%m-%d')
    if start_date.day == 1:
        return True
    return False

@task.short_circuit(ignore_downstream_trigger_rules=False, retries=0) #only skip immediately downstream task
def check_if_dow(isodow, ds):
    """Use to check if it's a specific day of week to trigger a weekly check.
    Uses isodow: Monday (1) to Sunday (7)"""
    
    from datetime import datetime
    assert (isodow >= 1 and isodow <= 7)

    start_date = datetime.strptime(ds, '%Y-%m-%d')
    return start_date.isoweekday() == isodow

def wait_for_weather_timesensor(timeout=8*3600):
    """Use to delay a data check until after yesterdays weather is available."""
    wait_for_weather = TimeSensor(
        task_id="wait_for_weather",
        timeout=timeout,
        mode="reschedule",
        poke_interval=3600,
        target_time=datetime.time(hour = 8, minute = 5)
    )
    wait_for_weather.doc_md = """
    Historical weather is pulled at 8:00AM daily through the `weather_pull` DAG.
    Use this sensor to have a soft link to that DAG.
    """
    return wait_for_weather