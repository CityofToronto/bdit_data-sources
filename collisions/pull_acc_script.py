"""ACC dataset puller.

Pulls ACC.dat from the Flashcrow server and uploads it to collisions.ACC
"""

import click
import configparser
import subprocess
import datetime
import pathlib
import psycopg2
import logging


def logger():
    """Generic logger, from intersection_tmc.py."""

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s        %(levelname)s    %(message)s',
        datefmt='%d %b %Y %H:%M:%S')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


logger = logger()
logger.debug('Start')


@click.command()
@click.option('--cfgpath',
              help='The filepath of the configparser settings file.')
@click.option('--templocation',
              default='./',
              help='Filepath for temporarily storing ACC.dat.')
@click.option('--deletefile', default=True, type=bool,
              help="If True, delete ACC.dat after use.")
def pull_acc(cfgpath, templocation, deletefile):
    """Main function."""

    # Extract the location of the pem key.
    config = configparser.ConfigParser()
    config.read(cfgpath)
    flashcrow_settings = config['FLASHCROW']

    # Extract Postgres creds.
    postgres_settings = config['POSTGRES']

    # Full filepath of downloaded ACC.dat.
    accpath = str(pathlib.Path(templocation).joinpath('ACC.dat'))

    logger.info('Upload ACC.dat from Flashcrow to Postgres table.')
    acc_lc = download_acc(flashcrow_settings, templocation, accpath, logger)
    upload_acc(accpath, postgres_settings, logger, deletefile)

    logger.info('Processing derived matviews on Postgres.')
    process_acc(postgres_settings, logger)

    logger.info('Checking upload self-consistency.')
    check_tables(acc_lc, postgres_settings, logger)

    logger.info('Complete!')


def download_acc(flashcrow_settings, templocation, accpath, logger):
    """Downloads ACC.dat from Flashcrow."""

    logger.info('Downloading ACC.dat from Flashcrow server.')
    rsync_command = [
        '/usr/bin/rsync', '-Pav', '-e',
        'ssh -i {:s}'.format(flashcrow_settings['pem']),
        ('ec2-user@flashcrow-etl.intra.dev-toronto.ca:'
         '/data/replicator/flashcrow-CRASH/dat/ACC.dat'), templocation]
    # check_returncode raises an exception if the command failed.
    outcome = subprocess.run(rsync_command, stderr=subprocess.PIPE)
    try:
        outcome.check_returncode()
    except subprocess.CalledProcessError:
        logger.info('rsync encountered the following error while downloading'
                    'from the MOVE server:')
        logger.info(outcome.stderr)
        raise

    # To also delete columns, use """sed -i -r 's/\S+//{COLUMN NUMBER}' {FILE}"""
    # https://stackoverflow.com/questions/15361632/delete-a-column-with-awk-or-sed
    logger.info('Removing double-quotes from ACC.dat for '
                'compatibility with PSQL.')
    sed_command = ['/bin/sed', '-i', 's/"//g', accpath]
    outcome = subprocess.run(sed_command, stderr=subprocess.PIPE)
    try:
        outcome.check_returncode()
    except subprocess.CalledProcessError:
        logger.info('sed encountered the following error '
                    'while trimming ACC.dat:')
        logger.info(outcome.stderr)
        raise

    # Get number of lines in ACC.dat, which is used later for self-consistency
    # checks.
    with open(accpath) as f:
        acc_lc = sum(1 for _ in f)

    return acc_lc


def upload_acc(accpath, postgres_settings, logger, deletefile):
    """Uploads ACC.dat to collisions.acc."""

    # Ensure modified rows are replaced.
    logger.info('Truncating collisions.acc.')
    with psycopg2.connect(**postgres_settings) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE collisions.acc;")

    # Upload new data.
    logger.info('Uploading ACC.dat to Postgres.')
    upload_command = [
        '/usr/bin/psql', '-U', postgres_settings['user'], '-h',
        postgres_settings['host'], '-d', postgres_settings['dbname'], '-c',
        ('\\COPY collisions.acc FROM \'{0}\' WITH (DELIMITER E\'\\t\', NULL '
         '\'\\N\', FORMAT CSV, HEADER FALSE);').format(accpath)
    ]
    # check_returncode raises an exception if the command failed.
    outcome = subprocess.run(upload_command, stderr=subprocess.PIPE)
    try:
        outcome.check_returncode()
    except subprocess.CalledProcessError:
        logger.info('psql encountered the following error '
                    'while uploading to Postgres:')
        logger.info(outcome.stderr)
        raise

    with psycopg2.connect(**postgres_settings) as conn:
        with conn.cursor() as cur:
            # Mention date of latest upload.
            cur.execute("COMMENT ON TABLE collisions.acc IS "
                        "'Raw collision database, from Flashcrow "
                        "/data/replicator/flashcrow-CRASH/dat/ACC.dat. "
                        "Refreshed on {td:s}.';".format(
                            td=str(datetime.date.today())))

    if deletefile:
        logger.info('Deleting ACC.dat')
        outcome = subprocess.run(['/bin/rm', accpath], stderr=subprocess.PIPE)
        try:
            outcome.check_returncode()
        except subprocess.CalledProcessError:
            logger.info('rm encountered the following error '
                        'while deleting ACC.dat:')
            logger.info(outcome.stderr)
            raise


def process_acc(postgres_settings, logger):
    """Processes child materialized views of collisions.acc."""

    with psycopg2.connect(**postgres_settings) as conn:
        with conn.cursor() as cur:
            # Refresh materialized views.
            logger.info('Refreshing collisions.collision_no.')
            cur.execute("REFRESH MATERIALIZED VIEW collisions.collision_no;")
            cur.execute("COMMENT ON MATERIALIZED VIEW "
                        "collisions.collision_no IS "
                        "'Collision number to link involved and "
                        "events matviews. Refreshed on {td:s}.';".format(
                            td=str(datetime.date.today())))

            logger.info('Refreshing collisions.events.')
            cur.execute("REFRESH MATERIALIZED VIEW collisions.events;")
            cur.execute("COMMENT ON MATERIALIZED VIEW "
                        "collisions.events IS "
                        "'Event-level variables in collisions.acc. "
                        "Refreshed on {td:s}.';".format(
                            td=str(datetime.date.today())))

            logger.info('Refreshing collisions.involved.')
            cur.execute("REFRESH MATERIALIZED VIEW collisions.involved;")
            cur.execute("COMMENT ON MATERIALIZED VIEW "
                        "collisions.involved IS "
                        "'Individual-level variables in collisions.acc. "
                        "Refreshed on {td:s}.';".format(
                            td=str(datetime.date.today())))


def check_tables(acc_lc, postgres_settings, logger):
    """Basic self-consistency checks of processed tables."""

    # Processing self-consistency checks.
    with psycopg2.connect(**postgres_settings) as conn:
        with conn.cursor() as cur:
            # Number of lines in collisions.acc
            cur.execute("SELECT COUNT(*) total_lc FROM collisions.acc")
            pgacc_lc = cur.fetchone()[0]

            # Number of unique ACCNB/year.
            cur.execute(
                """WITH distinct_col AS (
                       SELECT DISTINCT "ACCNB",
                              EXTRACT(year FROM "ACCDATE"::date) accyear
                       FROM collisions.acc
                       WHERE "ACCDATE"::date >= '1985-01-01'::date
                           AND "ACCDATE"::date <= current_date
                   )
                   SELECT COUNT(*) FROM distinct_col""")
            n_col_events = cur.fetchone()[0]

            # Lines in collisions.collision_no.
            cur.execute("SELECT COUNT(*) FROM collisions.collision_no")
            colno_lc = cur.fetchone()[0]

            # Lines in collisions.events.
            cur.execute("SELECT COUNT(*) FROM collisions.events")
            n_events = cur.fetchone()[0]

            # Unique collision numbers in in collisions.involved.
            cur.execute("SELECT COUNT(DISTINCT collision_no) "
                        "FROM collisions.involved")
            n_colno_involved = cur.fetchone()[0]

            # Number of NaN collision numbers in events.
            cur.execute("SELECT COUNT(*) FROM collisions.events "
                        "WHERE collision_no IS NULL")
            n_events_nan = cur.fetchone()[0]

            # Number of NaN collision numbers in involved.
            cur.execute("SELECT COUNT(*) FROM collisions.involved "
                        "WHERE collision_no IS NULL")
            n_involved_nan = cur.fetchone()[0]

        if acc_lc != pgacc_lc:
            logger.warning("Number of lines in ACC.dat ({0}) differs "
                           "from number of lines in collisions.acc ({1})!"
                           .format(acc_lc, pgacc_lc))

        if n_col_events != colno_lc:
            logger.warning("Number of unique ACCNB/years in collisions.acc "
                           "({0}) differs from number of lines in "
                           "collisions.collision_no ({1})!"
                           .format(n_col_events, colno_lc))

        if n_events != colno_lc:
            logger.warning("Number of lines in collisions.events "
                           "({0}) differs from number of lines in "
                           "collisions.collision_no ({1})!"
                           .format(n_events, colno_lc))

        if n_events != n_colno_involved:
            logger.warning("Number of lines in collisions.events "
                           "({0}) differs from number of unique collision "
                           "numbers in collisions.involved ({1})!"
                           .format(n_events, colno_lc))

        if n_events_nan:
            logger.warning("Found {0} lines in collisions.events "
                           "without a collision number!"
                           .format(n_events_nan))

        if n_involved_nan:
            logger.warning("Found {0} lines in collisions.involved "
                           "without a collision number!"
                           .format(n_involved_nan))


if __name__ == '__main__':
    pull_acc()
