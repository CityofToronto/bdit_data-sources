#!/bin/bash

set -eu
# shellcheck disable=SC2046
# shellcheck disable=SC2086
cd $(dirname $0)

psql -v ON_ERROR_STOP=1 -U airflow -h localhost -p 5432 flashcrow < create_signal_query_tables/create_signal_query_tables.sql

# Somehow implement this:
# https://github.com/CityofToronto/bdit_flashcrow/blob/master/scripts/replication/replicator-local.ps1#L249
