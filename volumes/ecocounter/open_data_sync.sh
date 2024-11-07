#! /bin/bash

set -euo pipefail
# shellcheck disable=SC1091

# activate python virtual environment
source "$HOME"/move/bin/activate

# Add ssh keys for environment
eval "$(ssh-agent -s)"
chmod 600 ~/creds/*
ssh-add ~/creds/*

# Synchronize file system with that on the AWS ETL machine
echo "sending files to server: $1"

# We need to add variables here for both source and destination to airflow config variable
# dev and qa should have a dummy endpoint
rsync -azq ec2-user@"$1":/data/open_data/tmcs/* "$2"

echo "Task Complete"
