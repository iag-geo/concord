#!/usr/bin/env bash

AWS_PROFILE="default"
FILE_PATH="/Users/$(whoami)/git/iag_geo/concord/data"

aws --profile=${AWS_PROFILE} s3 sync ${FILE_PATH} s3://minus34.com/opendata/geoscape-202202 --exclude "*" --include "*.csv" --acl public-read
