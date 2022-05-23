#!/usr/bin/env bash

AWS_PROFILE="default"
OUTPUT_FOLDER="/Users/$(whoami)/tmp/geoscape_202202"

mkdir -p "${OUTPUT_FOLDER}"

# Process using GDA94 boundaries
python3 /Users/$(whoami)/git/iag_geo/concord/create_concordance_file.py --output-path=${OUTPUT_FOLDER}

# copy to GDA94 & GDA2020 folders as GDA2020 would be the same as the GDA94 files
aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER} s3://minus34.com/opendata/geoscape-202202 --exclude "*" --include "*.csv" --acl public-read
aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER} s3://minus34.com/opendata/geoscape-202202-gda2020 --exclude "*" --include "*.csv" --acl public-read

