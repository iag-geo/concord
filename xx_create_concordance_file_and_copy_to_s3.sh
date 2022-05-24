#!/usr/bin/env bash

conda activate geo

AWS_PROFILE="default"
OUTPUT_FOLDER="/Users/$(whoami)/tmp/geoscape_202205"

mkdir -p "${OUTPUT_FOLDER}"

# Process using GDA94 boundaries
python3 /Users/$(whoami)/git/iag_geo/concord/create_concordance_file.py --output-path=${OUTPUT_FOLDER}
# sample arguments
#python3 /Users/$(whoami)/git/iag_geo/concord/create_concordance_file.py --admin-schema="admin_bdys_202205" --gnaf-schema="gnaf_202205" --output-path=${OUTPUT_FOLDER}

# copy concordance file to GDA94 & GDA2020 folders as GDA2020 would be the same as the GDA94 files
aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER} s3://minus34.com/opendata/geoscape-202205 --exclude "*" --include "*.csv" --acl public-read
aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER} s3://minus34.com/opendata/geoscape-202205-gda2020 --exclude "*" --include "*.csv" --acl public-read

# copy score file to GitHub repo local files
cp ${OUTPUT_FOLDER}/boundary_concordance_score.csv /Users/$(whoami)/git/iag_geo/concord/data/
