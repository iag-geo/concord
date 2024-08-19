#!/usr/bin/env bash

conda activate geo

AWS_PROFILE="minus34"
OUTPUT_FOLDER="/Users/$(whoami)/tmp/geoscape_202408"
OUTPUT_FOLDER_2020="/Users/$(whoami)/tmp/geoscape_202408_gda2020"

mkdir -p "${OUTPUT_FOLDER}"
mkdir -p "${OUTPUT_FOLDER_2020}"

# Process using GDA94 boundaries
python3 /Users/$(whoami)/git/iag_geo/concord/create_concordance_file.py --pgdb=geo --output-path=${OUTPUT_FOLDER}
# Process using GDA2020 boundaries
python3 /Users/$(whoami)/git/iag_geo/concord/create_concordance_file.py --pgdb=geo --admin-schema="admin_bdys_202408_gda2020" --gnaf-schema="gnaf_202408_gda2020" --output-path=${OUTPUT_FOLDER_2020}

# copy concordance files to GDA94 & GDA2020 folders as GDA2020 would be the same as the GDA94 files
aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER} s3://minus34.com/opendata/geoscape-202408 --exclude "*" --include "*.csv" --acl public-read
aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER_2020} s3://minus34.com/opendata/geoscape-202408-gda2020 --exclude "*" --include "*.csv" --acl public-read

# copy GDA94 score file to GitHub repo local files
cp ${OUTPUT_FOLDER}/boundary_concordance_score.csv /Users/$(whoami)/git/iag_geo/concord/data/
