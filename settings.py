# takes the command line parameters and creates a dictionary of setting_dict

import os
import argparse
import platform
import psycopg2
import sys

from datetime import datetime


# get latest Geoscape release version as YYYYMM, as of the date provided, as well as the prev. version 3 months prior
def get_geoscape_version(date):
    month = date.month
    year = date.year

    if month == 1:
        gs_version = str(year - 1) + "11"
        previous_gs_version = str(year - 1) + "08"
    elif 2 <= month < 5:
        gs_version = str(year) + "02"
        previous_gs_version = str(year - 1) + "11"
    elif 5 <= month < 8:
        gs_version = str(year) + "05"
        previous_gs_version = str(year) + "02"
    elif 8 <= month < 11:
        gs_version = str(year) + "08"
        previous_gs_version = str(year) + "05"
    else:
        gs_version = str(year) + "11"
        previous_gs_version = str(year) + "08"

    return gs_version, previous_gs_version


# get python, psycopg2 and OS versions
python_version = sys.version.split("(")[0].strip()
psycopg2_version = psycopg2.__version__.split("(")[0].strip()
os_version = platform.system() + " " + platform.version().strip()

# get the command line arguments for the script
parser = argparse.ArgumentParser(
    description="A CSV file and supporting scripts for converting data between Australian boundaries.")

# PG Options
parser.add_argument(
    "--pghost",
    help="Host name for Postgres server. Defaults to PGHOST environment variable if set, otherwise localhost.")
parser.add_argument(
    "--pgport", type=int,
    help="Port number for Postgres server. Defaults to PGPORT environment variable if set, otherwise 5432.")
parser.add_argument(
    "--pgdb",
    help="Database name for Postgres server. Defaults to PGDATABASE environment variable if set, "
         "otherwise geoscape.")
parser.add_argument(
    "--pguser",
    help="Username for Postgres server. Defaults to PGUSER environment variable if set, otherwise postgres.")
parser.add_argument(
    "--pgpassword",
    help="Password for Postgres server. Defaults to PGPASSWORD environment variable if set, "
         "otherwise \"password\".")

# schema names for the raw gnaf, flattened reference and admin boundary tables
geoscape_version, previous_geoscape_version = get_geoscape_version(datetime.today())
parser.add_argument(
    "--geoscape-version", default=geoscape_version,
    help="Geoscape release version number as YYYYMM. Defaults to latest release year and month \""
         + geoscape_version + "\".")
# parser.add_argument(
#     "--previous-geoscape-version", default=previous_geoscape_version,
#     help="Previous Geoscape release version number as YYYYMM; used for QA comparison. "
#          "Defaults to \"" + previous_geoscape_version + "\".")
parser.add_argument(
    "--gnaf-schema",
    help="Input schema name to store final GNAF tables in. Also the output schema for the concordance table."
         "Defaults to \"gnaf_" + geoscape_version + "\".")
parser.add_argument(
    "--admin-schema",
    help="Input schema name to store final admin boundary tables in. Defaults to \"admin_bdys_"
         + geoscape_version + "\".")
# parser.add_argument(
#     "--previous-gnaf-schema",
#     help="Schema with previous version of GNAF tables in. Defaults to \"gnaf_" + previous_geoscape_version + "\".")
# parser.add_argument(
#     "--previous-admin-schema",
#     help="Schema with previous version of GNAF tables in. Defaults to \"admin_bdys_"
#          + previous_geoscape_version + "\".")

# output file/table name & directory
parser.add_argument(
    "--output-table",
    help="Name of both output concordance table and file. Defaults to 'boundary_concordance'.")
parser.add_argument(
    "--output-score_table",
    help="Name of both output concordance QA table and file. Defaults to '<output_table>_score'.")
parser.add_argument(
    "--output-path", required=True,
    help="Local path where the boundary concordance files will be output.")

# global var containing all input parameters
args = parser.parse_args()

# assign parameters to global settings
gnaf_schema = args.gnaf_schema or "gnaf_" + geoscape_version
admin_bdys_schema = args.admin_schema or "admin_bdys_" + geoscape_version
# previous_gnaf_schema = args.previous_gnaf_schema or "gnaf_" + previous_geoscape_version
# previous_admin_bdys_schema = args.previous_admin_schema or "admin_bdys_" + previous_geoscape_version
output_path = args.output_path
output_table = args.output_table or "boundary_concordance"
output_score_table = args.output_score_table or f"{output_table}_score"

# create postgres connect string
pg_host = args.pghost or os.getenv("PGHOST", "localhost")
pg_port = args.pgport or os.getenv("PGPORT", 5432)
pg_db = args.pgdb or os.getenv("PGDATABASE", "geoscape")
pg_user = args.pguser or os.getenv("PGUSER", "postgres")
pg_password = args.pgpassword or os.getenv("PGPASSWORD", "password")

pg_connect_string = f"dbname='{pg_db}' host='{pg_host}' port='{pg_port}' user='{pg_user}' password='{pg_password}'"

# set postgres script directory
sql_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "postgres-scripts")

# ABS ASGS boundaries that align 100% - do not edit
asgs_concordance_list = ["sa1", "sa2", "sa3", "sa4", "gcc"]
# asgs_concordance_list = ["mb", "sa1", "sa2", "sa3", "sa4", "gcc", "state"]

# ---------------------------------------------------------------------------------------
# edit boundary list to find concordances with
# ---------------------------------------------------------------------------------------

# sources of address level data with boundary tags - names are hardcoded, don't edit them!
source_list = [
    {"name": "abs 2016", "schema": gnaf_schema, "table": "address_principal_census_2016_boundaries"},
    {"name": "abs 2021", "schema": gnaf_schema, "table": "address_principal_census_2021_boundaries"},
    {"name": f"geoscape {geoscape_version}", "schema": gnaf_schema, "table": "address_principal_admin_boundaries"}
]

# source of residential addresses to filter on - this will either be based on ABS Census 2021 meshblocks
#   or planning zone data from the Geoscape Buildings datasets (licensed dataset)

# residential_address_source = {"name": "geoscape", "schema": "geoscape_202203",
#                               "table": "address_principals_buildings"}
# residential_address_source = {"name": "abs 2016", "schema": gnaf_schema,
#                               "table": "address_principal_census_2016_boundaries"}
residential_address_source = {"name": "abs 2021", "schema": gnaf_schema,
                              "table": "address_principal_census_2021_boundaries"}

# the list of boundary pairs to create concordances - from and to sources must match the names of the above sources
# don't include ASGS ABS boundary pairs that are nested (e.g. SA2 > SA3);
# they have their own lookup table and are added automatically
boundary_list = [
    # ABS 2016 to ABS 2016 bdys
    {"from": "sa2", "from_source": "abs 2016", "to": "poa", "to_source": "abs 2016"},
    {"from": "sa2", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    {"from": "poa", "from_source": "abs 2016", "to": "sa2", "to_source": "abs 2016"},
    {"from": "poa", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    {"from": "sa3", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    # only 25% concordance with a ~14% error
    # {"from": "lga", "from_source": "abs 2016", "to": "poa", "to_source": "abs 2016"},
    {"from": "lga", "from_source": "abs 2016", "to": "sa3", "to_source": "abs 2016"},
    {"from": "lga", "from_source": "abs 2016", "to": "state", "to_source": "abs 2016"}

    # # Geoscape to ABS 2016 bdys
    # {"from": "locality", "from_source": f"geoscape {geoscape_version}", "to": "sa2", "to_source": "abs 2016"},
    # {"from": "locality", "from_source": f"geoscape {geoscape_version}", "to": "sa3", "to_source": "abs 2016"},
    # {"from": "locality", "from_source": f"geoscape {geoscape_version}", "to": "lga", "to_source": "abs 2016"},
    # {"from": "postcode", "from_source": f"geoscape {geoscape_version}", "to": "sa3", "to_source": "abs 2016"},
    # # TODO: handle the "duplicate" postcodes that go over state borders
    # # {"from": "postcode", "from_source": f"geoscape {geoscape_version}", "to": "poa", "to_source": "abs 2016"},
    # {"from": "postcode", "from_source": f"geoscape {geoscape_version}", "to": "lga", "to_source": "abs 2016"},
    # {"from": "lga", "from_source": f"geoscape {geoscape_version}", "to": "lga", "to_source": "abs 2016"},
    #
    # # Geoscape to Geoscape bdys
    # {"from": "locality", "from_source": f"geoscape {geoscape_version}",
    #  "to": "lga", "to_source": f"geoscape {geoscape_version}"},
    # {"from": "postcode", "from_source": f"geoscape {geoscape_version}",
    #  "to": "lga", "to_source": f"geoscape {geoscape_version}"}

    # # test concordance for measuring reliability against known differences
    # {"from": "sa2", "from_source": "abs 2016", "to": "sa2", "to_source": "abs 2021"}

    # TODO: add ABS Census 2016 to 2021 correspondences using official ABS files (assuming there"s a demand)
]

# ---------------------------------------------------------------------------------------
