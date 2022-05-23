
import argparse
import geoscape
import logging
import os
import psycopg2  # need to install package

from datetime import datetime

# ---------------------------------------------------------------------------------------
# edit database parameters
# ---------------------------------------------------------------------------------------

pg_connect_string = "dbname=geo host=localhost port=5432 user=postgres password=password"

output_schema = "gnaf_202202"

output_table = "boundary_concordance"
output_score_table = "boundary_concordance_score"

# ---------------------------------------------------------------------------------------
# edit boundary list to find concordances with
# ---------------------------------------------------------------------------------------

# sources of address level data with boundary tags - names are hardcoded, don't edit them!
source_list = [
    {"name": "abs 2016", "schema": "gnaf_202202", "table": "address_principal_census_2016_boundaries"},
    {"name": "abs 2021", "schema": "gnaf_202202", "table": "address_principal_census_2021_boundaries"},
    {"name": "geoscape 202202", "schema": "gnaf_202202", "table": "address_principal_admin_boundaries"}
]

# source of residential addresses to filter on - this will either be based on ABS Census 2021 meshblocks
#   or planning zone data from the Geoscape Buildings datasets (licensed dataset)

# residential_address_source = {"name": "geoscape", "schema": "geoscape_202203", "table": "address_principals_buildings"}
residential_address_source = {"name": "abs 2021", "schema": "gnaf_202202",
                              "table": "address_principal_census_2021_boundaries"}
# residential_address_source = {"name": "abs 2016", "schema": "gnaf_202202",
#                               "table": "address_principal_census_2016_boundaries"}

# the list of boundary pairs to create concordances - from and to sources must match the names of the above sources
# don't include ASGS ABS boundary pairs that are nested (e.g. SA2 > SA3) and have their own lookup table
# these are added automatically
boundary_list = [
    # ABS 2016 to ABS 2016 bdys
    {"from": "sa2", "from_source": "abs 2016", "to": "poa", "to_source": "abs 2016"},
    {"from": "sa2", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    {"from": "poa", "from_source": "abs 2016", "to": "sa2", "to_source": "abs 2016"},
    {"from": "poa", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    {"from": "sa3", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    # {"from": "lga", "from_source": "abs 2016", "to": "poa", "to_source": "abs 2016"}, # only 25% concordance with a ~14% error
    {"from": "lga", "from_source": "abs 2016", "to": "sa3", "to_source": "abs 2016"},

    # Geoscape to ABS 2016 bdys
    {"from": "locality", "from_source": "geoscape 202202", "to": "sa2", "to_source": "abs 2016"},
    {"from": "locality", "from_source": "geoscape 202202", "to": "sa3", "to_source": "abs 2016"},
    {"from": "locality", "from_source": "geoscape 202202", "to": "lga", "to_source": "abs 2016"},
    {"from": "postcode", "from_source": "geoscape 202202", "to": "sa3", "to_source": "abs 2016"},
    # {"from": "postcode", "from_source": "geoscape 202202", "to": "poa", "to_source": "abs 2016"}, # TODO: handle the "duplicate" postcodes that go over state borders
    {"from": "postcode", "from_source": "geoscape 202202", "to": "lga", "to_source": "abs 2016"},
    {"from": "lga", "from_source": "geoscape 202202", "to": "lga", "to_source": "abs 2016"},

    # Geoscape to Geoscape bdys
    {"from": "locality", "from_source": "geoscape 202202", "to": "lga", "to_source": "geoscape 202202"},
    {"from": "postcode", "from_source": "geoscape 202202", "to": "lga", "to_source": "geoscape 202202"}

    # # test concordance for measuring reliability against known differences
    # {"from": "sa2", "from_source": "abs 2016", "to": "sa2", "to_source": "abs 2021"}

    # TODO: add ABS Census 2016 to 2021 correspondences using official ABS files (assuming there's a demand)
]

# ---------------------------------------------------------------------------------------

# ABS ASGS boundaries that align 100% - do not edit
asgs_concordance_list = ['sa1', 'sa2', 'sa3', 'sa4', 'gcc']
# asgs_concordance_list = ['mb', 'sa1', 'sa2', 'sa3', 'sa4', 'gcc', 'state']


def main():
    # connect to Postgres database
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # create table
    create_table(pg_cur)

    # add requested concordances
    for bdys in boundary_list:
        add_concordances(bdys, pg_cur)

    # add all ASGS concordances
    add_asgs_concordances(pg_cur)

    # analyse and index table
    index_table(pg_cur)

    # get weighted scores as % concordance
    score_results(pg_cur)

    # # export results to csv
    export_to_csv(pg_cur, f"{output_schema}.{output_table}", output_table + ".csv")
    export_to_csv(pg_cur, f"{output_schema}.{output_score_table}", output_score_table + ".csv")

    # cleanup
    pg_cur.close()
    pg_conn.close()


def create_table(pg_cur):
    start_time = datetime.now()

    query = f"""drop table if exists {output_schema}.{output_table};
                create table {output_schema}.{output_table}
                (
                    from_source     text not null,
                    from_bdy        text not null,
                    from_id         text not null,
                    from_name       text not null,
                    to_source       text not null,
                    to_bdy          text not null,
                    to_id           text not null,
                    to_name         text not null,
                    address_count   integer,
                    address_percent numeric(4, 1)
                );
                alter table {output_schema}.{output_table} owner to postgres;"""

    pg_cur.execute(query)

    logger.info(f"\t - {output_schema}.{output_table} table created : {datetime.now() - start_time}")


def add_concordances(bdys, pg_cur):
    start_time = datetime.now()

    from_source = bdys["from_source"]
    from_bdy = bdys["from"].lower()

    to_source = bdys["to_source"]
    to_bdy = bdys["to"].lower()

    # get input table(s)
    from_table = get_source_table(from_source)
    if from_table is not None:
        input_tables = f"{from_table} as f"

        # if more than one table being used - add a join statement
        if from_source != to_source:
            to_table = get_source_table(to_source)
            input_tables += f"\n\t\t\t\t\t\tinner join {to_table} as t on t.gnaf_pid = f.gnaf_pid"""

        # set the code and name field names
        from_id_field, from_name_field = get_field_names(from_bdy, from_source, "from", input_tables)
        to_id_field, to_name_field = get_field_names(to_bdy, to_source, "to", input_tables)

        # add the residential address table join and filter
        residential_filter = ""
        if residential_address_source["name"] == 'geoscape':
            res_table = f'{residential_address_source["schema"]}.{residential_address_source["table"]}'
            input_tables += f"\n\t\t\t\t\t\tinner join {res_table} as r on r.gnaf_pid = f.gnaf_pid"""
            residential_filter = "and r.is_residential"
        elif from_source == "abs 2021":
            residential_filter = "and f.mb_category_2021 = 'Residential'"
        elif to_source == "abs 2021":
            residential_filter = "and t.mb_category_2021 = 'Residential'"
        # elif from_source == "abs 2016":
        #     residential_filter = "and f.mb_category = 'RESIDENTIAL'"
        # elif to_source == "abs 2016":
        #     residential_filter = "and t.mb_category = 'RESIDENTIAL'"

        # build the query
        query = f"""insert into {output_schema}.{output_table}
                    with agg as (
                        select {from_id_field}::text as from_id,
                               {from_name_field} as from_name,
                               {to_id_field}::text as to_id,
                               {to_name_field} as to_name,
                               count(*) as address_count
                        from {input_tables}
                        where {from_id_field} is not null
                            and {to_id_field} is not null
                            {residential_filter}
                        group by from_id,
                                 from_name,
                                 to_id,
                                 to_name
                    ), final as (
                        select '{from_source}',
                               '{from_bdy}',
                               agg.from_id,
                               agg.from_name,
                               '{to_source}',
                               '{to_bdy}',
                               agg.to_id,
                               agg.to_name,
                               agg.address_count,
                               (agg.address_count::float / 
                               (sum(agg.address_count) over (partition by agg.from_id))::float * 100.0) as percent
                        from agg
                    )
                    select * from final where percent > 0.0;"""

        # print(query)
        pg_cur.execute(query)

        if from_source == to_source:
            logger.info(f"\t - {from_source} {from_bdy} to {to_bdy} records added : {datetime.now() - start_time}")
        else:
            logger.info(f"\t - {from_source} {from_bdy} to {to_source} {to_bdy} records added : "
                        f"{datetime.now() - start_time}")

    else:
        logger.fatal(f"\t - {from_source} not in sources!")


def add_asgs_concordances(pg_cur):
    # adds ABS Census concordances for ASGS boundaries (ordered by increasing size)

    # add ABS Census concordances for ASGS boundaries, one census at a time
    source = "abs 2016"

    for from_bdy in asgs_concordance_list:
        from_index = asgs_concordance_list.index(from_bdy)

        for to_bdy in asgs_concordance_list:
            start_time = datetime.now()
            to_index = asgs_concordance_list.index(to_bdy)

            if to_index > from_index:
                query = f"""insert into {output_schema}.{output_table}
                            select '{source}' as from_source,
                                   '{from_bdy}' as from_bdy,
                                   {from_bdy}_16code as from_id,
                                   {from_bdy}_16name as from_name,
                                   '{source}' as to_source,
                                   '{to_bdy}' as to_bdy,
                                   {to_bdy}_16code as to_id,
                                   {to_bdy}_16name as to_name,
                                   count(*) as address_count,
                                   100.0 as address_percent
                            from admin_bdys_202202.abs_2016_mb as mb
                            inner join gnaf_202202.address_principals as gnaf on gnaf.mb_2016_code = mb.mb_16code
                            group by from_id,
                                     from_name,
                                     to_id,
                                     to_name"""

                # hardcode fixes for inconsistent meshblock, sa1, sa2 and state field names

                # if from_bdy == "mb":
                #     query = query.replace("mb_16name", "mb_category")

                if from_bdy == "sa1" or to_bdy == "sa1":
                    query = query.replace("sa1_16code", "sa1_16main").replace("sa1_16name", "sa1_16_7cd")

                if from_bdy == "sa2" or to_bdy == "sa2":
                    query = query.replace("sa2_16code", "sa2_16main")

                # print(query)
                pg_cur.execute(query)

                logger.info(f"\t - {source} {from_bdy} to {to_bdy} records added : {datetime.now() - start_time}")

    source = "abs 2021"

    for from_bdy in asgs_concordance_list:
        from_index = asgs_concordance_list.index(from_bdy)

        for to_bdy in asgs_concordance_list:
            start_time = datetime.now()
            to_index = asgs_concordance_list.index(to_bdy)

            if to_index > from_index:
                query = f"""insert into {output_schema}.{output_table}
                            select '{source}' as from_source,
                                   '{from_bdy}' as from_bdy,
                                   {from_bdy}_21code as from_id,
                                   {from_bdy}_21name as from_name,
                                   '{source}' as to_source,
                                   '{to_bdy}' as to_bdy,
                                   {to_bdy}_21code as to_id,
                                   {to_bdy}_21name as to_name,
                                   count(*) as address_count,
                                   100.0 as address_percent
                            from admin_bdys_202202.abs_2021_mb as mb
                                     inner join gnaf_202202.address_principals as gnaf on gnaf.mb_2021_code = mb.mb21_code
                            group by from_id,
                                     from_name,
                                     to_id,
                                     to_name"""

                # hardcode fixes for inconsistent meshblock, sa1, sa2 and state field names

                # if from_bdy == "mb":
                #     query = query.replace("mb_21name", "mb_cat")

                if from_bdy == "sa1" or to_bdy == "sa1":
                    query = query.replace("sa1_21name", "sa1_21pid")

                # print(query)
                pg_cur.execute(query)

                logger.info(f"\t - {source} {from_bdy} to {to_bdy} records added : {datetime.now() - start_time}")


def get_field_names(bdy, source, type, sql):
    # determine which table alias to prefix fields with
    if "inner join" in sql and type == "to":
        table = "t"
    else:
        table = "f"

    if source == "abs 2016":
        id_field = f"{table}.{bdy}_16code"
        name_field = f"{table}.{bdy}_16name"
    elif source == "abs 2021":
        id_field = f"{table}.{bdy}_code_2021"
        name_field = f"{table}.{bdy}_name_2021"
    else:
        if bdy == "postcode":
            id_field = f"{table}.postcode"
            name_field = f"concat({table}.postcode, ' ', {table}.state)"
        else:
            id_field = f"{bdy}_pid"
            name_field = f"{bdy}_name"

    return id_field, name_field


def get_source_table(name):
    for source_dict in source_list:
        if source_dict["name"] == name:
            return f'{source_dict["schema"]}.{source_dict["table"]}'

    return None


def index_table(pg_cur):
    start_time = datetime.now()

    # analyse table and add primary key & index
    query = f"""analyse {output_schema}.{output_table};
                alter table {output_schema}.{output_table} 
                    add constraint {output_table}_pkey 
                    primary key (from_source, from_bdy, from_id, to_source, to_bdy, to_id);
                create index {output_table}_combo_idx on {output_schema}.{output_table}
                    using btree (from_source, from_bdy, to_source, to_bdy);
                alter table {output_schema}.{output_table} cluster on {output_table}_combo_idx;"""
    pg_cur.execute(query)

    logger.info(f"\t - table analysed, primary key & index added : {datetime.now() - start_time}")


def score_results(pg_cur):
    start_time = datetime.now()

    # calculate concordance score (weighted by address count)
    query = f"""drop table if exists {output_schema}.{output_score_table};
                create table {output_schema}.{output_score_table} as
                with cnt as (
                    select from_source,
                           from_bdy,
                           from_id,
                           to_source,
                           to_bdy,
                           sum(address_count::float * address_percent) as weighted_address_count,
                           sum(address_count) as address_count
                    from {output_schema}.{output_table}
                    group by from_source,
                             from_bdy,
                             from_id,
                             to_source,
                             to_bdy
                )
                select from_source,
                       from_bdy,
                       to_source,
                       to_bdy,
                       (sum(weighted_address_count) / sum(address_count)::float)::smallint as concordance_percent,
                       null::numeric(5, 1) as error_percent
                from cnt
                group by from_source,
                         from_bdy,
                         to_source,
                         to_bdy
                order by from_source,
                         from_bdy,
                         to_source,
                         to_bdy;
                analyse {output_schema}.{output_score_table};
                alter table {output_schema}.{output_score_table} 
                    add constraint {output_score_table}_pkey primary key (from_source, from_bdy, to_source, to_bdy);"""

    pg_cur.execute(query)

    # log results
    pg_cur.execute(f"select * from {output_schema}.{output_score_table} order by from_source, from_bdy, to_source, to_bdy")
    rows = pg_cur.fetchall()

    logger.info(f"\t - results scored : {datetime.now() - start_time}")
    logger.info("\t\t---------------------------------------------------------------------------------")
    logger.info("\t\t| {:24} | {:24} | {:11} | {:9} |".format("from", "to", "concordance", "avg error"))
    logger.info("\t\t---------------------------------------------------------------------------------")

    # add average errors for ABS 2016 bdys and log QA metrics
    for row in rows:
        from_source = row[0]
        from_bdy = row[1]
        to_source = row[2]
        to_bdy = row[3]

        concordance = row[4]

        # add average expected error using population data from the 2016 census
        if from_source == "abs 2016" and to_source == "abs 2016":
            if from_bdy in asgs_concordance_list and to_bdy in asgs_concordance_list:
                # don't calculate error - it's zero!
                error_percent = 0.0
            else:
                query = f"""with pc as (
                                select con.to_id,
                                       con.to_name,
                                       con.to_source,
                                       sum(from_bdy.g3::float * con.address_percent / 100.0)::integer as population1
                                from census_2016_data.{from_bdy}_g01 as from_bdy
                                         inner join {output_schema}.{output_table} as con on from_bdy.region_id = con.from_id
                                where from_source = '{from_source}'
                                    and from_bdy = '{from_bdy}'
                                    and to_source = '{to_source}'
                                    and to_bdy = '{to_bdy}'
                                group by con.to_id,
                                         con.to_name,
                                         con.to_source
                            ), merge as (
                                select to_id,
                                       to_name,
                                       to_source,
                                       population1,
                                       g3 as population2
        --                                g3 - population1 as pop_difference,
        --                                (abs((g3 - population1) / g3) * 100.0)::smallint as pop_diff_percent
                                from census_2016_data.{to_bdy}_g01 as to_bdy
                                inner join pc on pc.to_id = to_bdy.region_id
                            )
                            select (sum(abs(population2 - population1)) / sum(population2) * 100.0)::numeric(5, 1) as error_percent
    --                                sum(population1) as population1,
    --                                sum(population2) as population2,
    --                                sum(abs(pop_difference)) as pop_difference,
    --                                sqrt(avg(power(population2 - population1, 2)))::smallint as rmse
                            from merge"""

                pg_cur.execute(query)
                error_percent = pg_cur.fetchone()[0]

            error_percent_str = str(error_percent) + "%"

            # update score table
            query = f"""update {output_schema}.{output_score_table}
                            set error_percent = {error_percent}
                        where from_source = '{from_source}'
                            and from_bdy = '{from_bdy}'
                            and to_source = '{to_source}'
                            and to_bdy = '{to_bdy}'"""
            pg_cur.execute(query)

        else:
            error_percent = None
            error_percent_str = "N/A"

        logger.info(f"\t\t| {from_source + ' ' + from_bdy:24} | {to_source + ' ' + to_bdy:24} "
                    f"| {concordance:10}% | {error_percent_str:>9} |")

    logger.info("\t\t---------------------------------------------------------------------------------")


def export_to_csv(pg_cur, table, file_name):

    query = f"""COPY (
                    select * 
                    from {table} 
                    order by from_source, 
                             from_bdy, 
                             to_source, 
                             to_bdy
                ) TO STDOUT WITH CSV HEADER"""
    with open(os.path.join(output_path, file_name), "w") as f:
        pg_cur.copy_expert(query, f)


# set the command line arguments for the script
def set_arguments():
    parser = argparse.ArgumentParser(
        description='A CSV file and supporting scripts for converting data between Australian boundaries.')

    # PG Options
    parser.add_argument(
        '--pghost',
        help='Host name for Postgres server. Defaults to PGHOST environment variable if set, otherwise localhost.')
    parser.add_argument(
        '--pgport', type=int,
        help='Port number for Postgres server. Defaults to PGPORT environment variable if set, otherwise 5432.')
    parser.add_argument(
        '--pgdb',
        help='Database name for Postgres server. Defaults to PGDATABASE environment variable if set, '
             'otherwise geo.')
    parser.add_argument(
        '--pguser',
        help='Username for Postgres server. Defaults to PGUSER environment variable if set, otherwise postgres.')
    parser.add_argument(
        '--pgpassword',
        help='Password for Postgres server. Defaults to PGPASSWORD environment variable if set, '
             'otherwise \'password\'.')

    # schema names
    geoscape_version = geoscape.get_geoscape_version(datetime.today())
    parser.add_argument(
        '--admin-schema', default='admin_bdys_' + geoscape_version,
        help='Destination schema name to store final admin boundary tables in. Defaults to \'admin_bdys_'
             + geoscape_version + '\'.')
    parser.add_argument(
        '--output-schema', default='gnaf_' + geoscape_version,
        help='Destination schema name to store final boundary concordance tables in. Defaults to \'gnaf_'
             + geoscape_version + '\'.')

    # output directory
    parser.add_argument(
        '--output-path', required=True,
        help='Local path where the Shapefile and GeoJSON files will be output.')

    return parser.parse_args()


if __name__ == '__main__':
    full_start_time = datetime.now()

    # set logger
    log_file = os.path.abspath(__file__).replace(".py", ".log")
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                        datefmt="%m/%d/%Y %I:%M:%S %p")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # setup logger to write to screen as well as writing to log file
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    task_name = "Create boundary concordance table"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))

    main()

    logger.info("{} finished : {}".format(task_name, datetime.now() - full_start_time))

