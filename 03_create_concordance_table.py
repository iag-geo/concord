
import logging
import os
import psycopg2  # need to install package

from datetime import datetime

# ---------------------------------------------------------------------------------------
# edit database parameters
# ---------------------------------------------------------------------------------------

pg_connect_string = "dbname=geo host=localhost port=5432 user=postgres password=password"

output_schema = "testing"
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

# source of residential addresses to us - this will either be based on ABS Census 2021 meshblocks
#   or planning zone data from the Geoscape Buildings datasets if you have purchased it
# residential_address_source = {"name": "geoscape", "schema": "geoscape_202203", "table": "address_principals_buildings"}
residential_address_source = {"name": "abs 2021", "schema": "gnaf_202202",
                              "table": "address_principal_census_2021_boundaries"}

# the list of boundary pair to create concordances - from and to sources must match the names of the above sources
boundary_list = [
    # # ABS 2016 to ABS 2016 bdys
    # {"from": "poa", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    # {"from": "sa3", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    # {"from": "lga", "from_source": "abs 2016", "to": "sa3", "to_source": "abs 2016"},
    # {"from": "sa2", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    # {"from": "sa2", "from_source": "abs 2016", "to": "sa3", "to_source": "abs 2016"},
    # {"from": "sa2", "from_source": "abs 2016", "to": "poa", "to_source": "abs 2016"},
    #
    # # Geoscape to ABS 2016 bdys
    # {"from": "locality", "from_source": "geoscape 202202", "to": "lga", "to_source": "abs 2016"},
    # {"from": "postcode", "from_source": "geoscape 202202", "to": "lga", "to_source": "abs 2016"},
    # {"from": "lga", "from_source": "geoscape 202202", "to": "lga", "to_source": "abs 2016"},
    #
    # # Geoscape to Geoscape bdys
    # {"from": "locality", "from_source": "geoscape 202202", "to": "lga", "to_source": "geoscape 202202"},
    # {"from": "postcode", "from_source": "geoscape 202202", "to": "lga", "to_source": "geoscape 202202"}

    {"from": "sa2", "from_source": "abs 2016", "to": "sa2", "to_source": "abs 2021"}
]

# ---------------------------------------------------------------------------------------
# edit output file path
# ---------------------------------------------------------------------------------------

output_path = os.path.dirname(os.path.realpath(__file__))

# ---------------------------------------------------------------------------------------


def main():
    # connect to Postgres database
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # create table
    create_table(pg_cur)

    # add concordances
    for bdys in boundary_list:
        add_concordances(bdys, pg_cur)

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
                    from_type       text not null,
                    from_id         text not null,
                    from_name       text not null,
                    to_source       text not null,
                    to_type         text not null,
                    to_id           text not null,
                    to_name         text not null,
                    address_count   integer,
                    address_percent double precision
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

        # add the residential address table join and filter
        if residential_address_source["name"] == 'geoscape':
            res_table = f'{residential_address_source["schema"]}.{residential_address_source["table"]}'
            input_tables += f"\n\t\t\t\t\t\tinner join {res_table} as r on r.gnaf_pid = f.gnaf_pid"""
            residential_filter = "and r.is_residential = 'residential'"
        elif from_source == "abs 2021":
            residential_filter = "and f.mb_category_2021 = 'Residential'"
        elif to_source == "abs 2021":
            residential_filter = "and t.mb_category_2021 = 'Residential'"

        # set the code and name field names
        from_id_field, from_name_field = get_field_names(from_bdy, from_source, "from", input_tables)
        to_id_field, to_name_field = get_field_names(to_bdy, to_source, "to", input_tables)

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

        # hardcode fixes for SA1 and SA2 oddities
        if "sa1" in [from_bdy, to_bdy]:
            query = query.replace("sa1_16code", "sa1_16main").replace("sa1_16name", "sa1_16_7cd")

        if "sa2" in [from_bdy, to_bdy]:
            query = query.replace("sa2_16code", "sa2_16main")

        # print(query)
        pg_cur.execute(query)

        logger.info(f"\t - {from_source} {from_bdy} to {to_source} {to_bdy} records added : {datetime.now() - start_time}")

    else:
        logger.fatal(f"\t - {from_source} not in sources!")


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

    query = f"""analyse {output_schema}.{output_table};
                alter table {output_schema}.{output_table} 
                    add constraint {output_table}_pkey primary key (from_id, to_id);"""

    pg_cur.execute(query)

    logger.info(f"\t - primary key added : {datetime.now() - start_time}")


def score_results(pg_cur):
    start_time = datetime.now()

    # calculate concordance score (weighted by address count)
    query = f"""drop table if exists {output_schema}.{output_score_table};
                create table {output_schema}.{output_score_table} as
                with cnt as (
                    select concat(from_source, ' ', from_type) as from_bdy,
                           from_id,
                           concat(to_source, ' ', to_type) as to_bdy,
                           sum(address_count::float * address_percent) as weighted_address_count,
                           sum(address_count) as address_count
                    from {output_schema}.{output_table}
                    group by from_bdy,
                             from_id,
                             to_bdy
                )
                select from_bdy,
                       to_bdy,
                       (sum(weighted_address_count) / sum(address_count)::float)::smallint as concordance_percent
                from cnt
                group by from_bdy,
                         to_bdy;
                analyse {output_schema}.{output_score_table};
                alter table {output_schema}.{output_score_table} 
                    add constraint {output_score_table}_pkey primary key (from_bdy, to_bdy);"""

    pg_cur.execute(query)

    # log results
    pg_cur.execute(f"select * from {output_schema}.{output_score_table} order by from_bdy, to_bdy")
    rows = pg_cur.fetchall()

    logger.info(f"\t - results scored : {datetime.now() - start_time}")
    logger.info("\t\t---------------------------------------------------------------------")
    logger.info("\t\t| {:24} | {:24} | {:11} |".format("from", "to", "concordance"))
    logger.info("\t\t---------------------------------------------------------------------")

    for row in rows:
        logger.info("\t\t| {:24} | {:24} | {:10}% |".format(row[0], row[1], row[2]))

    logger.info("\t\t---------------------------------------------------------------------")


def export_to_csv(pg_cur, table, file_name):

    query = f"COPY (select * from {table}) TO STDOUT WITH CSV HEADER"
    with open(os.path.join(output_path, file_name), "w") as f:
        pg_cur.copy_expert(query, f)


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

