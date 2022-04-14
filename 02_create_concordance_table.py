
import io
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

# ---------------------------------------------------------------------------------------
# edit boundary list tovfind concordances with
# ---------------------------------------------------------------------------------------

source_list = [
    {"name": "abs 2016", "schema": "gnaf_202202", "table": "address_principal_census_2016_boundaries"},
    {"name": "abs 2021", "schema": "gnaf_202202", "table": "address_principal_census_2021_boundaries"},
    {"name": "geoscape", "schema": "gnaf_202202", "table": "address_principal_admin_boundaries"}
]

# from and to sources must match the names of the above sources
boundary_list = [
    # ABS 2016 to ABS 2016 bdys
    {"from": "poa", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    {"from": "sa3", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    {"from": "lga", "from_source": "abs 2016", "to": "sa3", "to_source": "abs 2016"},
    {"from": "sa2", "from_source": "abs 2016", "to": "lga", "to_source": "abs 2016"},
    {"from": "sa2", "from_source": "abs 2016", "to": "sa3", "to_source": "abs 2016"},
    {"from": "sa2", "from_source": "abs 2016", "to": "poa", "to_source": "abs 2016"}

    # Geoscape to ABS 2016 bdys
    # {"from": "", "from_source": "geoscape", "to": "lga", "to_source": "abs 2016"},
]

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
        # if more than source table create a join statement
        if from_source != to_source:
            to_table = get_source_table(to_source)
            input_tables = f"""{from_table} as f\n\t\t\t\t\t\tinner join {to_table} as t on t.gnaf_pid = f.gnaf_pid"""
        else:
            input_tables = from_table

        # set the code and name field names
        # TODO: replace the hardcoding
        # TODO: handle situation where from/to field names are the same on an inner join
        if from_source == "abs 2016":
            from_id_field = f"{from_bdy}_16code"
            from_name_field = f"{from_bdy}_16name"
        elif from_source == "abs 2021":
            from_id_field = f"{from_bdy}_code_2021"
            from_name_field = f"{from_bdy}_name_2021"
        else:
            from_id_field = f"{from_bdy}_pid"
            from_name_field = f"{from_bdy}_name"

        if to_source == "abs 2016":
            to_id_field = f"{to_bdy}_16code"
            to_name_field = f"{to_bdy}_16name"
        elif to_source == "abs 2021":
            to_id_field = f"{to_bdy}_code_2021"
            to_name_field = f"{to_bdy}_name_2021"
        else:
            to_id_field = f"{to_bdy}_pid"
            to_name_field = f"{to_bdy}_name"

        # build the query
        query = f"""insert into {output_schema}.{output_table}
                    with agg as (
                        select {from_id_field}::text as from_id,
                               {from_name_field} as from_name,
                               {to_id_field}::text as to_id,
                               {to_name_field} as to_name,
                               count(*) as address_count
                        from {input_tables}
                        where mb_category IN ('RESIDENTIAL')
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

        logger.info(f"\t - {from_bdy} to {to_bdy} records added : {datetime.now() - start_time}")

    else:
        logger.fatal(f"\t - {from_source} not in sources!")


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
    query = f"""with cnt as (
                select from_type,
                       from_id,
                       to_type,
                       sum(address_count::float * address_percent) as weighted_address_count,
                       sum(address_count) as address_count
                from {output_schema}.{output_table}
                group by from_type,
                         from_id,
                         to_type
            )
            select from_type,
                   to_type,
                   (sum(weighted_address_count) / sum(address_count)::float)::smallint as concordance_percent
            from cnt
            group by from_type,
                     to_type;"""

    pg_cur.execute(query)
    rows = pg_cur.fetchall()

    logger.info(f"\t - results scored : {datetime.now() - start_time}")
    logger.info("\t\t---------------------------------------------")
    logger.info("\t\t| {:12} | {:12} | {:11} |".format("from", "to", "concordance"))
    logger.info("\t\t---------------------------------------------")

    for row in rows:
        logger.info("\t\t| {:12} | {:12} | {:10}% |".format(row[0], row[1], row[2]))

    logger.info("\t\t---------------------------------------------")


def copy_table(input_pg_cur, export_pg_cur, input_schema, input_table, export_schema, export_table):
    start_time = datetime.now()

    # load source table into memory
    input_rows = io.StringIO()

    export_sql = "COPY (SELECT * FROM {}.{}) TO STDOUT".format(input_schema, input_table)

    input_pg_cur.copy_expert(export_sql, input_rows)
    input_rows.seek(0)

    logger.info("\t - source table loaded into memory: {}".format(datetime.now() - start_time))
    start_time = datetime.now()

    # import into target Postgres
    export_pg_cur.copy_expert("COPY {}.{} FROM STDOUT".format(export_schema, export_table), input_rows)
    export_pg_cur.execute("ANALYSE {}.{}".format(export_schema, export_table))

    input_rows.close()

    export_pg_cur.execute("SELECT count(*) FROM {}.{}".format(export_schema, export_table))
    num_rows = export_pg_cur.fetchone()[0]

    logger.info("\t - target table imported : {} total rows : {}"
                .format(num_rows, datetime.now() - start_time))


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

