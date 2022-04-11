
import io
import logging
import os
import psycopg2  # need to install package

from datetime import datetime

# ---------------------------------------------------------------------------------------
# edit database parameters
# ---------------------------------------------------------------------------------------

pg_connect_string = "dbname=geo host=localhost port=5432 user=postgres password=password"

input_schema = "gnaf_202202"
input_table = "address_principal_census_2016_boundaries"

output_schema = "testing"
output_table = "census_2016_bdy_concordance"

# ---------------------------------------------------------------------------------------
# edit boundary list tovfind concordances with
# ---------------------------------------------------------------------------------------

boundary_list = [{"from": "poa", "to": "lga"},
                 {"from": "lga", "to": "poa"},
                 {"from": "sa3", "to": "lga"},
                 {"from": "lga", "to": "sa3"},
                 {"from": "sa2", "to": "lga"},
                 {"from": "lga", "to": "sa2"}
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
        add_concordances(bdys["from"].lower(), bdys["to"].lower(), pg_cur)

    # analyse and index table
    index_table(pg_cur)

    # cleanup
    pg_cur.close()
    pg_conn.close()


def create_table(pg_cur):
    start_time = datetime.now()

    query = f"""drop table if exists {output_schema}.{output_table};
                create table {output_schema}.{output_table}
                (
                    from_type       text not null,
                    from_id         text not null,
                    from_name       text not null,
                    to_type         text not null,
                    to_id           text not null,
                    to_name         text not null,
                    address_count   integer,
                    address_percent double precision
                );
                alter table {output_schema}.{output_table} owner to postgres;"""

    pg_cur.execute(query)

    logger.info(f"\t - {output_schema}.{output_table} table created : {datetime.now() - start_time}")


def add_concordances(from_bdy, to_bdy, pg_cur):
    start_time = datetime.now()

    query = f"""insert into {output_schema}.{output_table}
                with agg as (
                    select {from_bdy}_16code as from_id,
                           {from_bdy}_16name as from_name,
                           {to_bdy}_16code as to_id,
                           {to_bdy}_16name as to_name,
                           count(*) as address_count
                    from {input_schema}.{input_table}
                    group by from_id,
                             from_name,
                             to_id,
                             to_name
                ), final as (
                    select '{from_bdy}',
                           agg.from_id,
                           agg.from_name,
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

    pg_cur.execute(query)

    logger.info(f"\t - from {from_bdy} to {to_bdy} records added : {datetime.now() - start_time}")


def index_table(pg_cur):
    start_time = datetime.now()

    query = f"""analyse {output_schema}.{output_table};
                alter table {output_schema}.{output_table} 
                    add constraint {output_table}_pkey primary key (from_id, to_id);"""

    pg_cur.execute(query)

    logger.info(f"\t - primary key added : {datetime.now() - start_time}")


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

