
import geoscape
import logging
import os
import psycopg  # need to install package
import settings  # gets global vars and runtime arguments
import zipfile

from datetime import datetime


def main():
    # connect to Postgres database
    pg_conn = psycopg.connect(settings.pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # create Census bdy tag tables for all GNAF addresses
    create_bdy_tag_tables(pg_cur)

    # create concordance table
    create_table(pg_cur)

    # add requested concordances
    for bdys in settings.boundary_list:
        add_concordances(bdys, pg_cur)

    # add all ASGS concordances
    add_asgs_concordances(pg_cur)

    # analyse and index table
    index_table(pg_cur)

    # get weighted scores as % concordance
    score_results(pg_cur)

    # export results to csv
    export_to_csv(pg_cur, f'{settings.gnaf_schema}.{settings.output_table}',
                  settings.output_table + ".csv", True)
    export_to_csv(pg_cur, f'{settings.gnaf_schema}.{settings.output_score_table}',
                  settings.output_score_table + ".csv", False)

    # copy to GDA2020 schema
    sql = geoscape.open_sql_file("data-prep/03_copy_to_gda2020_schema.sql")
    pg_cur.execute(sql)

    logger.info('\t - tables copied to GDA2020 schema')

    # cleanup
    pg_cur.close()
    pg_conn.close()


def create_bdy_tag_tables(pg_cur):
    start_time = datetime.now()

    sql = geoscape.open_sql_file("data-prep/01a_create_gnaf_2016_census_bdy_table.sql")
    pg_cur.execute(sql)
    logger.info(f'\t - ABS 2016 boundary tag table created : {datetime.now() - start_time}')

    start_time = datetime.now()
    sql = geoscape.open_sql_file("data-prep/01b_create_gnaf_2021_census_bdy_table.sql")
    pg_cur.execute(sql)
    logger.info(f'\t - ABS 2021 boundary tag table created : {datetime.now() - start_time}')


def create_table(pg_cur):
    start_time = datetime.now()

    query = f"""drop table if exists {settings.gnaf_schema}.{settings.output_table};
                create table {settings.gnaf_schema}.{settings.output_table}
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
                alter table {settings.gnaf_schema}.{settings.output_table} owner to postgres;"""

    pg_cur.execute(query)

    logger.info(f'\t - {settings.gnaf_schema}.{settings.output_table} table created : '
                f'{datetime.now() - start_time}')


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
        if settings.residential_address_source["name"] == 'geoscape':
            res_table = f'{settings.residential_address_source["schema"]}.' \
                        f'{settings.residential_address_source["table"]}'
            input_tables += f"\n\t\t\t\t\t\tinner join {res_table} as r on r.gnaf_pid = f.gnaf_pid"""
            residential_filter = "and r.is_residential"
        elif from_source == "abs 2021":
            residential_filter = "and f.mb_category_2021 in ('Residential', 'Primary Production', 'Other')"
        elif to_source == "abs 2021":
            residential_filter = "and t.mb_category_2021 in ('Residential', 'Primary Production', 'Other')"
        # elif from_source == "abs 2016":
        #     residential_filter = "and f.mb_category = 'RESIDENTIAL'"
        # elif to_source == "abs 2016":
        #     residential_filter = "and t.mb_category = 'RESIDENTIAL'"

        # build the query
        query = f"""insert into {settings.gnaf_schema}.{settings.output_table}
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

    for from_bdy in settings.asgs_concordance_list:
        from_index = settings.asgs_concordance_list.index(from_bdy)

        for to_bdy in settings.asgs_concordance_list:
            start_time = datetime.now()
            to_index = settings.asgs_concordance_list.index(to_bdy)

            if to_bdy == "gccsa":
                to_bdy = "gcc"

            if to_index > from_index:
                query = f"""insert into {settings.gnaf_schema}.{settings.output_table}
                            select '{source}' as from_source,
                                   '{from_bdy}' as from_bdy,
                                   {from_bdy}_code16 as from_id,
                                   {from_bdy}_name16 as from_name,
                                   '{source}' as to_source,
                                   '{to_bdy}' as to_bdy,
                                   {to_bdy}_code16 as to_id,
                                   {to_bdy}_name16 as to_name,
                                   count(*) as address_count,
                                   100.0 as address_percent
                            from census_2016_bdys.mb_2016_aust as mb
                            inner join gnaf_202505.address_principals as gnaf on gnaf.mb_2016_code::text = mb.mb_code16
                            group by from_id,
                                     from_name,
                                     to_id,
                                     to_name"""

                # hardcode fixes for inconsistent meshblock, sa1, sa2 and state field names

                # if from_bdy == "mb":
                #     query = query.replace("mb_name16", "mb_category")

                if from_bdy == "sa1" or to_bdy == "sa1":
                    query = query.replace("sa1_code16", "sa1_main16").replace("sa1_name16", "sa1_7dig16")

                if from_bdy == "sa2" or to_bdy == "sa2":
                    query = query.replace("sa2_code16", "sa2_main16")

                # print(query)
                pg_cur.execute(query)

                logger.info(f"\t - {source} {from_bdy} to {to_bdy} records added : {datetime.now() - start_time}")

    source = "abs 2021"

    for from_bdy in settings.asgs_concordance_list:
        from_index = settings.asgs_concordance_list.index(from_bdy)

        for to_bdy in settings.asgs_concordance_list:
            start_time = datetime.now()
            to_index = settings.asgs_concordance_list.index(to_bdy)

            # fix for field name change between censuses
            if to_bdy == "gcc":
                to_bdy = to_bdy.replace("gcc", "gccsa")

            if to_index > from_index:
                query = f"""insert into {settings.gnaf_schema}.{settings.output_table}
                            select '{source}' as from_source,
                                   '{from_bdy}' as from_bdy,
                                   {from_bdy}_code_2021 as from_id,
                                   {from_bdy}_name_2021 as from_name,
                                   '{source}' as to_source,
                                   '{to_bdy}' as to_bdy,
                                   {to_bdy}_code_2021 as to_id,
                                   {to_bdy}_name_2021 as to_name,
                                   count(*) as address_count,
                                   100.0 as address_percent
                            from census_2021_bdys_gda94.mb_2021_aust_gda94 as mb
                                     inner join gnaf_202505.address_principals as gnaf 
                                         on gnaf.mb_2021_code::text = mb.mb_code_2021
                            group by from_id,
                                     from_name,
                                     to_id,
                                     to_name"""

                # hardcode fixes for inconsistent meshblock, sa1, sa2 and state field names

                # if from_bdy == "mb":
                #     query = query.replace("mb_21name", "mb_cat")

                if from_bdy == "sa1" or to_bdy == "sa1":
                    query = query.replace("sa1_name_2021", "sa1_code_2021")

                # print(query)
                pg_cur.execute(query)

                logger.info(f"\t - {source} {from_bdy} to {to_bdy} records added : {datetime.now() - start_time}")


def get_field_names(bdy, source, to_from, sql):
    # determine which table alias to prefix fields with
    if "inner join" in sql and to_from == "to":
        table = "t"
    else:
        table = "f"

    if source == "abs 2016":
        id_field = f"{table}.{bdy}_code16"
        name_field = f"{table}.{bdy}_name16"

        if bdy == "gccsa":
            id_field = id_field.replace("gccsa_code16", "gcc_code16")
            name_field = name_field.replace("gccsa_name16", "gcc_name16")

    elif source == "abs 2021":
        id_field = f"{table}.{bdy}_code_2021"
        name_field = f"{table}.{bdy}_name_2021"

        if bdy == "sa1":
            name_field = name_field.replace("sa1_name_2021", "sa1_code_2021")
    else:
        if bdy == "postcode":
            id_field = f"{table}.postcode"
            name_field = f"concat({table}.postcode, ' ', {table}.state)"
        else:
            id_field = f"{bdy}_pid"
            name_field = f"{bdy}_name"

    return id_field, name_field


def get_source_table(name):
    for source_dict in settings.source_list:
        if source_dict["name"] == name:
            return f'{source_dict["schema"]}.{source_dict["table"]}'

    return None


def index_table(pg_cur):
    start_time = datetime.now()

    # analyse table and add primary key & index
    query = f"""analyse {settings.gnaf_schema}.{settings.output_table};
                alter table {settings.gnaf_schema}.{settings.output_table} 
                    add constraint {settings.output_table}_pkey 
                    primary key (from_source, from_bdy, from_id, to_source, to_bdy, to_id);
                create index {settings.output_table}_combo_idx 
                    on {settings.gnaf_schema}.{settings.output_table}
                    using btree (from_source, from_bdy, to_source, to_bdy);
                alter table {settings.gnaf_schema}.{settings.output_table} 
                    cluster on {settings.output_table}_combo_idx;"""
    pg_cur.execute(query)

    logger.info(f"\t - table analysed, primary key & index added : {datetime.now() - start_time}")


def score_results(pg_cur):
    start_time = datetime.now()

    # calculate concordance score (weighted by address count)
    query = f"""drop table if exists {settings.gnaf_schema}.{settings.output_score_table};
                create table {settings.gnaf_schema}.{settings.output_score_table} as
                with cnt as (
                    select from_source,
                           from_bdy,
                           from_id,
                           to_source,
                           to_bdy,
                           sum(address_count::float * address_percent) as weighted_address_count,
                           sum(address_count) as address_count
                    from {settings.gnaf_schema}.{settings.output_table}
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
                analyse {settings.gnaf_schema}.{settings.output_score_table};
                alter table {settings.gnaf_schema}.{settings.output_score_table} 
                    add constraint {settings.output_score_table}_pkey 
                        primary key (from_source, from_bdy, to_source, to_bdy);"""

    pg_cur.execute(query)

    # log results
    pg_cur.execute(f'select * from {settings.gnaf_schema}.{settings.output_score_table} '
                   f'order by from_source, from_bdy, to_source, to_bdy')
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
            if from_bdy in settings.asgs_concordance_list and to_bdy in settings.asgs_concordance_list:
                # don't calculate error - it's zero!
                error_percent = 0.0
            else:
                query = f"""with pc as (
                                select con.to_id,
                                       con.to_name,
                                       con.to_source,
                                       sum(from_bdy.g3::float * con.address_percent / 100.0)::integer as population1
                                from census_2016_data.{from_bdy}_g01 as from_bdy
                                         inner join {settings.gnaf_schema}.{settings.output_table} as con 
                                             on from_bdy.region_id = con.from_id
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
                            select (sum(abs(population2 - population1)) / sum(population2) * 100.0)::numeric(5, 1) 
                                as error_percent
    --                                sum(population1) as population1,
    --                                sum(population2) as population2,
    --                                sum(abs(pop_difference)) as pop_difference,
    --                                sqrt(avg(power(population2 - population1, 2)))::smallint as rmse
                            from merge"""

                pg_cur.execute(query)
                error_percent = pg_cur.fetchone()[0]

            error_percent_str = str(error_percent) + "%"

            # update score table
            query = f"""update {settings.gnaf_schema}.{settings.output_score_table}
                            set error_percent = {error_percent}
                        where from_source = '{from_source}'
                            and from_bdy = '{from_bdy}'
                            and to_source = '{to_source}'
                            and to_bdy = '{to_bdy}'"""
            pg_cur.execute(query)

        else:
            # error_percent = None
            error_percent_str = "N/A"

        logger.info(f"\t\t| {from_source + ' ' + from_bdy:24} | {to_source + ' ' + to_bdy:24} "
                    f"| {concordance:10}% | {error_percent_str:>9} |")

    logger.info("\t\t---------------------------------------------------------------------------------")


def export_to_csv(pg_cur, table, file_name, compress_file):
    query = f"""COPY (
                    select * 
                    from {table} 
                    order by from_source, 
                             from_bdy, 
                             to_source, 
                             to_bdy
                ) TO STDOUT WITH CSV HEADER"""

    file_path = os.path.join(settings.output_path, file_name)

    # export to CSV
    with open(file_path, "wb") as f:
        with pg_cur.copy(query) as copy:
            while data := copy.read():
                f.write(data)

    # compress CSV (if required)
    if compress_file:
        zip_path = file_path.replace(".csv", ".zip")
        zipfile.ZipFile(zip_path, mode="w").write(file_path, compress_type=zipfile.ZIP_DEFLATED)


if __name__ == "__main__":
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
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)

    task_name = "Create boundary concordance table"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))

    main()

    logger.info("{} finished : {}".format(task_name, datetime.now() - full_start_time))
