
import os

from psycopg import sql

import settings


def open_sql_file(file_name: str):
    with open(os.path.join(settings.sql_dir, file_name), "rt") as f:
        sql = f.read()
    return prep_sql(sql)


def prep_sql(sql_string: str):
    if settings.gnaf_schema:
        sql_string = sql_string.replace(" gnaf.", f" {settings.gnaf_schema}.")
    if settings.admin_bdys_schema:
        sql_string = sql_string.replace(" admin_bdys.", f" {settings.admin_bdys_schema}.")

    if settings.pg_user != "postgres":
        # alter create table script to run with correct Postgres username
        sql_string = sql_string.replace(" postgres;", f" {sql.Identifier(settings.pg_user)};")

    return sql_string
