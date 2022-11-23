
import os
import settings


def open_sql_file(file_name):
    sql = open(os.path.join(settings.sql_dir, file_name), "r").read()
    return prep_sql(sql)


def prep_sql(sql):
    if settings.gnaf_schema is not None:
        sql = sql.replace(" gnaf.", f" {settings.gnaf_schema}.")
    if settings.admin_bdys_schema is not None:
        sql = sql.replace(" admin_bdys.", f" {settings.admin_bdys_schema}.")

    if settings.pg_user != "postgres":
        # alter create table script to run with correct Postgres username
        sql = sql.replace(" postgres;", f" {settings.pg_user};")

    return sql
