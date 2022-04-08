# ---------------------------------------------------------------------------------------------------------------------
#
# script to download S3 parquet files for loading into local postgres
#
# ---------------------------------------------------------------------------------------------------------------------

import argparse
import boto3
import dask.dataframe as dd
# import dask_geopandas
import os
import sqlalchemy
import sys

from datetime import datetime


# create postgres connect string
sql_alchemy_engine_string = "postgresql+psycopg2://postgres:password@localhost/geo"


def main():
    start_time = datetime.now()

    # parser = argparse.ArgumentParser(
    #     description='downloads S3 parquet files for loading into local postgres')
    #
    # parser.add_argument('--source-s3-bucket',
    #                     default='mobai-sandpit-bucket-compassiot-oem', help='The S3 bucket for the CSV file(s)')
    # parser.add_argument('--source-s3-folder',
    #                     help='The S3 folder for the parquet file(s)')
    # parser.add_argument('--spatial',
    #                     default='false', help='Do the  input files have a geomtry column?')
    # parser.add_argument('--target-table',
    #                     help='The schema and table name (e.g. <schemaname>.<tablename>) for the target Postgres table')
    #
    # args = parser.parse_args()
    # s3_bucket = args.source_s3_bucket
    # s3_folder = args.source_s3_folder
    # is_spatial = args.spatial
    # schema_name = args.target_table.split(".")[0]
    # table_name = args.target_table.split(".")[1]

    # input_folder = os.path.join(temp_folder, s3_folder)

    # # create missing directory path
    # Path(input_folder).mkdir(parents=True, exist_ok=True)

    # # create AWS s3 client
    # initialize()
    #
    # # get list of S3files
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(s3_bucket)
    # objs = bucket.objects.filter(Prefix=s3_folder)
    #
    # s3_files = list()
    #
    # for obj in objs:
    #     s3_files.append(obj.key)
    #
    # # config = TransferConfig(multipart_threshold=1024 ** 2)  # 1MB
    # jobs = [(s3_bucket, key, os.path.join(temp_folder, key)) for key in s3_files]
    # # print(jobs)
    #
    # print("Got S3 file list : {}".format(datetime.now() - start_time))
    # start_time = datetime.now()
    #
    # # make a process pool and download all files in parallel
    # pool = multiprocessing.Pool(multiprocessing.cpu_count(), initialize)
    # pool.map(download, jobs)
    # pool.close()
    # pool.join()
    #
    # print("Downloaded {} files from S3 : {}".format(len(jobs), datetime.now() - start_time))
    # start_time = datetime.now()


    # create dask dataframe from Postgres table
    df = dd.read_sql_table("address_principal_census_2016_boundaries", sql_alchemy_engine_string,
                           schema="gnaf_202202", index_col="gid", npartitions=32)

    # print(df.head())

    # print(f"{df.count()} rows")




#     # create dask GeoDataFrame from local parquet files
#     ddf = dask.dataframe.read_parquet(input_folder, engine='pyarrow')
#     dgdf = dask_geopandas.from_dask_dataframe(ddf)
#
#     # add geometry column if required
#
#     # TODO: test this when 'from_wkt' gets added to dask_geopandas
#
#     if is_spatial:
#         output_df = dgdf.set_geometry(dask_geopandas.GeoSeries.from_wkt(dgdf["wkt_geom"].str.replace("SRID=4326;", "")))
#
#         # output_df = geopandas.GeoDataFrame(df, geometry=geopandas.GeoSeries.from_wkt(df["wkt_geom"].str.replace("SRID=4326;", "")), crs="EPSG:4326") \
#         #     .drop(["wkt_geom"], axis=1)
#         # print("Imported {} into a GeoPandas dataframe : {}".format(s3_path, datetime.now() - start_time))
#     else:
#         output_df = dgdf
#         # print("Imported {} into a Pandas dataframe : {}".format(s3_path, datetime.now() - start_time))
#
#     print("Dask GeoDataFrame created : {}".format(datetime.now() - start_time))
#     start_time = datetime.now()
#
#     # Export to Postgres/PostGIS
#     engine = sqlalchemy.create_engine(sql_alchemy_engine_string)
#     output_df.to_postgis(table_name, engine, schema=schema_name, if_exists="replace")
#
#     print("Exported to Postgres : {}".format(datetime.now() - start_time))
#     start_time = datetime.now()
#
#     # analyse & cluster table - and rename geom column to PostGIS standard
#     with engine.connect() as conn:
#         conn.execute("ANALYSE {}.{}".format(schema_name, table_name))
#
#         if is_spatial:
#             conn.execute("ALTER TABLE {0}.{1} CLUSTER ON idx_{1}_geometry".format(schema_name, table_name))
#             conn.execute("ALTER TABLE {}.{} RENAME COLUMN geometry TO geom".format(schema_name, table_name))
#
#     print("Table optimised : {}".format(datetime.now() - start_time))
#
#
# def initialize():
#     global s3_client
#     s3_client = boto3.client('s3')
#
#
# def download(job):
#     bucket, key, filename = job
#
#     print(filename)
#
#     s3_client.download_file(bucket, key, filename)


if __name__ == "__main__":
    full_start_time = datetime.now()

    task_name = "Create Boundary Concordance Table"

    print("{} started".format(task_name))
    print("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    print("{} finished : {}".format(task_name, time_taken))
    print()
