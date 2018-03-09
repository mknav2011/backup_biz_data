#!/usr/bin/python
import os, sys, json, uuid
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import input_file_name

from pyspark.sql import SparkSession
from pyspark import SparkConf

import time, datetime
from dateutil.parser import parse

import boto3, botocore, logging # for s3 file access as a string
client = boto3.client('s3')

from io import BytesIO, StringIO

from urlparse import urlparse # For URL Parsing


"""
--JOB_NAME:PropertyDim_Full_Refresh
--s3_source_path:s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim
--s3_target_path:s3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-full
--source_sql_file_path:s3://move-dataeng-lstp-dev/edw/propertydim_temp_scripts/property_dim_dedupe.sql
--temp_table_name:property_dim_All

--s3_bucket_sql_file:move-dataeng-lstp-dev
--s3_prefix_sql_file:edw/propertydim_temp_scripts/property_dim_dedupe.sql

/usr/lib/spark/bin/spark-submit --conf spark.hadoop.yarn.resourcemanager.connect.max-wait.ms=60000 --conf spark.hadoop.fs.defaultFS=hdfs://ip-172-31-38-180.us-west-2.compute.internal:8020 --conf spark.hadoop.yarn.resourcemanager.address=ip-172-31-38-180.us-west-2.compute.internal:8032 --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=18 --conf spark.executor.memory=5g --conf spark.executor.cores=4 --name tape --master yarn --deploy-mode cluster --jars /opt/amazon/superjar/glue-assembly.jar --files /tmp/glue-default.conf,/tmp/glue-override.conf,/opt/amazon/certs/InternalAndExternalAndAWSTrustStore.jks,/opt/amazon/certs/rds-combined-ca-bundle.pem,/tmp/g-ef1db6367ac2ca9900a1ec51e0610890dd85420b-2014450836307118787/script_2018-01-16-19-19-57.py --py-files /tmp/PyGlue.zip --driver-memory 5g --executor-memory 5g /tmp/runscript.py script_2018-01-16-19-19-57.py --JOB_NAME PropertyDim_Full_Refresh --JOB_ID j_50471421e9761b8bb5ab038777ad7d47ca763b820c11175075f667160acf650b --s3_bucket_sql_file move-dataeng-lstp-dev --s3_prefix_sql_file edw/propertydim_temp_scripts/property_dim_dedupe.sql --JOB_RUN_ID jr_9b9536625270bf934b4fd16afb589ac5019e1d1792b9ff228b8f6084ad403e08 --job-bookmark-option job-bookmark-disable --temp_table_name property_dim_All --s3_source_path s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim/year=2011/month=04/day=21/hour=07 --TempDir s3://move-dataeng-temp-dev/glue-results/mk/propertyfullrefresh/
s3_target_path="s3://move-dataeng-lstp-dev/edw/post-pdt/listingdim/listingdim-bd-full"

/usr/lib/spark/bin/spark-submit
--conf spark.hadoop.yarn.resourcemanager.connect.max-wait.ms=60000
--conf spark.hadoop.fs.defaultFS=hdfs://ip-172-31-38-180.us-west-2.compute.internal:8020
--conf spark.hadoop.yarn.resourcemanager.address=ip-172-31-38-180.us-west-2.compute.internal:8032
--conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true
--conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=18
--conf spark.executor.memory=5g
--conf spark.executor.cores=4
--name tape
--master yarn
--deploy-mode cluster
--jars /opt/amazon/superjar/glue-assembly.jar
--files /tmp/glue-default.conf,/tmp/glue-override.conf,/opt/amazon/certs/InternalAndExternalAndAWSTrustStore.jks,/opt/amazon/certs/rds-combined-ca-bundle.pem,/tmp/g-ef1db6367ac2ca9900a1ec51e0610890dd85420b-2014450836307118787/script_2018-01-16-19-19-57.py
--py-files /tmp/PyGlue.zip
--driver-memory 5g
--executor-memory 5g /tmp/runscript.py script_2018-01-16-19-19-57.py
--JOB_NAME PropertyDim_Full_Refresh
--JOB_ID j_50471421e9761b8bb5ab038777ad7d47ca763b820c11175075f667160acf650b
--s3_bucket_sql_file move-dataeng-lstp-dev
--s3_prefix_sql_file edw/propertydim_temp_scripts/property_dim_dedupe.sql
--JOB_RUN_ID jr_9b9536625270bf934b4fd16afb589ac5019e1d1792b9ff228b8f6084ad403e08
--job-bookmark-option job-bookmark-disable
--temp_table_name property_dim_All
--s3_source_path s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim/year=2011/month=04/day=21/hour=07
--TempDir s3://move-dataeng-temp-dev/glue-results/mk/propertyfullrefresh/
--s3_target_path="s3://move-dataeng-lstp-dev/edw/post-pdt/listingdim/listingdim-bd-full"

/usr/lib/spark/bin/spark-submit
s3://aws-glue-scripts-289154003759-us-west-2/mk/business-data-latest/code/listing_data_refresh_spark_custom.py
--args '{"--JOB_NAME":"ListingDim_Full_Refresh-3",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/mk/business-data-latest/code/listing_data_refresh_joins_custom.py",
"--glue_dpu_instance_count" :"100",
"--s3_target_path":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-bd-full_jan31",
"--s3_latest_usps_geo_data":"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt",
"--s3_latest_property_dim_data":"s3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-latest_jan25",
"--source_sql_file_path":"s3://aws-glue-scripts-289154003759-us-west-2/mk/business-data-latest/sqls/listing_dim_dedupe_full_refresh.sql",
"--s3_source_pdt_listing_data":"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim"}'

"""
# Extract the Glue Job Arguments
"""
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_source_path',
                           's3_target_path',
                           'source_sql_file_path',
                           'temp_table_name'])
print "Job Name is: ", args['JOB_NAME']
print "S3 Source File Path: ", args['s3_source_path']
print "S3 Target File Path: ", args['s3_target_path']
print "Source SQL File Path: ", args['source_sql_file_path']
print "Temp Table Name: ", args['temp_table_name']
s3_target_path=args['s3_target_path']
#Parse Bucket and Prefix of the SQL File Path
source_sql_path = urlparse(args['source_sql_file_path'])
s3_bucket_sql_file = source_sql_path.netloc
s3_prefix_sql_file = source_sql_path.path.lstrip('/')
"""

APP_NAME = 'JobNNN'
conf = SparkConf().setAppName(APP_NAME)
# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", APP_NAME) \
    .getOrCreate()

# Method to read S3 file as a string
def getStringFromFile(bucket_name, key):
    s3_client = boto3.client('s3', region_name='us-west-2')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read()
    return data

#1. Variables
s3_target_path="s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-latest-jan29"

#s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim

def read_parquet_and_create_dataframe_table(s3_source_path = None, temp_table_name = None):
    # Read Source File into a data frame
    df_par_All = spark.read.option("mergeSchema", "true").parquet(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All

def read_textfile_and_create_dataframe_table(s3_source_path = None, flg_file_header="true", temp_table_name = None):
    # Read Source File into a data frame
    #df_par_All = spark.read.option("header", "true").csv("s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt")
    df_par_All = spark.read.option("header", flg_file_header).csv(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All

def main():
    df_usps_geo_data = read_textfile_and_create_dataframe_table("s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt", "true", "edw_usps_geo_data")
    df_property_dim_All = read_parquet_and_create_dataframe_table('s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest', 'edw_properties_deduped')

    df_listing_dim_All = read_parquet_and_create_dataframe_table('s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim','listing_dim_All')

    # Read SQL Query file
    #sql_file = '/home/hadoop/post-pdt/pyspark/lstp/propertydim/SQLs/property_dim_dedupe.sql'
    #-->sql_text = getStringFromFile(s3_bucket_sql_file,s3_prefix_sql_file)
    sql_text = getStringFromFile('aws-glue-scripts-289154003759-us-west-2', 'mk/business-data/sqlfiles/listing_dim_dedupe_bd_addtn_joins_jan29.sql')
    print sql_text
    # Call the Spark SQL and create a new Data Frame
    Final_Data_Frame = spark.sql(sql_text)

    #Output the data from the dataframe
    Final_Data_Frame.write.mode('overwrite').save(s3_target_path)

if __name__ == "__main__":
    main()
