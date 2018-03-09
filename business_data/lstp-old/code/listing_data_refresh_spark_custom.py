#!/usr/bin/python
import os, sys, json, uuid
import argparse
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


#from awsglue.utils import getResolvedOptions # For extracting Glue Arguments

from urlparse import urlparse # For URL Parsing

"""
/usr/lib/spark/bin/spark-submit s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/code/custom_lstp/listing_data_refresh_spark_custom.py --args '{"--JOB_NAME":"ListingDim_Full_Refresh","--s3_target_path":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-bd-full_spark/part1","--s3_latest_usps_geo_data":"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt","--s3_latest_property_dim_data":"s3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-latest_jan25","--source_sql_file_path":"s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/sqls/lstp/biz_data_listings/sqls/lstp/biz_data_listings/listing_dim_dedupe_full_refresh_part1.sql","--s3_source_pdt_listing_data":"s3://move-dataeng-lstp-prod/edw/processed-data-xact-bkp-01-31-18/listing_dim"}'

/usr/lib/spark/bin/spark-submit s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/code/custom_lstp/listing_data_refresh_spark_custom.py --args '{"--JOB_NAME":"ListingDim_Full_Refresh","--s3_target_path":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-bd-full_spark-All","--s3_latest_usps_geo_data":"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt","--s3_latest_property_dim_data":"s3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-latest_jan25","--source_sql_file_path":"s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/sqls/lstp/biz_data_listings/listing_dim_dedupe_full_refresh.sql","--s3_source_pdt_listing_data":"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim"}'


"""

APP_NAME = 'ListingDim_Spark_Refresh'
conf = SparkConf().setAppName(APP_NAME)

block_size = str(1024 * 1024 * 512)

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", APP_NAME) \
    .config("dfs.blocksize", block_size) \
    .config("parquet.block.size", block_size) \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# Method to read S3 file as a string
def getStringFromFile(bucket_name, key):
    s3_client = boto3.client('s3', region_name='us-west-2')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read()
    return data

# Extract the Glue Job Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--args", help="Spark Job Arguments as a Dictionary")
input_arguments = parser.parse_args()
if input_arguments.args:
    var_args = json.loads(input_arguments.args)

#1. Variables
s3_target_path = var_args['--s3_target_path']
s3_latest_usps_geo_data = var_args['--s3_latest_usps_geo_data']
s3_latest_property_dim_data = var_args['--s3_latest_property_dim_data']
s3_source_pdt_listing_data = var_args['--s3_source_pdt_listing_data']
source_sql_path = urlparse(var_args['--source_sql_file_path'])

#Parse Bucket and Prefix of the SQL File Path
s3_bucket_sql_file = source_sql_path.netloc
s3_prefix_sql_file = source_sql_path.path.lstrip('/')

"""
s3_target_path="s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-latest"
s3_latest_usps_geo_data="s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt"
s3_latest_property_dim_data="s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest"
s3_source_pdt_listing_data="s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim"
"""

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
    df_usps_geo_data = read_textfile_and_create_dataframe_table(s3_latest_usps_geo_data, "true", "edw_usps_geo_data")
    df_property_dim_All = read_parquet_and_create_dataframe_table(s3_latest_property_dim_data, 'edw_properties_deduped')

    df_listing_dim_All = read_parquet_and_create_dataframe_table(s3_source_pdt_listing_data,'listing_dim_All')

    # Read SQL Query file
    #sql_file = '/home/hadoop/post-pdt/pyspark/lstp/propertydim/SQLs/property_dim_dedupe.sql'
    sql_text = getStringFromFile(s3_bucket_sql_file,s3_prefix_sql_file)
    #sql_text = getStringFromFile('aws-glue-scripts-289154003759-us-west-2', 'mk/business-data/sqlfiles/listing_dim_dedupe_bd_addtn_joins.sql')
    print sql_text
    # Call the Spark SQL and create a new Data Frame
    Final_Data_Frame = spark.sql(sql_text)

    #Output the data from the dataframe
    Final_Data_Frame.write.mode('overwrite').save(s3_target_path)

if __name__ == "__main__":
    main()
