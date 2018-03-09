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

from awsglue.utils import getResolvedOptions # For extracting Glue Arguments

from urlparse import urlparse # For URL Parsing

from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

"""
python glue_wrapper.py --args '{"--JOB_NAME":"ListingDim_Full_Refresh-2010",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/code/custom_lstp/listing_data_refresh_glue_custom.py",
"--glue_dpu_instance_count" :"90",
"--s3_target_path":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/year=2010",
"--s3_latest_usps_geo_data":"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt",
"--s3_latest_property_dim_data":"s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest",
"--s3_source_historical_listing_dim_key_data":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/",
"--s3_source_pdt_listing_data":"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim",
"--source_sql_file_path":"s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/sqls/lstp/biz_data_listings/listing_dim_dedupe_full_refresh_glue_year2010.sql"}'

python glue_wrapper.py --args '{"--JOB_NAME":"ListingDim_Full_Refresh-latest",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/code/custom_lstp/listing_data_refresh_glue_custom.py",
"--glue_dpu_instance_count" :"90",
"--s3_target_path":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=latest/year=9999",
"--s3_latest_usps_geo_data":"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt",
"--s3_latest_property_dim_data":"s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest",
"--s3_source_historical_listing_dim_key_data":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/",
"--s3_source_pdt_listing_data":"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim",
"--source_sql_file_path":"s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/sqls/lstp/biz_data_listings/listing_dim_dedupe_full_refresh_glue_latest.sql"}'
"""



def read_parquet_and_create_dataframe_table(s3_source_path = None, temp_table_name = None):
    # Read Source File into a data frame
    df_par_All = spark.read.option("mergeSchema", "true").parquet(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All

def read_csv_file_and_create_dataframe_table(s3_source_path = None, flg_file_header="true", temp_table_name = None):
    # Read Source File into a data frame
    #df_par_All = spark.read.option("header", "true").csv("s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt")
    df_par_All = spark.read.option("header", flg_file_header).csv(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All

def read_textfile_file_and_create_dataframe_table(s3_source_path = None, header_file_path=None, header_data_delimiter=",", temp_table_name = None):
s3_source_path="s3://move-dataeng-dropbox-dev/adobe/adobe_omniture_homerealtor/raw-data-rd-uncompressed/languages/year=2017/month=08/day=01/hour=00"
header_string = "key\tvalue"
header_data_delimiter="\t"
length_of_header=2
raw_data = spark.read.text(s3_source_path).rdd.map(lambda l: l)
data_rows = raw_data.map(lambda l: l[0].split("\t")).filter(lambda l : len(l) == length_of_header )

fields = [StructField(field_name, StringType(), True) for field_name in header_string.split(header_data_delimiter)]
schema = StructType(fields)

df_data_rows_by_column = spark.createDataFrame(data_rows, schema)

for header_column in header_string.split(header_data_delimiter):
    # Assign the column names by position
    if data_columns <> '':
        data_columns = data_columns + "," + header_column + "=" + "r["+str(col_count)+"]"
    else:
        data_columns = header_column + "=" + "r["+str(col_count)+"]"
    col_count += 1

temp="Row("+data_columns+")"
data_rows_by_column = data_rows.map(lambda r: temp)
data_rows_by_column = data_rows.map(lambda r: Row(data_columns))
data_rows_by_column = data_rows.map(lambda r: Row(key=r[0],value=r[1]))
data_rows_by_column.take(5)

df_data_rows_by_column = spark.createDataFrame(data_rows_by_column)
    schemadatafile = sqlContext.createDataFrame(data_rows_by_column)

    # Register Events data frame as 'Events' table

    schemaEventfile.registerTempTable("tblEvents")

# Method to read S3 file as a string
def getStringFromFile(bucket_name, key):
    s3_client = boto3.client('s3', region_name='us-west-2')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read()
    return data

# Extract the Glue Job Arguments
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_target_path',
                           'data_frame_sources',
                           'inputvariables',
                           'source_sql_file_path',
                           'target_partition_by_column'])

#1. Variables
job_name = args['JOB_NAME']
s3_target_path = args['s3_target_path']
data_frame_sources = args['data_frame_sources']
inputvariables = args['inputvariables']
source_sql_path = urlparse(args['source_sql_file_path'])
target_partition_by_column = args['target_partition_by_column']

#data_frame_sources = data_frame_sources.replace("'",'"')
#inputvariables=inputvariables.replace("'",'"')
json_inputvariables = json.loads(inputvariables)
json_data_frame_sources = json.loads(data_frame_sources)

#Parse Bucket and Prefix of the SQL File Path
s3_bucket_sql_file = source_sql_path.netloc
s3_prefix_sql_file = source_sql_path.path.lstrip('/')


APP_NAME = job_name
conf = SparkConf().setAppName(APP_NAME)
# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", APP_NAME) \
    .getOrCreate()



def main():

    try:
        sql_text = getStringFromFile(s3_bucket_sql_file, s3_prefix_sql_file)

        for i in json_inputvariables:
            for key, value in i.iteritems():
                print key
                if value == 'GETDATE():format:%Y-%m-%d':
                    value = datetime.datetime.now().strftime("%Y-%m-%d")
                elif value == 'GETDATE():format:%Y-%m-%d %H:%M':
                    value = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                sql_text = sql_text.replace(key, str(value))

        print sql_text

        json_data_frame_sources = json.loads(data_frame_sources)

        print json_data_frame_sources

        for var_data_frame in json_data_frame_sources:
            if var_data_frame["source_file_type_parquet_or_text"] == "csv":
                print "csv"
                read_csv_file_and_create_dataframe_table(var_data_frame["s3_source_path"], "true", var_data_frame["temp_table_name"])
            elif var_data_frame["source_file_type_parquet_or_text"] == "parquet":
                print "parquet"
                read_parquet_and_create_dataframe_table(var_data_frame["s3_source_path"], var_data_frame["temp_table_name"])

        # Call the Spark SQL and create a new Data Frame
        Final_Data_Frame = spark.sql(sql_text)

        #Output the data from the dataframe
        if target_partition_by_column.upper() <> "NONE":
            Final_Data_Frame.write.mode('overwrite').partitionBy(target_partition_by_column).save(s3_target_path)
        else:
            Final_Data_Frame.write.mode('overwrite').save(s3_target_path)

    except Exception, err:
        sys.stderr.write('Failed while running the Glue Job %sn : %sn' % str(job_name),str(err))
        raise Exception('Failed while running the Glue Job %sn : %sn' % str(job_name),str(err))
        return 1

if __name__ == "__main__":
    main()
