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



APP_NAME = 'Customer Generic'
conf = SparkConf().setAppName(APP_NAME)
# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL For Customer Load") \
    .enableHiveSupport() \
    .config("spark.some.config.option", APP_NAME) \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .getOrCreate()

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
print job_name
s3_target_path = args['s3_target_path']
print s3_target_path
data_frame_sources = args['data_frame_sources']
print data_frame_sources
inputvariables = args['inputvariables']
print inputvariables
source_sql_path = urlparse(args['source_sql_file_path'])
print source_sql_path
target_partition_by_column = args['target_partition_by_column']
print target_partition_by_column


data_frame_sources = data_frame_sources.replace("'",'"')
inputvariables=inputvariables.replace("'",'"')
json_inputvariables = json.loads(inputvariables)
json_data_frame_sources = json.loads(data_frame_sources)

#Parse Bucket and Prefix of the SQL File Path
s3_bucket_sql_file = source_sql_path.netloc
s3_prefix_sql_file = source_sql_path.path.lstrip('/')


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

    try:

        sql_text = getStringFromFile(s3_bucket_sql_file, s3_prefix_sql_file)

        for i in json_inputvariables:
            for key, value in i.iteritems():
                sql_text = sql_text.replace(key, str(value))

        print sql_text

        print json_data_frame_sources

        for var_data_frame in json_data_frame_sources:
            if var_data_frame["source_file_type_parquet_or_text"] == "csv":
                print "csv"
                read_textfile_and_create_dataframe_table(var_data_frame["s3_source_path"], "true", var_data_frame["temp_table_name"])
            elif var_data_frame["source_file_type_parquet_or_text"] == "parquet":
                print "parquet"
                read_parquet_and_create_dataframe_table(var_data_frame["s3_source_path"], var_data_frame["temp_table_name"])

        # Call the Spark SQL and create a new Data Frame
        final_data_frame = spark.sql(sql_text)

        #Output the data from the dataframe
        final_data_frame.write.mode('overwrite').save(s3_target_path)

        #if target_partition_by_column.upper() <> "NONE":
        #    final_data_frame.write.mode('overwrite').partitionBy(target_partition_by_column).save(s3_target_path)
        #else:
        #    final_data_frame.write.mode('overwrite').save(s3_target_path)

    except Exception, err:
        sys.stderr.write('Failed while running the Glue Job %sn : %sn' % str(job_name),str(err))
        raise Exception('Failed while running the Glue Job %sn : %sn' % str(job_name),str(err))
        return 1

if __name__ == "__main__":
    main()
