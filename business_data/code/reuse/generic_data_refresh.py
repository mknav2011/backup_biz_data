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

# Method to read S3 file as a string
def getStringFromFile(bucket_name, key):
    s3_client = boto3.client('s3', region_name='us-west-2')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read()
    return data

def delete_s3_files(s3_bucket, s3_prefix):
    s3_resource = boto3.resource('s3', region_name='us-west-2')
    objects_to_delete = s3_resource.meta.client.list_objects(Bucket=s3_bucket, Prefix=s3_prefix)
    delete_keys = {'Objects': []}
    i = 0
    for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]:
        if k != s3_prefix:
            delete_keys['Objects'].append({'Key': k})
            i = i + 1
            logging.info(k)
    if i > 0:
        s3_resource.meta.client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)

def copy_s3_files(source_s3_bucket, source_s3_prefix, target_s3_bucket, target_s3_prefix):
    s3_resource = boto3.resource('s3', region_name='us-west-2')
    objects_to_copy = s3_resource.meta.client.list_objects(Bucket=source_s3_bucket, Prefix=source_s3_prefix)
    for k in [obj['Key'] for obj in objects_to_copy.get('Contents', [])]:
        logging.info(k)
        if k != source_s3_prefix:
            copy_source = {
                'Bucket': source_s3_bucket,
                'Key': k
            }
            s3_resource.meta.client.copy_object(Bucket=target_s3_bucket, CopySource=copy_source, Key=target_s3_prefix+k.replace(source_s3_prefix,''))


'''
s3_target_path_temp = urlparse(s3_target_path)
s3_bucket_s3_target_path = s3_target_path_temp.netloc
s3_prefix_s3_target_path = s3_target_path_temp.path.lstrip('/')
s3_prefix_s3_target_temp_path = s3_prefix_s3_target_path + "bd_stage"

s3_target_path
prepare temp location
delete existing files from temp location
copy files from temp location to target location
'''

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

'''
read_textfile_file_and_create_dataframe_table("
s3://move-dataeng-dropbox-dev/adobe/adobe_omniture_homerealtor/raw-data-rd-uncompressed/languages/",
"s3://move-dataeng-omniture-dev/raw-data-rd-uncompressed/column_headers/generic_look_up_header/generic_look_up_header.tsv","\t","mk")

year=2017/month=08/day=01/hour=00
s3_source_path="s3://move-dataeng-dropbox-dev/adobe/adobe_omniture_homerealtor/raw-data-rd-uncompressed/languages/"
header_string = "key\tvalue"
header_data_delimiter="\t"
length_of_header=2
temp_table_name="mk"
header_file_path='s3://move-dataeng-omniture-dev/raw-data-rd-uncompressed/column_headers/generic_look_up_header/generic_look_up_header.tsv'

'''

def read_textfile_file_and_create_dataframe_table(s3_source_path = None, header_file_path=None, header_data_delimiter=",", temp_table_name = None):
    header_file_path = urlparse(header_file_path)
    s3_bucket_sql_file = header_file_path.netloc
    s3_prefix_sql_file = header_file_path.path.lstrip('/')
    header_string = getStringFromFile(s3_bucket_sql_file, s3_prefix_sql_file)
    header_string = header_string.strip()
    length_of_header = len(header_string.split(header_data_delimiter))
    # Read Source File into a RDD
    raw_data = spark.read.text(s3_source_path).rdd.map(lambda l: l)
    # Pick only records where number of data columns matches to number of header columns
    data_rows = raw_data.map(lambda l: l[0].split(header_data_delimiter)).filter(lambda l : len(l) == length_of_header )
    #Generate Schema out of header row
    fields = [StructField(field_name, StringType(), True) for field_name in header_string.split(header_data_delimiter)]
    schema = StructType(fields)
    # Create Data Frame
    df_data_rows_by_column = spark.createDataFrame(data_rows, schema)

    # Create Temp Table
    df_data_rows_by_column.createOrReplaceTempView(temp_table_name)

    #Final_Data_Frame = spark.sql("select * from mk limit 10")
    #read_textfile_file_and_create_dataframe_table(s3_source_path="s3://move-dataeng-dropbox-dev/adobe/adobe_omniture_homerealtor/raw-data-rd-uncompressed/languages/",header_file_path="s3://move-dataeng-omniture-dev/raw-data-rd-uncompressed/column_headers/generic_look_up_header/generic_look_up_header.tsv",header_data_delimiter="\t",temp_table_name="temp_tbl_two_column_lookup")


# Extract the Glue Job Arguments
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_target_path',
                           'data_frame_sources',
                           'inputvariables',
                           'source_sql_file_path',
                           'target_partition_by_column',
                           'block_size_MB'])

#1. Variables
job_name = args['JOB_NAME']
s3_target_path = args['s3_target_path']
data_frame_sources = args['data_frame_sources']
inputvariables = args['inputvariables']
source_sql_path = urlparse(args['source_sql_file_path'])
target_partition_by_column = args['target_partition_by_column']
block_size_MB = args['block_size_MB']

s3_target_path_temp = urlparse(s3_target_path)
s3_bucket_s3_target_path = s3_target_path_temp.netloc
s3_prefix_s3_target_path = s3_target_path_temp.path.lstrip('/')

s3_stage_temp_path = s3_target_path + "bd_stage"
s3_prefix_s3_stage_temp = urlparse(s3_stage_temp_path).path.lstrip('/')


#Block size if required for data set with more number of columns
if type(block_size_MB) == int:
    if int(block_size_MB) > 0:
       block_size = str(1024 * 1024 * int(block_size_MB))
    else:
       block_size = str(1024 * 1024 * 128)
else:
    block_size = str(1024 * 1024 * 128)

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
    .config("dfs.blocksize", block_size) \
    .config("parquet.block.size", block_size) \
    .getOrCreate()


def main():

    try:
        sql_text = getStringFromFile(s3_bucket_sql_file, s3_prefix_sql_file)

        for i in json_inputvariables:
            for key, value in i.iteritems():
                sql_text = sql_text.replace(key, str(value))

        print sql_text
        print "Before Json Load, data_frame_sources"
        print data_frame_sources
        json_data_frame_sources = json.loads(data_frame_sources)
        print "After Json Load, data_frame_sources"

        print json_data_frame_sources

        for var_data_frame in json_data_frame_sources:
            if var_data_frame["source_file_type_parquet_or_text"] == "csv":
                print "csv"
                read_csv_file_and_create_dataframe_table(var_data_frame["s3_source_path"], "true", var_data_frame["temp_table_name"])
            elif var_data_frame["source_file_type_parquet_or_text"] == "parquet":
                print "parquet"
                read_parquet_and_create_dataframe_table(var_data_frame["s3_source_path"], var_data_frame["temp_table_name"])
            elif var_data_frame["source_file_type_parquet_or_text"] == "text":
                print "text"
                read_textfile_file_and_create_dataframe_table(s3_source_path=var_data_frame["s3_source_path"],header_file_path=var_data_frame["header_file_path"],header_data_delimiter=var_data_frame["header_data_delimiter"],temp_table_name=var_data_frame["temp_table_name"])

        # Call the Spark SQL and create a new Data Frame
        final_data_frame = spark.sql(sql_text)

        #s3_stage_temp_path = s3_target_path + "bd_stage"
        #s3_prefix_s3_stage_temp = urlparse(s3_stage_temp_path).path.lstrip('/')

        # Cleanup BD files from the Stage location if anything exists
        delete_s3_files(s3_bucket_s3_target_path, s3_prefix_s3_stage_temp)

        #Output the data from the dataframe to a Stage location
        final_data_frame.write.mode('overwrite').save(s3_stage_temp_path)

        # Copy output files from Stage location to Target
        copy_s3_files(s3_bucket_s3_target_path, s3_prefix_s3_stage_temp, s3_bucket_s3_target_path, s3_prefix_s3_target_path)

        # Delete files from Stage location
        delete_s3_files(s3_bucket_s3_target_path, s3_prefix_s3_target_temp_path)

        #if target_partition_by_column.upper() <> "NONE":
        #    final_data_frame.write.mode('overwrite').partitionBy(target_partition_by_column).save(s3_target_path)
        #else:
        #    final_data_frame.write.mode('overwrite').save(s3_target_path)

    except Exception, err:
        sys.stderr.write('Failed while running the Glue Job %sn : %sn' % (str(job_name),str(err)))
        raise Exception('Failed while running the Glue Job %sn : %sn' % (str(job_name),str(err)))
        return 1

if __name__ == "__main__":
    main()
