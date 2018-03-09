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

import boto3, botocore, logging
client = boto3.client('s3')

from io import BytesIO, StringIO

from awsglue.utils import getResolvedOptions

**************************
**************************

import boto3, sys, json, time, uuid, datetime, botocore, re
import threading
from multiprocessing.pool import ThreadPool
import traceback



"""
Approach:
1. Read current PDT Data for required sources according to LGK
2. For each sources get LGK entries
3. Modify the SQL query with the LGK data filters
4. Run the query

"""

class GlueClass(object):
    region_name = 'us-west-2'
    client = boto3.client(service_name='glue', region_name='us-west-2', endpoint_url='https://glue.us-west-2.amazonaws.com')

    s3_staging_folder_csv = None
    s3_target_folder_parquet = None
    glue_script_location = None
    glue_job_name = None
    glue_role = None
    util = None
    glue_job = None

    def __init__(self, s3_staging_folder_csv, s3_target_folder_parquet, glue_role, glue_script_location):
        """ constructor requires s3 staging folder for storing results
        Parameters:
        s3_staging_folder = s3 folder with write permissions for storing results
        """
        print 's3_staging_folder_csv=', s3_staging_folder_csv
        print 's3_target_folder_parquet = ', s3_target_folder_parquet
        self.s3_staging_folder_csv = s3_staging_folder_csv
        self.s3_target_folder_parquet = s3_target_folder_parquet
        #         self.sql_query = sql_query
        # Athena Initialization
        self.util = AthenaUtil(s3_staging_folder=self.s3_staging_folder_csv)
        # Glue Create Job derive from s3_target_folder_parquet
        job_name = '.'.join([re.sub('[^0-9a-zA-Z]+', '', x).title() for x in
                             s3_target_folder_parquet.replace('s3://', '').split('/')[1:]])
        self.glue_job_name = job_name  # 'athena_ctas_part2'
        glue_job = self.client.create_job(Name=self.glue_job_name, Role=glue_role,
                                          Command={'Name': 'glueetl',
                                                   'ScriptLocation': glue_script_location})

        #self.glue_script_location = glue_script_location  # 's3://move-dataeng-temp-dev/glue-etl/scripts/athena_ctas_part2.py'#MK2

    def wait_for_job_to_complete(self, JobRunId):
        """ waits for query to execute """
        text = 'Waiting for JobName = %s and  JobId=%s  to Complete processing ...' % (self.glue_job_name, JobRunId)
        print text
        #         self.banner(text)
        status = "STARTING"  # assumed
        error_count = 0
        response = None
        response = self.client.get_job_run(JobName=self.glue_job_name, RunId=JobRunId)
        status = response["JobRun"]["JobRunState"]
        while (status in (
        "QUEUED','RUNNING, STARTING")):  # 'JobRunState': 'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED',
            try:
                response = self.client.get_job_run(JobName=self.glue_job_name, RunId=JobRunId)
                status = response["JobRun"]["JobRunState"]
                # my_print(status)
                time.sleep(0.5)
            except botocore.exceptions.ClientError as ce:

                error_count = error_count + 1
                if (error_count > 3):
                    status = "FAILED"
                    print(str(ce))
                    break  # out of the loop
                if "ExpiredTokenException" in str(ce):
                    self.client = boto3.session.Session(region_name=self.region_name).client('glue')

        if (status == "FAILED" or status == "STOPPED"):
            # print(response)
            pass

        if response is None:
            return {"SUCCESS": False,
                    "STATUS": status}
        else:
            return response

    def job_cleanup(self):
        self.banner("Job Cleanup")
        return self.client.delete_job(JobName = self.glue_job_name)

    def glue_etl_execute_job(self, target_table_name):
        text = 'Starting glue_etl_execute_job process  for %s ....' % (self.glue_job_name)
        print text
        print 's3_staging_folder_csv=', self.s3_staging_folder_csv
        print 's3_target_folder_parquet = ', self.s3_target_folder_parquet
        print ''
        #         self.banner(text)
        response = self.client.start_job_run(JobName=self.glue_job_name, Arguments={
            '--s3_location_csv_file': self.s3_staging_folder_csv,
            '--s3_location_parquet_file': self.s3_target_folder_parquet,
            '--table_name': target_table_name})
        return response

    #         if self.wait_for_job_to_complete(response['JobRunId']):
    #             return True
    #         else:
    #             return False
    def job_cleanup(self):
        self.banner("Job Cleanup")
        return self.client.delete_job(JobName=self.glue_job_name)

    @staticmethod
    def banner(text, ch='=', length=78):
        spaced_text = ' %s ' % text
        banner = spaced_text.center(length, ch)
        return banner

#1. Define Stage
s3_output_dir = "s3://move-dataeng-dev/stage/"
s3_bucket_name = 'move-dataeng-temp'
env = 'dev'
etl_layer = 'glue-etl/athena_csv'
target_table_name = 'cnpd_pagedim_test'
sql_file = '../sql/cnpd_pdtp_page_dim.sql'
s3_target_folder_parquet = 's3://move-dataeng-temp-dev/glue-etl/parquet_data/cnpd_pagedim_test'
glue_role = 'move-dataeng-emr-role'
glue_script_location = 's3://move-dataeng-temp-dev/glue-etl/scripts/athena_ctas_part2.py'

#2. Create RunId
run_instance = str(uuid.uuid1())

#3. Read the Query file
sql_text = open(sql_file).read()
#AWS Glue is a fully managed ETL (extract, transform, and load) service that makes it simple and cost-effective to categorize your data, clean it, enrich it, and move it reliably between various data stores.

#4. Prepare Stage S3 Folder (Athena output is staged here in csv format)
s3_staging_folder_csv = "s3://%s-%s/%s/%s/%s" % (s3_bucket_name, env, etl_layer, target_table_name, run_instance)

#5. Create instance of AthenaCTAS
"""
This initializes Athena Util, Glue Client, Create Glue Job instance
"""
ctas = GlueClass(s3_staging_folder_csv, s3_target_folder_parquet, glue_role, glue_script_location)

#6. Run Athena Util with the SQL Script, which exports the output into given Stage S3 folder
ctas.athena_query_execute_save_s3(sql_text)

#7. Start Glue Job created in Step #5, with the Source and Target locations and capture the "response"
response = ctas.glue_etl_execute_job(target_table_name)
print response

#8. Capture the 'JobRunId' from the "response" which can be used to delete the Glue job
glue_job_run_id = response['JobRunId']

#9. Wait for the Glue Job to Complete by checking the status recursively.
ctas.wait_for_job_to_complete(glue_job_run_id )

#10. Delete the Glue job from the 'JobRunId' extracted in step #8.
print ("Start, Clean up Glue Job")
ctas.job_cleanup()
print ("Finish, Clean up Glue Job")


#athena = AthenaUtil(s3_staging_folder=s3_staging_dir)
#result = athena.execute_save_s3(sql_text,s3_output_dir)
#print result
#df = athena.get_pandas_frame(result)
#print(df)
**************************
**************************


"""
--JOB_NAME:PropertyDim_Full_Refresh
--s3_source_path:s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim
--s3_target_path:s3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-full
--s3_bucket_sql_file:move-dataeng-lstp-dev
--s3_prefix_sql_file:edw/propertydim_temp_scripts/property_dim_dedupe.sql
--temp_table_name:property_dim_All

"""
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_source_path',
                           's3_target_path',
                           'sql_file_path'])
print "Job Name is: ", args['JOB_NAME']
print "S3 Source File Path: ", args['s3_source_path']
print "S3 Target File Path: ", args['s3_target_path']
print "SQL File Path: ", args['s3_bucket_sql_file']
print "SQL File Path: ", args['s3_prefix_sql_file']
print "Temp Table Name: ", args['temp_table_name']


#s3://aws-glue-scripts-289154003759-us-west-2/mk/property_full_refresh.py
# Created by Cassidy:
# The purpose of s3 helper is to help out duing the s3 processing
# Some more complicated process such as uploading a string to s3 files
# Get the need to process list from a specific bucket
# Deleting the files from a folder in s3
# will be handled here and can be reused for in multiple places

def getStringFromFile(bucket_name, key):
    s3_client = boto3.client('s3', region_name='us-west-2')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read()
    return data


#1. Variables
s3_location_target = args['s3_target_path']#'s3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-full_spark220_spark2/'

#s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim
"""
ToDo:
Reviews
[Murali]:
1. Create a EffectiveTo = 9999-12-31 to a temp location and then swp the Partitions for Athena Table
2. Store the MaxEffectiveTo in DynamoDB  or as a text file in S3 (Prefer: s3)
3. Time Taken for 2 months of data '2017-06', '2017-07'
4. Capture the Start and End Timings for each step

N. Remove Data De-Dupes for 9999-12-31 records
[Team]:
1.
"""

def main():
    APP_NAME = args['JOB_NAME']
    conf = SparkConf().setAppName(APP_NAME)
    # # conf = conf.setMaster("spark://ip-10-193-146-151.us-west-2.compute.internal:7077")
    # #######conf = conf.setMaster("spark://ip-10-193-129-66.us-west-2.compute.internal:7077")
    # #conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    #conf.set("spark.default.parallelism", "500")
    #conf.set("spark.memory.fraction", "0.90")
    #conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark = SparkSession \
         .builder \
         .appName("Python Spark SQL basic example") \
         .config("spark.some.config.option", args['JOB_NAME']) \
         .getOrCreate()
    source_s3_inbund_s3files_location_list=[]
    #TODO, Extract below list from LGK
    #source_s3_inbund_s3files_location_list=["s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim"]
    print "A"
    partition_list = []
    partition_list.append(partition)
    print partition_list
    #start Loop
    # Read Latest ListingDim Data & Incrementals (Prep. the folder paths from LGK or some way)
    df_par_All = spark.read.option("mergeSchema", "true").parquet(args['s3_source_path'])
    df_par_All.createOrReplaceTempView(args['temp_table_name'])
    # 3. Read the Query file
    #sql_file = '/home/hadoop/post-pdt/pyspark/lstp/propertydim/SQLs/property_dim_dedupe.sql'
    sql_text = getStringFromFile(args['s3_bucket_sql_file'],args['s3_prefix_sql_file'])
    #sql_text = open(sql_file).read()
    print sql_text
    Final_Data_Frame = spark.sql(sql_text)
    Final_Data_Frame.write.mode('overwrite').save(s3_location_target)
    # End Loop

if __name__ == "__main__":
    main()
