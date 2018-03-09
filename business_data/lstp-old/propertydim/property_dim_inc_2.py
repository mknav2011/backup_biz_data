#!/usr/bin/python
import os, sys, json, uuid
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, *
from pyspark.sql.functions import udf
from pyspark.sql.functions import input_file_name

from pyspark.sql import SparkSession

import time, datetime
from dateutil.parser import parse
debug = 1

import boto3, botocore
client = boto3.client('s3')

#1. Variables
s3_location_target = 's3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-1'

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
spark = SparkSession \
     .builder \
     .appName("Python Spark SQL basic example") \
     .config("spark.some.config.option", "ListingDim-Initial-Load") \
     .getOrCreate()

source_s3_inbund_s3files_location_list=[]
#TODO, Extract below list from LGK
source_s3_inbund_s3files_location_list=["s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim/year=2011/month=04/day=21/hour=07","s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim/year=2012/month=04/day=23/hour=04"]

for partition in source_s3_inbund_s3files_location_list:
    print "A"
    partition_list = []
    partition_list.append(partition)
    print partition_list
    #start Loop
    # Read Latest ListingDim Data & Incrementals (Prep. the folder paths from LGK or some way)
    df_par = spark.read.parquet(*partition_list)
    # Create a Table
    df_par.createOrReplaceTempView("property_dim");
    # 3. Read the Query file
    sql_file = '/home/hadoop/post-pdt/pyspark/lstp/propertydim/SQLs/property_dim_unique_effectiveto_dates.sql'
    sql_text = open(sql_file).read()
    print "B"
    print sql_text
    # De-Dupe them and pick the latest (Partition By ListingDimKey, EffectiveTo in Ascending order)
    distinct_effectiveto_dates_not_99991231 = spark.sql(sql_text)
    print distinct_effectiveto_dates_not_99991231
    print "C"
    # TODO, Add SQL files for the identified days
    try:
        for datestring in distinct_effectiveto_dates_not_99991231.collect():
            tempmstdate = datestring['effectiveto']
            #print tempmstdate
            tempdateStringYYYY = tempmstdate.strftime("%Y")
            tempdateStringmm = tempmstdate.strftime("%m")
            tempdateStringDD = tempmstdate.strftime("%d")
            temp_effectiveto_partition="s3://move-dataeng-lstp-dev/edw/post-pdt/propertydim/propertydim-bd-1/year=" + tempdateStringYYYY + "/month=" + tempdateStringmm + "/day=" + tempdateStringDD + "/hour=00/"
            print "D"
            result = client.list_objects(Bucket="move-dataeng-lstp-dev", Prefix="edw/post-pdt/propertydim/propertydim-bd-1/year=" + tempdateStringYYYY + "/month=" + tempdateStringmm + "/day=" + tempdateStringDD + "/hour=00/")
            print "Loop-3"
            try:
                if result["Contents"]:
                    partition_list.append(temp_effectiveto_partition+'*')
                    print "Adding"
            except:
                print "ZZZ"
                pass
            print "E"
    except Exception, err:
        sys.stderr.write('effectiveto data extract : %sn' % str(err))
        raise Exception('effectiveto data extract : %sn' % str(err))
    print "F"
    df_par_All = spark.read.parquet(*partition_list)
    df_par_All.createOrReplaceTempView("property_dim_All")
    # 3. Read the Query file
    sql_file = '/home/hadoop/post-pdt/pyspark/lstp/propertydim/SQLs/property_dim_dedupe.sql'
    sql_text = open(sql_file).read()
    print sql_text
    df_EffectiveTo_99991231 = spark.sql(sql_text)
    df_EffectiveTo_99991231.write.mode('overwrite').partitionBy("year","month","day","hour").parquet(s3_location_target)
    # End Loop


# *** From Last load, get the Max EffectiveTo other than 9999-12-31 ****

'''
Extract distinct YYYY-mm-DD where EffectiveTo<>"9999-12-31"
Add all the partitions in a loop and append the s3 file list to "source_s3_file_list" variable

'''







# De-Dupe them and pick the latest (Partition By ListingDimKey, EffectiveTo in Descending order)


"""
Create a EffectiveTo = 9999-12-31 to a temp location and then swp the Partitions for Athena Table

"""


# Refresh Athena Table Schema
