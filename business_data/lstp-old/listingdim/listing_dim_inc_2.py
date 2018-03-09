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

#1. Variables
s3_location_target = 's3://move-dataeng-lstp-dev/edw/post-pdt/listingdim/listingdim_deduped_stage'


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
source_s3_inbund_s3files_location_list=["s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year=2017/month=07/day=15/hour=01/*","s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year=2017/month=06/day=16/hour=08/*"]

# Read Latest ListingDim Data & Incrementals (Prep. the folder paths from LGK or some way)
df_par=spark.read.parquet(*source_s3_inbund_s3files_location_list)

# Create a Table
df_par.createOrReplaceTempView("listing_dim");

# *** From Last load, get the Max EffectiveTo other than 9999-12-31 ****

'''
Extract distinct YYYY-mm-DD where EffectiveTo<>"9999-12-31"
Add all the partitions in a loop and append the s3 file list to "source_s3_file_list" variable

'''
#3. Read the Query file
sql_file = '/home/hadoop/post-pdt/pyspark/lstp/listingdim/SQLs/listing_dim_unique_effectiveto_dates.sql'
sql_text = open(sql_file).read()
print sql_text

# De-Dupe them and pick the latest (Partition By ListingDimKey, EffectiveTo in Ascending order)
distinct_effectiveto_dates_not_99991231 = spark.sql(sql_text)

print distinct_effectiveto_dates_not_99991231

#TODO, Add SQL files for the identified days

try:
    for datestring in distinct_effectiveto_dates_not_99991231.collect():
        tempmstdate = datestring['effectiveto']
        tempdateStringYYYY = tempmstdate.strftime("%Y")
        tempdateStringmm = tempmstdate.strftime("%m")
        tempdateStringDD = tempmstdate.strftime("%d")
        source_s3_inbund_batch_location_list.append("s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year="+tempdateStringYYYY+"/month="+tempdateStringmm+"/day="+tempdateStringDD+"/*")
except Exception, err:
    sys.stderr.write('effectiveto data extract : %sn' % str(err))
    raise Exception('effectiveto data extract : %sn' % str(err))
    return 1

#3. Read the Query file
sql_file = '/home/hadoop/post-pdt/pyspark/lstp/listingdim/SQLs/listing_dim_dedupe.sql'
sql_text = open(sql_file).read()
print sql_text

df_par_All=spark.read.parquet(
    *source_s3_inbund_s3files_location_list
)

df_par_All.createOrReplaceTempView("listing_dim_All");

# De-Dupe them and pick the latest (Partition By ListingDimKey, EffectiveTo in Descending order)
df_EffectiveTo_99991231 = spark.sql(sql_text)

df_EffectiveTo_99991231.write.mode('overwrite').partitionBy("effectiveto_date").parquet(s3_location_target)

"""
Create a EffectiveTo = 9999-12-31 to a temp location and then swp the Partitions for Athena Table

"""


# Refresh Athena Table Schema
