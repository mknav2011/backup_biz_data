#-- Glue specific imports
from pyspark.sql.types import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import SparkSession
import json
#------------ Input section -----
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_location_source', 
                           's3_location_target',
                           'files_per_partition'] )

s3_location_source = args['s3_location_source']
s3_location_target = args['s3_location_target']
files_per_partition = int(args['files_per_partition'])

#------------

#conf = SparkConf().setAppName("parquet_testing").set("spark.yarn.executor.memoryOverhead", "2000").set("spark.executor.memory", "12g")

sc = SparkContext()
#print sc.conf.get("spark.executor.memory")
#print sc.conf.get("spark.yarn.executor.memoryOverhead")

sqlContext = HiveContext(sc)

df_par= sqlContext.read.option("mergeSchema", "true").parquet(s3_location_source)

df_par.printSchema()
block_size = str(1024 * 1024 * 512)
sc._jsc.hadoopConfiguration().set("dfs.block.size", block_size)
sc._jsc.hadoopConfiguration().set("parquet.block.size", block_size)


output_folder = s3_location_target # With absolute path
codec='snappy'
partitionby=['year', 'month','day', 'hour']
#df = df_with_partitions.filter((df_with_partitions.hour.cast('Integer') == 11 ))
#df_with_partitions.write.partitionBy("hour").mode('overwrite').parquet(output_folder, compression=codec) 
df_par.coalesce(files_per_partition).write.mode('overwrite').parquet(output_folder, compression=codec) 
