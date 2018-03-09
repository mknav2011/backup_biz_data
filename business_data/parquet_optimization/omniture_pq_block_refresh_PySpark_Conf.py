from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
import json 
conf = SparkConf().setAppName("parquet_testing").set("spark.yarn.executor.memoryOverhead", "2000").set("spark.executor.memory", "12g")
         
sc = SparkContext(conf = conf)

#sc = SparkContext()
sqlContext = HiveContext(sc)

df_par= sqlContext.read.option("mergeSchema", "true").parquet('s3://move-dataeng-omniture-prod/homerealtor/processed-data-xact/hit_data/year=2017/month=11/')

df_par.printSchema()

block_size = str(1024 * 1024 * 512)
sc._jsc.hadoopConfiguration().set("dfs.block.size", block_size)
sc._jsc.hadoopConfiguration().set("parquet.block.size", block_size)

s3_location_target = 's3://move-dataeng-temp-dev/glue-etl/parquet_block_poc/omtr_pq_block_512'
  
output_folder = s3_location_target # With absolute path
print 'output_folder= %s' %(output_folder)
#----  PySpark section ----
from pyspark.sql.functions import lit
from pyspark.sql.functions import reverse, split
#---
df_with_hour = df_par.withColumn("hour", split(reverse(split(reverse(df_par.etl_source_filename), '/')[1] ),'=')[1].cast("string")) 

df_with_day = df_with_hour.withColumn("day", split(reverse(split(reverse(df_with_hour.etl_source_filename), '/')[2] ),'=')[1].cast("string")) 

df_with_month = df_with_day.withColumn("month", split(reverse(split(reverse(df_with_day.etl_source_filename), '/')[3] ),'=')[1].cast("string")) 

df_with_partitions = df_with_month.withColumn("year", split(reverse(split(reverse(df_with_month.etl_source_filename), '/')[4] ),'=')[1].cast("string")) 


#----
codec='snappy'  
partitionby=['year', 'month','day', 'hour']
df = df_with_partitions.filter((df_with_partitions.day.cast('Integer') < 2 ) & ( df_with_partitions.day.cast('Integer') > 0))
#df.repartition(*partitionby).write.partitionBy("hour").mode('overwrite').parquet(output_folder, compression=codec) 
df.repartition(*partitionby).write.partitionBy(['year', 'month','day', 'hour']).mode('overwrite').parquet(output_folder, compression=codec) 
#df_with_partitions.repartition(*partitionby).write.partitionBy(['year', 'month','day', 'hour']).mode('overwrite').parquet(output_folder, compression=codec) 

#df_par.write.mode('overwrite').partitionBy("hour").parquet(output_folder) 
#df_par.write.mode('overwrite').parquet(output_folder)
print sc._jsc.hadoopConfiguration().get("dfs.block.size")
print 'Done Parquet Conversion !'
