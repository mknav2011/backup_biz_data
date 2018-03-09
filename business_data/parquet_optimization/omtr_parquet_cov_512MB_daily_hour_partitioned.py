#----
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import SparkSession    
import json

#----
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
## @params: [JOB_NAME]
#------------ Input section -----
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_location_source', 
                           's3_location_target',
                           'start_hour',
                           'end_hour'] )

s3_location_source = args['s3_location_source']
s3_location_target = args['s3_location_target']
#files_per_partition = int(args['files_per_partition'])
start_hour = int(args['start_hour'])
end_hour = int(args['end_hour'])

#------------
 
sc = SparkContext()
sqlContext = HiveContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
#--- Reading Input 
#df_par= sqlContext.read.option("mergeSchema", "true").parquet('s3://move-dataeng-omniture-prod/homerealtor/processed-data-xact/hit_data/year=2017/month=11/day=11/')
df_par= sqlContext.read.option("mergeSchema", "true").parquet(s3_location_source)

df_par.printSchema()

#--- set block size
block_size = str(1024 * 1024 * 512)
sc._jsc.hadoopConfiguration().set("dfs.block.size", block_size)
sc._jsc.hadoopConfiguration().set("parquet.block.size", block_size)
#---
#limit records to be processed, due to Glue memory limitation
df = df_par.filter((df_par.hour >= start_hour  ) & (df_par.hour < end_hour ))
print 'Total records read from Source S3 location', df.count()
#---- Prepare SQLs
select_list = []
ddl_list = []
for col_name, col_dtype in sorted(df.dtypes):
          #dtype =  column_type_lookup(col_name, new_list)
          #projection_str = ' cast(%s as %s) as %s' %(col_name, col_dtype, col_name)
          if col_name not in ('year', 'month','day', 'hour'):
             ddl_str = '%s %s'  %(col_name, col_dtype)
             projection_str = ' cast(%s as %s) as %s' %(col_name, col_dtype, col_name)
             ddl_list.append(ddl_str)
             select_list.append(projection_str)

#           location 's3://move-dataeng-temp-dev/glue-etl/parquet_block_poc/hit_data_pdt_512mb_ctas/'
ddl_str = """ create external table output_table_omtr_part( %s )  
           partitioned by (year string, month string, day string, hour string)  
           stored as parquet 
           location '%s'
           """ %(','.join(ddl_list), s3_location_target)

#---
print 'ddl_str = ', ddl_str
print 'select_str = ', ','.join(select_list)
#-----
df.createOrReplaceTempView("hit_data"); 
df_joined = sqlContext.sql("""
    select %s,
     split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[0] as year
          ,split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[1] as month
          ,split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[2] as day
          ,split(split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[3], ':')[0] as hour
    from hit_data
    """ %(','.join(select_list))
    )


#----
df_joined.createOrReplaceTempView("hit_data_v") 
sqlContext.sql( ddl_str     )

sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

print df.count()   
sqlContext.sql( "show tables").show()

#---
sqlContext.sql( """
   select  accept_language , browser,
     split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[0]  as year
          ,split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[1]  as month
          ,split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[2]  as day
          ,split(split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[3], ':')[0] as hour 
    from hit_data_v
    limit 10
    """).show()
#----
ctas_sql_str = """
        insert overwrite table output_table_omtr_part
    partition (year, month, day, hour)
    select *
    from hit_data_v
    """ 
print  ' ctas_sql_str =', ctas_sql_str
sqlContext.sql( ctas_sql_str)
#---
print 'Parquet Conversion Done!'
