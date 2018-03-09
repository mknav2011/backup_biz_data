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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
sqlContext = HiveContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
#--- Reading Input 
df_par= sqlContext.read.option("mergeSchema", "true").parquet('s3://move-dataeng-omniture-prod/homerealtor/processed-data-xact/hit_data/year=2017/month=11/day=11/')

df_par.printSchema()

#--- set block size
block_size = str(1024 * 1024 * 512)
sc._jsc.hadoopConfiguration().set("dfs.block.size", block_size)
sc._jsc.hadoopConfiguration().set("parquet.block.size", block_size)
#---

df = df_par.filter((df_par.hour< 2 ))
print df.count()
#---- Prepare SQLs
select_list = []
ddl_list = []
for col_name, col_dtype in sorted(df.dtypes):
          #dtype =  column_type_lookup(col_name, new_list)
          #projection_str = ' cast(%s as %s) as %s' %(col_name, col_dtype, col_name)
          if col_name not in ('year', 'month','day', 'hour'):
             ddl_str = '%s %s'  %(col_name, col_dtype)
             projection_str = '  %s ' %(col_name)
             ddl_list.append(ddl_str)
             select_list.append(projection_str)
ddl_str = """create external table output_table_omtr( %s )   
           stored as parquet 
           location 's3://move-dataeng-temp-dev/glue-etl/parquet_block_poc/hit_data_pdt_512mb_ctas/'
           """ %(','.join(ddl_list))

#---
print 'ddl_str = ', ddl_str
print 'select_str = ', ','.join(select_list)
sqlContext.sql( ddl_str     )

df.createOrReplaceTempView("hit_data_big"); 
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

print df.count()   
sqlContext.sql( "show tables").show()

#----- Hive section ----
hadoopConf = {}
iterator = sc._jsc.hadoopConfiguration().iterator()
while iterator.hasNext():
    prop = iterator.next()
    hadoopConf[prop.getKey()] = prop.getValue()
for item in sorted(hadoopConf.items()): print(item)

for item in sorted(sc._conf.getAll()): print(item)

#---
sqlContext.sql( """
   select  accept_language , browser,
     split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[0]  
          ,split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[1]  
          ,split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[2]  
          ,split(split( from_unixtime(unix_timestamp(post_cust_hit_datetime_mst, 'yyyy-mm-dd hh24')) ,'-| ')[3], ':')[0]  
    from hit_data_big
    limit 10
    """).show()
#----
ctas_sql_str = """
    insert overwrite table output_table_omtr 
    select  %s 
    from hit_data_big
    """ %(','.join(select_list))
print  ' ctas_sql_str =', ctas_sql_str
sqlContext.sql( ctas_sql_str)
#---
job.commit()
