import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


from awsglue.utils import getResolvedOptions 

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_location_csv_file', 
                           's3_location_parquet_file',
                            'table_name'] )

s3_location_source = args['s3_location_csv_file']
s3_location_target = args['s3_location_parquet_file']
target_table_name = args['table_name']
print 'Starting Parquet Conversion ...'
input_file = '%s/*.csv' %(s3_location_source)
#output_s3_base = 's3://move-dataeng-omniture-dev/homerealtor/pdtp' 
#output_s3_base = s3_location_target
#date_partition = 'event_date=%s-%s-%s' %(y, m, d)
#output_partition =  '%s/%s' %(table_name,date_partition ) 

#output_folder = '%s/%s' %(output_s3_base, output_partition)
output_folder = s3_location_target # With absolute path
print 'input_file= %s' %(input_file)
print 'output_folder= %s' %(output_folder)
#----  PySpark section ----

sc = SparkContext()
sqlContext = SQLContext(sc)
#glueContext = GlueContext(sc)
#spark = glueContext.spark_session
#job = Job(glueContext)


csv_df = sqlContext.read.format('csv').options(header='true', inferSchema='true').load(input_file)
# rdd = sc.textFile(input_file).map(lambda line: line.split(","))
# df = sqlContext.createDataFrame(rdd, schema)

#output_folder = 's3://move-dataeng-omniture-dev/homerealtor/pdtp/rdc_vistor_aggregate_glue'
csv_df.write.mode('overwrite').parquet(output_folder)
print 'Done Parquet Conversion !'
csv_df.printSchema()
 
for col_name, col_dtype in sorted(csv_df.dtypes):
   if col_name.strip() not in ('year', 'month', 'day', 'hour'):
       col_Str = '%s %s,' %(col_name.strip() , col_dtype.strip() ) 
       print col_Str.replace('end:', '`end`:')
