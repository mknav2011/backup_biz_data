#------------------------------
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
def column_type_lookup(column_name, schema_list):
       '''
       Return Spark datatype based on column_name suffix
       Reference: custom schema json file supplied
       '''
       column_suffix = column_name.split('_')[-1]
       for el in schema_list:
           if el[0] == column_suffix:
              return el[1]
       return 'string'

#sc = SparkContext.getOrCreate()

#---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

#----

# Setting Bigger Parquet blocks: 512 MB
block_size = str(1024 * 1024 * 512)
sc._jsc.hadoopConfiguration().set("dfs.block.size", block_size)
sc._jsc.hadoopConfiguration().set("parquet.block.size", block_size)

#glueContext = GlueContext(SparkContext.getOrCreate())
#glueContext = GlueContext(sc)
  
#spark = glueContext.spark_session

## Main
#------------ Input section -----
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_location_csv_file', 
                           's3_location_parquet_file',
                           's3_location_custom_schema_json_file',
                           'num_partitions',
                            'table_name'] )

s3_location_source = args['s3_location_csv_file']
s3_location_target = args['s3_location_parquet_file']
s3_location_custom_schema = args['s3_location_custom_schema_json_file']
target_table_name = args['table_name']
num_partitions = int(args['num_partitions'])

#------------

print 'Starting Parquet Conversion ...'
input_file = '%s/*.csv' %(s3_location_source)
output_folder = s3_location_target # With absolute path
#-- Inputs 
print '---------------Inputs Section -------------------'
print 'input_file= %s' %(input_file)
print 'output_folder= %s' %(output_folder)
print 's3_location_custom_schema= %s' %(s3_location_custom_schema)
print 'num_partitions= %d' %(num_partitions)
print 'target_table_name= %s' %(target_table_name)
print '-----------------------------------------------------'
 
json_rdd = spark.read.json(s3_location_custom_schema).rdd
new_list = [ (row['column_suffix'], row['spark_data_type']) for row in json_rdd.toLocalIterator()]


csv_df = spark.read.format('csv').options(header='true', inferSchema='false', parserLib='univocity',multiLine='true',wholeFile='true').load(input_file)
#TODO
# Filter out if count is 0
csv_df.registerTempTable("csv_table")
select_list = []
for col_name, col_dtype in sorted(csv_df.dtypes):
          dtype =  column_type_lookup(col_name, new_list)
          projection_str = ' cast(%s as %s) as %s' %(col_name, dtype, col_name)
          select_list.append(projection_str)
sql_str = 'select %s from csv_table' %(','.join(select_list))

print '---------------Query String Start -------------------'
print 'sql_str =', sql_str
print '---------------Query String End -------------------'

customTypedDf = spark.sql(sql_str)
#num_partitions = 20
customTypedDf.repartition(num_partitions).write.mode('overwrite').parquet(output_folder)
print 'Done Parquet Conversion !'
customTypedDf.printSchema()

for col_name, col_dtype in sorted(customTypedDf.dtypes):
       col_str = '%s %s,' %(col_name.strip() , col_dtype.strip() )
       print col_str

