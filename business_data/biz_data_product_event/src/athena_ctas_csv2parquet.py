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

glueContext = GlueContext(SparkContext.getOrCreate())
  
spark = glueContext.spark_session


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
## Main
#------------ Input section -----
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_location_csv_file', 
                           's3_location_parquet_file',
                           's3_location_custom_schema_json_file',
                            'table_name'] )

s3_location_source = args['s3_location_csv_file']
s3_location_target = args['s3_location_parquet_file']
s3_location_custom_schema = args['s3_location_custom_schema_json_file']
target_table_name = args['table_name']

#------------

print 'Starting Parquet Conversion ...'
input_file = '%s/*.csv' %(s3_location_source)
output_folder = s3_location_target # With absolute path
print 'input_file= %s' %(input_file)
print 'output_folder= %s' %(output_folder)
print 's3_location_custom_schema= %s' %(s3_location_custom_schema)
 
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
customTypedDf = spark.sql(sql_str)
customTypedDf.write.mode('overwrite').parquet(output_folder)
print 'Done Parquet Conversion !'
customTypedDf.printSchema()

for col_name, col_dtype in sorted(customTypedDf.dtypes):
       col_str = '%s %s,' %(col_name.strip() , col_dtype.strip() )
       print col_str

