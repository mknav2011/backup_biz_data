import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
data_source = glueContext.getSource("file", paths=["s3://move-dataeng-leads-prod/raw-data/2017/11/20/01"])
data_source.setFormat("json")
lcs_df = data_source.getFrame()
#lcs = glueContext.create_dynamic_frame.from_options(connection_type ="s3", connection_options="", format="JSON")
print "Count: ", lcs_df.count()
lcs_df.printSchema() 

myFrame = glueContext.create_dynamic_frame_from_options(connection_type="file",
                                     connection_options={"paths": ["s3://move-dataeng-leads-prod/raw-data/2017/11/20/01/move-dataeng-leads-firehose-1-2017-11-20-01-03-02-bbccb4aa-2a10-41ab-b5db-0cb7c73e9358.gz"]},
                                      format="json")

myFrame.count()
myFrame.printSchema()

from awsglue.dynamicframe import DynamicFrame
redshift_temp_dir = 's3://move-dataeng-temp-dev/glue-test-ap/glue-sample-target/lcs-temp-dir/'
# Turn it back to a dynamic frame
input = 's3://move-dataeng-leads-prod/raw-data/2017/11/20/01'
file_df =  glueContext.read.json(input)
lcs_tmp = DynamicFrame.fromDF(file_df, glueContext, "nested")
# Convert the data to flat tables
print "Converting to flat tables ..."
dfc = lcs_tmp.relationalize("lcs_root", redshift_temp_dir)                                      
 
output_history_dir = 's3://move-dataeng-temp-dev/glue-test-ap/glue-sample-target/lcs-base-dir/'
#glueContext.write_dynamic_frame.from_options(frame = l_history, connection_type = "s3", connection_options = {"path": output_history_dir}, format = "parquet")
for df_name in dfc.keys():
    m_df = dfc.select(df_name)
    print "Writing to Redshift table: ", df_name, " ..."
    child_output_dir = output_history_dir + df_name.replace('.', '_')
    print 'child_output_dir: ', child_output_dir
    glueContext.write_dynamic_frame.from_options(frame = m_df, connection_type = "s3", connection_options = {"path":  child_output_dir}, format = "parquet") 
job.commit()
