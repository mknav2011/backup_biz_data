import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import input_file_name
import uuid
import json
import time
import datetime

from dateutil.parser import parse
import os 
 


import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())

sc = SparkContext()  
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME' ] )

df_par= spark.read.option("mergeSchema", "true").parquet('s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim')
#df_par.printSchema()
#df_par.count()

df_par.createOrReplaceTempView("listing_dim"); 
block_size = str(1024 * 1024 * 512)
sc._jsc.hadoopConfiguration().set("dfs.block.size", block_size)
sc._jsc.hadoopConfiguration().set("parquet.block.size", block_size)

df_joined = spark.sql('''WITH edw_listings AS
(
select
row_number() OVER (PARTITION BY listingdimkey ORDER BY  effectiveto ASC) AS row_number_seq,
listingdimkey ,
listingid ,
buyeragentofficedatasourceid ,
buyeragentsourcealiasid ,
buyercoagentofficedatasourceid ,
buyercoagentsourcealiasid ,
buyercoofficesourcealiasid ,
buyerofficesourcealiasid ,
createdby ,
createddate ,
dapendingdate ,
dapendingflag ,
effectivefrom ,
effectiveto ,
etl_create_date ,
etl_source_filename ,
etl_ztg_id ,
fulfillmentgroupbitmask ,
hascobroke ,
hascobrokephone ,
headline2 ,
headline ,
isaolsyndication ,
ischoiceenabled ,
isdisplayed ,
isforeclosure ,
isopenhouse ,
ispricedropped ,
ispriceincreased ,
isrdcsyndication ,
isrental ,
isvideo ,
isvirtualtour ,
listingaddress ,
listingagentofficedatasourceid ,
listingagentsourcealiasid ,
listingcity ,
listingcoagentofficedatasourceid ,
listingcoagentsourcealiasid ,
listingcoofficesourcealiasid ,
listingcountry ,
listingcounty ,
listingcurrentprice ,
listingdatasourceid ,
listingenddate ,
listinggeoapproximation ,
listinglotsquarefeet ,
listingmarketingtype ,
listingnumberofbathrooms ,
listingnumberofbedrooms ,
listingnumberofstories ,
listingofficesourcealiasid ,
listingoriginalprice ,
listingpercentagecompleteness ,
listingphotocount ,
listingphotourl ,
listingpostalcode ,
listingpriceincreaseddate ,
listingpricereduceddate ,
listingprovider ,
listingrawstatus ,
listingrentalprice ,
listingsolddate ,
listingsoldprice ,
listingsourcealiasid ,
listingsquarefeet ,
listingstartdate ,
listingstate ,
listingstatus ,
listingstatuschangeddate ,
listingstyle ,
listingtype ,
masterplanid ,
modifiedby ,
modifieddate ,
newconstructionflag ,
nonmlsflag ,
openhouseenddate ,
openhousestartdate ,
producttypedimkey ,
propertydescription ,
propertyid ,
specialmessage ,
specid ,
systemcreationdate ,
udblistingid
from
listing_dim  
)
SELECT
listingdimkey ,
listingid ,
buyeragentofficedatasourceid ,
buyeragentsourcealiasid ,
buyercoagentofficedatasourceid ,
buyercoagentsourcealiasid ,
buyercoofficesourcealiasid ,
buyerofficesourcealiasid ,
createdby ,
createddate ,
dapendingdate ,
dapendingflag ,
effectivefrom ,
effectiveto ,
etl_create_date ,
etl_source_filename ,
etl_ztg_id ,
fulfillmentgroupbitmask ,
hascobroke ,
hascobrokephone ,
headline2 ,
headline ,
isaolsyndication ,
ischoiceenabled ,
isdisplayed ,
isforeclosure ,
isopenhouse ,
ispricedropped ,
ispriceincreased ,
isrdcsyndication ,
isrental ,
isvideo ,
isvirtualtour ,
listingaddress ,
listingagentofficedatasourceid ,
listingagentsourcealiasid ,
listingcity ,
listingcoagentofficedatasourceid ,
listingcoagentsourcealiasid ,
listingcoofficesourcealiasid ,
listingcountry ,
listingcounty ,
listingcurrentprice ,
listingdatasourceid ,
listingenddate ,
listinggeoapproximation ,
listinglotsquarefeet ,
listingmarketingtype ,
listingnumberofbathrooms ,
listingnumberofbedrooms ,
listingnumberofstories ,
listingofficesourcealiasid ,
listingoriginalprice ,
listingpercentagecompleteness ,
listingphotocount ,
listingphotourl ,
listingpostalcode ,
listingpriceincreaseddate ,
listingpricereduceddate ,
listingprovider ,
listingrawstatus ,
listingrentalprice ,
listingsolddate ,
listingsoldprice ,
listingsourcealiasid ,
listingsquarefeet ,
listingstartdate ,
listingstate ,
listingstatus ,
listingstatuschangeddate ,
listingstyle ,
listingtype ,
masterplanid ,
modifiedby ,
modifieddate ,
newconstructionflag ,
nonmlsflag ,
openhouseenddate ,
openhousestartdate ,
producttypedimkey ,
propertydescription ,
propertyid ,
specialmessage ,
specid ,
systemcreationdate ,
udblistingid,
to_date(effectiveto) AS effectiveto_date
FROM edw_listings WHERE row_number_seq = 1
            '''
            )
#df_joined.cache()
#df_joined.describe()
df_joined.printSchema()
#print  df_joined.count()          
s3_location_target = 's3://move-dataeng-temp-dev/glue-etl/parquet_data/listingdim_pdt_deduped_pq_512'
 
output_folder = s3_location_target # With absolute path 
print 'output_folder= %s' %(output_folder)
#----  PySpark section ----

#df_joined.write.mode('overwrite').parquet(output_folder) 
df_joined.write.mode('overwrite').save(output_folder)
#new_dynamic_frame = DynamicFrame.fromDF(df_joined, glueContext, "new_dynamic_frame")
#codec = 'snappy'
#glueContext.write_dynamic_frame.from_options(frame = m_df, connection_type = "s3", connection_options = {"path":  child_output_dir}, format = "parquet", compression=codec)
#glueContext.write_dynamic_frame.from_options(frame = new_dynamic_frame, connection_type = "s3", connection_options = {"path":  output_folder}, format = "parquet", format_options = {compression: "snappy" })
#df_joined.write.mode('overwrite').parquet(output_folder)
print 'Done Parquet Conversion !'
