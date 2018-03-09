
#from __future__ import print_function
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
debug = 1
 


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
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME' ] )

df_par= spark.read.option("mergeSchema", "true").parquet('s3://move-dataeng-lstp-prod/edw/processed-data/MoveDM_EDW_tblListingDim/')
df_par.printSchema()
df_par.count()

df_par.createOrReplaceTempView("listing_dim"); 
df_joined = spark.sql('''WITH edw_listings AS
(
select
row_number() OVER (PARTITION BY listingdimkey ORDER BY  effectiveto DESC) AS row_number_seq,
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
etl_row_md5_ckecksum ,
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
where year='2017'
 -- and month = '11'
  -- and day = '11'
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
etl_row_md5_ckecksum ,
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
FROM edw_listings WHERE row_number_seq = 1 
            '''
            )
df_joined.cache()
df_joined.describe()
df_joined.printSchema()
print  df_joined.count()          
s3_location_target = 's3://move-dataeng-temp-dev/glue-etl/parquet_data/listingdim_deduped_ap'
 
output_folder = s3_location_target # With absolute path
print 'input_file= %s' %(input_file)
print 'output_folder= %s' %(output_folder)
#----  PySpark section ----

 
df_joined.write.mode('overwrite').parquet(output_folder)
print 'Done Parquet Conversion !'
df.printSchema()
