import sys
from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
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
#---
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
sc = SparkContext()
spark = HiveContext(sc)
glueContext = GlueContext(sc)
#spark = HiveContext(sc)
#spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
##---
df_par= spark.read.option("mergeSchema", "true").parquet('s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/')
df_par.rdd.getNumPartitions()
 
df_par.createOrReplaceTempView("listing_dim")  
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

#----- Hive section ----
hadoopConf = {}
iterator = sc._jsc.hadoopConfiguration().iterator()
while iterator.hasNext():
    prop = iterator.next()
    hadoopConf[prop.getKey()] = prop.getValue()
for item in sorted(hadoopConf.items()): print(item)

for item in sorted(sc._conf.getAll()): print(item)

#--
select_list = []
for col_name, col_dtype in sorted(df_joined.dtypes):
          #dtype =  column_type_lookup(col_name, new_list)
          #projection_str = ' cast(%s as %s) as %s' %(col_name, col_dtype, col_name)
          ddl_str = '%s %s'  %(col_name, col_dtype)
          select_list.append(ddl_str)
ddl_str = """create external table output_table( %s )    
            stored as parquet location 's3://move-dataeng-temp-dev/glue-etl/parquet_data/listingdim_ctas/' 
            """ %(','.join(select_list))

print 'ddl_str = ', ddl_str
# Create the output Hive table
spark.sql( ddl_str     )
df_joined.rdd.getNumPartitions()
df_joined.createOrReplaceTempView("listing_dim_deduped")  
print df_joined.count()   
spark.sql( "show tables").show()

spark.sql(
    """
    insert overwrite table output_table
    select *  from listing_dim_deduped
    """
    ) 
    
##---
job.commit()
