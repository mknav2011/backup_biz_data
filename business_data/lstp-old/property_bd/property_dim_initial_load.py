#!/usr/bin/python
import os, sys, json, uuid
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, *
from pyspark.sql.functions import udf
from pyspark.sql.functions import input_file_name

from pyspark.sql import SparkSession

import time, datetime
from dateutil.parser import parse
debug = 1

#1. Variables
s3_location_target = 's3://move-dataeng-lstp-dev/edw/post-pdt/listingdim/listingdim_deduped_poc2'


"""
ToDo:
Reviews
[Murali]:
1. Create a EffectiveTo = 9999-12-31 to a temp location and then swp the Partitions for Athena Table
2. Store the MaxEffectiveTo in DynamoDB  or as a text file in S3 (Prefer: s3)
3. Time Taken for 2 months of data '2017-06', '2017-07'
4. Capture the Start and End Timings for each step

N. Remove Data De-Dupes for 9999-12-31 records
[Team]:
1.
"""


spark = SparkSession \
     .builder \
     .appName("Python Spark SQL basic example") \
     .config("spark.some.config.option", "ListingDim-Initial-Load") \
     .getOrCreate()

# Read Latest ListingDim Data & Incrementals (Prep. the folder paths from LGK or some way)
df_par=spark.read.parquet(
"s3://move-dataeng-lstp-dev/edw/post-pdt/listingdim/listingdim_deduped_poc2/effectiveto_date=9999-12-31/*",

"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year=2017/month=07/day=15/*",
"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year=2017/month=06/day=21/*",
"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year=2017/month=07/day=19/*",
"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year=2017/month=06/day=03/*",
"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim/year=2017/month=06/day=25/*"
)

#Create a Table
df_par.createOrReplaceTempView("listing_dim");

# *** From Last load, get the Max EffectiveTo other than 9999-12-31 ****

# De-Dupe them and pick the latest (Partition By ListingDimKey, EffectiveTo in Ascending order)
df_EffectiveTo_not_99991231 = spark.sql('''WITH edw_listings AS
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
listing_dim WHERE to_date(effectiveto) = "9999-12-31"
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
AND to_date(effectiveto) > $LASTEFFECTIVETO - 1
            '''
)

df_EffectiveTo_not_99991231.partitionBy("effectiveto_date").write.mode('append').parquet(output_folder)

# De-Dupe them and pick the latest (Partition By ListingDimKey, EffectiveTo in Descending order)
df_EffectiveTo_99991231 = spark.sql('''WITH edw_listings AS
(
select
row_number() OVER (PARTITION BY listingid ORDER BY listingdimkey DESC, effectiveto DESC) AS row_number_seq,
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
AND to_date(effectiveto) = "9999-12-31"
            '''
            )

df_EffectiveTo_99991231.write.mode('overwrite').partitionBy("effectiveto_date").parquet(output_folder)

"""
Create a EffectiveTo = 9999-12-31 to a temp location and then swp the Partitions for Athena Table

"""


# Refresh Athena Table Schema
