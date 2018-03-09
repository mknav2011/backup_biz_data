
#!/usr/bin/python
import os, sys, json, uuid
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import input_file_name

from pyspark.sql import SparkSession

import time, datetime
from dateutil.parser import parse
debug = 1


spark = SparkSession \
     .builder \
     .appName("Python Spark SQL for Customer mlsset Load") \
     .config("spark.some.config.option", "Customer_mlsset-Load") \
     .getOrCreate()

output_folder = 's3://move-dataeng-cust-dev/cdh/business-data/customer_mlsset/'

print output_folder

df_par=spark.read.parquet("s3://move-dataeng-cust-prod/cdh/processed-data-xact/mktgmdm_move_lf_cdh_mlsset_v/*")

print 'source df created'

df_par.createOrReplaceTempView("mktgmdm_move_lf_cdh_mlsset_v");

print 'table created'

df_party = spark.sql('''
select
address,
broker_nar_id,
city,
email_address,
mst_end_active_date,
mls_account_name,
mst_mls_last_update_date,
mls_set_id,
mlsa_party_id,
mlsa_source_ref,
mls_set_type,
nar_id,
office_mls_set_id,
office_party_id,
party_id,
phone_number,
postal_code,
primary_mls,
mst_start_active_date,
state,
status
from
(
select
address,
brokernarid as broker_nar_id,
city,
email_address,
end_date_active_mst as mst_end_active_date,
mls_account_name,
mls_last_update_mst as mst_mls_last_update_date,
mls_set_id,
mlsa_party_id,
mlsa_source_ref,
lower(mlsset_type) as mls_set_type,
narid as nar_id,
office_mls_set_id,
regexp_extract(office_party_id,'\.([^.]+)', 1)  as office_party_id,
-- orig_system_ref_id,
party_id,
phone_number,
postal_code,
primary_mls,
-- record_type,
start_date_mst as mst_start_active_date,
state,
lower(status) as status,
row_number() over(partition by mls_set_id order by mls_last_update_mst desc, etl_create_date desc) as row_num
from mktgmdm_move_lf_cdh_mlsset_v
) A
where row_num = 1
''')

print 'query executed'

df_party.write.mode('overwrite').parquet(output_folder)

print 'overrite complete'
