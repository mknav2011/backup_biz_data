
#!/usr/bin/python
import os, sys, json, uuid
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import input_file_name

from pyspark.sql import SparkSession

import time, datetime
from dateutil.parser import parse
debug = 1


spark = SparkSession \
     .builder \
     .appName("Python Spark SQL for Customer party Load") \
     .config("spark.some.config.option", "Customer_party-Load") \
     .getOrCreate()

output_folder = 's3://move-dataeng-cust-dev/cdh/business-data/customer_party/'

df_par=spark.read.parquet("s3://move-dataeng-cust-prod/cdh/processed-data-xact/mktgmdm_move_lf_cdh_account_v/*")

df_par.createOrReplaceTempView("mktgmdm_move_lf_cdh_account_v");

df_party = spark.sql('''
select
lower(account_sub_type) as account_sub_type,
-- regexp_extract(adv_con_pt_id,'\.([^.]+)', 1)  as adv_con_pt_id,
advertiser_email,
regexp_extract(advertiser_id,'\.([^.]+)', 1)  as advertiser_id,
-- regexp_extract(bill_to,'\.([^.]+)', 1)  as bill_to_id,
bill_to_address,
bill_to_city,
bill_to_country,
bill_to_postalcode,
bill_to_state,
brokerage_id,
broker_nar_id,
display_name,
lower(division) as division,
franchise_type,
-- regexp_extract(home_con_pt_id,'\.([^.]+)', 1)  as home_con_pt_id,
home_phone_number,
homepage_url,
last_update_date as mst_last_update_date,
-- regexp_extract(main_con_pt_id,'\.([^.]+)', 1)  as main_con_pt_id,
main_email,
-- regexp_extract(mls_con_pt_id,'\.([^.]+)', 1)  as mls_con_pt_id,
mls_email,
mls_set_id,
nar_id,
-- regexp_extract(other_con_pt_id,'\.([^.]+)', 1)  as other_con_pt_id,
other_email,
party_id,
party_name,
preferred_phone,
primary_cust_id,
-- record_type,
-- regexp_extract(secondary_con_pt_id,'\.([^.]+)', 1)  as secondary_con_pt_id,
secondary_email,
-- regexp_extract(ship_to,'\.([^.]+)', 1)  as ship_to_id,
ship_to_address,
ship_to_postalcode,
ship_to_city,
ship_to_state,
ship_to_country,
lower(status) as status,
lower(type) as type,
work_fax,
-- regexp_extract(work_fax_con_pt_id,'\.(.*)','')  as work_fax_con_pt_id,
-- regexp_extract(work_mobile_con_pt_id,'\.(.*)','')  as work_mobile_con_pt_id,
work_mobile_number,
-- regexp_extract(work_phone_con_pt_id,'\.(.*)','')  as work_phone_con_pt_id,
work_phone_number
-- current_date as etl_snapshot_date
from
(
select
account_sub_type,
adv_con_pt_id,
advertiser_email,
advertiser_id,
bill_to,
bill_to_address,
bill_to_city,
bill_to_country,
bill_to_postalcode,
bill_to_state,
brokerageid as brokerage_id,
brokernarid as broker_nar_id,
display_name,
division,
franchisetype as franchise_type,
home_con_pt_id,
home_phone_number,
homepage_url,
last_update_date_mst as last_update_date,
main_con_pt_id,
main_email,
mls_con_pt_id,
mls_email,
mls_set_id,
narid as nar_id,
other_con_pt_id,
other_email,
party_id,
party_name,
preferred_phone,
primary_cust_id,
record_type,
secondary_con_pt_id,
secondary_email,
ship_to,
ship_to_address,
ship_to_postalcode,
ship_to_city,
ship_to_state,
ship_to_country,
status,
type,
work_fax,
work_fax_con_pt_id,
work_mobile_con_pt_id,
work_mobile_number,
work_phone_con_pt_id,
work_phone_number,
row_number() over(partition by party_id order by last_update_date_mst desc, etl_create_date desc) as row_num
from mktgmdm_move_lf_cdh_account_v
-- where last_update_date_mst < current_timestamp
) A
where row_num = 1
''')

df_party.write.mode('overwrite').parquet(output_folder)

