
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
     .appName("Python Spark SQL for Customer hierarchy load") \
     .config("spark.some.config.option", "Customer_hierarchy-Load") \
     .getOrCreate()

output_folder = 's3://move-dataeng-cust-dev/cdh/business-data/customer_hierarchy/'

df_par_orig=spark.read.parquet("s3://move-dataeng-cust-prod/cdh/processed-data-xact/cdhdb_move_orig_sys_references_v/*")
df_par_rel=spark.read.parquet("s3://move-dataeng-cust-prod/cdh/processed-data-xact/apps_move_hz_relationships/*")
df_par_acc=spark.read.parquet("s3://move-dataeng-cust-prod/cdh/processed-data-xact/mktgmdm_move_lf_cdh_account_v/*")

df_par_orig.createOrReplaceTempView("cdhdb_move_orig_sys_references_v");
df_par_rel.createOrReplaceTempView("apps_move_hz_relationships");
df_par_acc.createOrReplaceTempView("mktgmdm_move_lf_cdh_account_v");

df_hierarchy = spark.sql('''
WITH
pdt_cdh_cdhdb_move_orig_sys_references_v AS
(SELECT *
FROM cdhdb_move_orig_sys_references_v
),
pdt_cdh_apps_move_hz_relationships AS
(SELECT *
FROM apps_move_hz_relationships
),
pdt_cdh_mktgmdm_move_lf_cdh_account_v AS
(SELECT *
FROM mktgmdm_move_lf_cdh_account_v
),
orig_system_ref_customerhierarchy_temp as
(
select
distinct
orig_system,
orig_system_reference,
owner_table_name,
owner_table_id,
status,
attribute15,
attribute20,
last_update_date_mst
from
(
select
orig_system,
orig_system_reference,
owner_table_name,
owner_table_id,
status,
attribute15,
attribute20,
last_update_date_mst,
row_number() over(partition by orig_system_reference order by last_update_date_mst desc, etl_create_date desc) as row_num
from pdt_cdh_cdhdb_move_orig_sys_references_v
WHERE orig_system IN ('MLS')
 -- and status = 'A'
) X
where row_num = 1 and status = 'A'),
hz_relationships_customerhierarchy_temp as
(
SELECT
DISTINCT
object_id,
subject_id,
object_type,
subject_type,
status,
relationship_code,
last_update_date_mst
from
(select
object_id,
subject_id,
object_type,
subject_type,
status,
relationship_code,
last_update_date_mst,
row_number() over(partition by subject_id order by last_update_date_mst desc, etl_create_date desc) as row_num
from
pdt_cdh_apps_move_hz_relationships
WHERE
relationship_code IN ('CHILD_OF')
AND subject_type ='ORGANIZATION'
AND object_type ='ORGANIZATION'
-- AND status = 'A' 
) Y
where row_num = 1 and status = 'A'),
account_customerhierarchy_temp as
(
SELECT
DISTINCT
party_id,
type AS broker_type,
last_update_date_mst
from
(select
party_id,
type,
last_update_date_mst,
row_number() over(partition by party_id order by last_update_date_mst desc, etl_create_date desc) as row_num
from
pdt_cdh_mktgmdm_move_lf_cdh_account_v) Z
where row_num = 1),
adv_customerhierarchy_temp as
(
select
distinct
orig_system,
orig_system_reference,
owner_table_name,
owner_table_id,
status,
last_update_date_mst
from
(
select
orig_system,
orig_system_reference,
owner_table_name,
owner_table_id,
status,
last_update_date_mst,
row_number() over(partition by orig_system_reference order by last_update_date_mst desc, etl_create_date desc) as row_num
from pdt_cdh_cdhdb_move_orig_sys_references_v
WHERE orig_system IN ('XADV')
-- and status = 'A'
) ZZ
where row_num = 1 and status = 'A'
)
SELECT
distinct
pre_final.agent_party_id,
pre_final.agent_mls_set_id,
pre_final.agent_primary_mls_flg,
adva.orig_system_reference as agent_advertiser_id,
pre_final.office_party_id,
pre_final.office_mls_set_id,
pre_final.office_primary_mls_flg,
advo.orig_system_reference as office_advertiser_id,
-- pre_final.primary_broker_flag as primary_broker_flag,
pre_final.broker_party_id ,
pre_final.broker_type,
advb.orig_system_reference as broker_advertiser_id,
greatest(
coalesce(a1_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
coalesce(o1_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
coalesce(o2_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
coalesce(b1_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
coalesce(b2_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
coalesce(adva.last_update_date_mst,cast('1900-01-01 00:00:00.000' as timestamp)),
coalesce(advo.last_update_date_mst,cast('1900-01-01 00:00:00.000' as timestamp)),
coalesce(advb.last_update_date_mst,cast('1900-01-01 00:00:00.000' as timestamp))
) as last_update_date
from
(SELECT
distinct
AO.agent_party_id,
AO.agent_mls_set_id,
AO.agent_primary_mls_flg,
AO.office_party_id,
AO.office_mls_set_id,
AO.office_primary_mls_flg,
OB.primary_broker_flag,
OB.broker_party_id,
OB.broker_type,
a1_updt,
o1_updt,
o2_updt,
b1_updt,
b2_updt
FROM
(SELECT a.owner_table_id AS agent_party_id,
trim(a.orig_system_reference) AS agent_mls_set_id,
trim(o.orig_system_reference) AS office_mls_set_id,
o.owner_table_id AS office_party_id,
a.a1_updt,
o.o1_updt,
CASE
WHEN trim(a.attribute15) <> '' THEN
a.attribute15
WHEN a.agent_is_multiple = 1 THEN '1'
ELSE '0'
END agent_primary_mls_flg,
CASE
WHEN trim(o.attribute15) <> '' THEN
o.attribute15
WHEN o.office_is_multiple = 1 THEN '1'
ELSE '0'
END office_primary_mls_flg
FROM
(SELECT owner_table_id,
orig_system_reference,
attribute20,
attribute15,
last_update_date_mst as a1_updt,
count(1)
OVER (partition by owner_table_id) agent_is_multiple
FROM orig_system_ref_customerhierarchy_temp
WHERE orig_system = 'MLS'
AND status = 'A'
AND owner_table_name = 'HZ_PARTIES'
AND orig_system_reference LIKE 'A%' ) a
RIGHT JOIN
(SELECT owner_table_id,
attribute15,
orig_system_reference,
last_update_date_mst as o1_updt,
count(1)
OVER (partition by owner_table_id) office_is_multiple
FROM orig_system_ref_customerhierarchy_temp
WHERE orig_system = 'MLS'
AND status = 'A'
AND orig_system_reference LIKE 'O%') o
ON a.attribute20 = o.orig_system_reference) AO
JOIN
(SELECT office_party_id,
broker_party_id,
primary_broker_flag,
broker_type,
o2_updt,
b1_updt,
b2_updt
FROM
(SELECT
DISTINCT
a.owner_table_id AS office_party_id,
b.object_id AS broker_party_id,
a.last_update_date_mst as o2_updt,
b.last_update_date_mst as b1_updt,
CASE
WHEN b.relationship_code = 'CHILD_OF' THEN 1
END AS primary_broker_flag
FROM orig_system_ref_customerhierarchy_temp a
LEFT JOIN hz_relationships_customerhierarchy_temp b
ON b.subject_id = a.owner_table_id
AND b.status = 'A'
AND b.relationship_code IN ('CHILD_OF')
AND subject_type ='ORGANIZATION'
AND object_type ='ORGANIZATION'
WHERE a.orig_system = 'MLS'
AND a.status = 'A'
AND a.orig_system_reference LIKE 'O%'
AND a.owner_table_name='HZ_PARTIES') A
LEFT JOIN
(SELECT DISTINCT party_id,
broker_type,
last_update_date_mst as b2_updt
FROM account_customerhierarchy_temp) B
ON A.broker_party_id = B.party_id) OB
ON AO.office_party_id = OB.office_party_id
where AO.agent_party_id is not null or OB.broker_party_id is not null
) pre_final
left join adv_customerhierarchy_temp adva on adva.owner_table_id = pre_final.agent_party_id
left join adv_customerhierarchy_temp advo on advo.owner_table_id = pre_final.office_party_id
left join adv_customerhierarchy_temp advb on advb.owner_table_id = pre_final.broker_party_id
''')

df_hierarchy.write.mode('overwrite').parquet(output_folder)

