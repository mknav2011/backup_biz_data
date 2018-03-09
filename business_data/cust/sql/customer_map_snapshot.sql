with orig_system_ref_customermap_temp as
(
select
distinct
orig_system,
orig_system_reference,
owner_table_name,
owner_table_id,
status,
attribute15,
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
last_update_date_mst,
row_number() over(partition by orig_system_reference,orig_system,owner_table_id  order by last_update_date_mst desc, etl_create_date desc) as row_num
from cdhdb_move_orig_sys_references_v
WHERE orig_system IN ('MLS','SEBL','SFDC','XADV','RLTR')
and last_update_date_mst < cast('PULL_DATE_PARAM' as date)
) X
where row_num = 1),
map_dataset as (
SELECT
distinct
A.party_id as party_id,
B.mls_set_id as mls_set_id,
CASE
WHEN trim(B.attribute15) <> '' THEN
B.attribute15
WHEN B.is_multiple = 1 THEN '1'
ELSE '0'
END primary_mls_set_flag,
A.sfdcaccountid as enterprise_account_id,
A.advertiser_id as advertiser_id,
A.legacy_cust_id as legacy_cust_id,
greatest(coalesce(A.mls_set_id_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
         coalesce(A.sfdcaccountid_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
         coalesce(A.advertiser_id_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
         coalesce(B.ismultiple_updt,cast('1900-01-01 00:00:00.000' as timestamp)),
        coalesce(A.legacy_cust_id_updt,cast('1900-01-01 00:00:00.000' as timestamp))) as mst_last_update_date,
cast('SNAPSHOT_DATE_PARAM' as date) as data_snapshot_date,
CURRENT_TIMESTAMP as etl_create_date
FROM
(SELECT owner_table_id AS party_id,
max(case
WHEN orig_system = 'MLS' THEN
orig_system_reference end) AS mls_set_id,
max(case
WHEN orig_system = 'MLS' THEN
last_update_date_mst end) AS mls_set_id_updt,
max(case
WHEN orig_system IN ('SEBL','SFDC') THEN
orig_system_reference end) AS sfdcaccountid,
max(case
WHEN orig_system IN ('SEBL','SFDC') THEN
last_update_date_mst end) AS sfdcaccountid_updt,
max(case
WHEN orig_system = 'XADV' THEN
orig_system_reference end) AS advertiser_id,
max(case
WHEN orig_system = 'XADV' THEN
last_update_date_mst end) AS advertiser_id_updt,
max(case
WHEN orig_system = 'RLTR' THEN
orig_system_reference end) AS legacy_cust_id,
max(case
WHEN orig_system = 'RLTR' THEN
last_update_date_mst end) AS legacy_cust_id_updt
FROM orig_system_ref_customermap_temp
WHERE orig_system IN ('MLS','SEBL','SFDC','XADV','RLTR')
AND status = 'A'
GROUP BY owner_table_id) A
LEFT JOIN
(SELECT owner_table_id AS party_id,
orig_system_reference AS mls_set_id,
attribute15,
last_update_date_mst as ismultiple_updt,
count(1) OVER (partition by owner_table_id) as is_multiple
FROM orig_system_ref_customermap_temp
WHERE orig_system = 'MLS'
AND status = 'A'
AND owner_table_name = 'HZ_PARTIES') B
ON A.party_id = B.party_id
)
select
party_id,
mls_set_id,
primary_mls_set_flag,
enterprise_account_id,
advertiser_id,
legacy_cust_id,
max(mst_last_update_date) as mst_last_update_date,
data_snapshot_date,
etl_create_date
from
map_dataset
group by
party_id,
mls_set_id,
primary_mls_set_flag,
enterprise_account_id,
advertiser_id,
legacy_cust_id,
data_snapshot_date,
etl_create_date
