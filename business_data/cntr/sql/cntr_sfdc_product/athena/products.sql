CREATE EXTERNAL TABLE `biz_data.products`(
auto_renewal string,
big_machines_part_number string,
charge_type string,
commission_category string,
created_date timestamp,
default_renewal_term string,
default_term string,
description string,
division string,
family string,
fulfillment_id string,
fulfillment_required string,
fulfillment_type string,
inventory string,
inventory_description string,
is_active string,
is_cfbc string,
is_deleted string,
last_modified_date timestamp,
last_referenced_date timestamp,
normalization_factor string,
pricing_method string,
product_code string,
product_id string,
product_name string,
renewal_term_type string,
s_no string,
subscription_type string,
systemmodstamp string,
tax_category string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://move-dataeng-cntr-dev/contracts/businessdata/sfdc_products'
TBLPROPERTIES (
  'PARQUET.COMPRESS'='SNAPPY',
  'transient_lastDdlTime'='1519870171')