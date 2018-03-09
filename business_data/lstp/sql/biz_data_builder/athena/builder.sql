CREATE EXTERNAL TABLE `builders`(
  `am_builder_record_status` string,
  `bdx_builder_id` string,
  `brand_logo` string,
  `brand_name` string,
  `builder_dim_key` string,
  `builder_id` string,
  `builder_number` string,
  `builder_website` string,
  `copy_leads_email` string,
  `copy_leads_email_leads_per_message` string,
  `corp_builder_number` string,
  `corp_id` string,
  `corp_name` string,
  `corp_reporting_email` string,
  `corp_state` string,
  `created_by` string,
  `created_date` timestamp,
  `date_is_live` timestamp,
  `default_leads_email` string,
  `default_leads_email_leads_per_message` string,
  `effective_from` timestamp,
  `effective_to` timestamp,
  `is_deleted` string,
  `modified_by` string,
  `modified_date` timestamp,
  `reporting_name` string,
  `send_to_corp_only` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://move-dataeng-lstp-dev/edw/business-data/builders'
TBLPROPERTIES (
  'PARQUET.COMPRESS'='SNAPPY',
  'transient_lastDdlTime'='1518805996')