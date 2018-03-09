CREATE EXTERNAL TABLE `properties`(
  `etl_create_date` timestamp,
  `etl_datatype_processed_ind` string,
  `etl_dedupe_md5_checksum` string,
  `etl_invalid_criticality_ind` string,
  `etl_last_update_timestamp` timestamp,
  `etl_partition_key_timestamp` timestamp,
  `etl_rd_arrival_day` string,
  `etl_rd_arrival_hour` string,
  `etl_rd_arrival_month` string,
  `etl_rd_arrival_year` string,
  `etl_source_filename` string,
  `etl_source_partition_timestamp` timestamp,
  `etl_ztg_id` string,
  `is_foreclosure` int,
  `macro_neighborhood` string,
  `mst_effective_from_datetime` timestamp,
  `mst_effective_to_datetime` timestamp,
  `mst_property_foreclosure_datetime` timestamp,
  `mst_property_last_sale_datetime` timestamp,
  `neighborhood` string,
  `property_address` string,
  `property_architecture_style` string,
  `property_city` string,
  `property_country` string,
  `property_dim_key` string,
  `property_estimated_value` double,
  `property_id` string,
  `property_last_sale_price` double,
  `property_latitude` string,
  `property_longitude` string,
  `property_lot_square_feet` int,
  `property_number_of_bath_rooms` double,
  `property_number_of_bed_rooms` int,
  `property_number_of_stories` double,
  `property_postal_code` string,
  `property_square_feet` int,
  `property_state` string,
  `property_tax_assessed_value` double,
  `property_tax_paid_amount` double,
  `property_tax_year` int,
  `property_type` string,
  `property_year_built` int,
  `property_zoning` string,
  `residential_neighborhood` string,
  `sub_neighborhood` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-glue/bucket=latest'
TBLPROPERTIES (
  'PARQUET.COMPRESS'='SNAPPY',
  'transient_lastDdlTime'='1517815096')