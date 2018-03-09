CREATE EXTERNAL TABLE `listings_history`(
  `created_by` string,
  `da_pending_flag` int,
  `etl_create_date` timestamp,
  `etl_datatype_processed_ind` string,
  `etl_dedupe_md5_checksum` string,
  `etl_invalid_criticality_ind` string,
  `etl_invalid_details` string,
  `etl_last_update_timestamp` timestamp,
  `etl_partition_key_timestamp` timestamp,
  `etl_rd_arrival_day` string,
  `etl_rd_arrival_hour` string,
  `etl_rd_arrival_month` string,
  `etl_rd_arrival_year` string,
  `etl_source_filename` string,
  `etl_source_partition_timestamp` timestamp,
  `etl_ztg_id` string,
  `fulfillment_group_bitmask` int,
  `geo_city` string,
  `geo_city_id` string,
  `geo_country` string,
  `geo_country_id` string,
  `geo_county` string,
  `geo_county_fips` string,
  `geo_county_id` string,
  `geo_county_type` string,
  `geo_county_type_id` string,
  `geo_state` string,
  `geo_state_fips` string,
  `geo_state_id` string,
  `has_cobroke` int,
  `head_line` string,
  `head_line2` string,
  `is_choice_enabled` int,
  `is_displayed` int,
  `is_fore_closure` int,
  `is_open_house` int,
  `is_price_dropped` int,
  `is_price_increased` int,
  `is_rental` int,
  `is_video` int,
  `listing_address` string,
  `listing_agent_mls_id` string,
  `listing_agent_mls_set_id` string,
  `listing_city` string,
  `listing_country` string,
  `listing_county` string,
  `listing_current_price` double,
  `listing_data_source_id` string,
  `listing_dim_key` string,
  `listing_id` string,
  `listing_lot_square_feet` double,
  `listing_number_of_bath_rooms` double,
  `listing_number_of_bed_rooms` int,
  `listing_number_of_stories` double,
  `listing_office_mls_id` string,
  `listing_office_mls_set_id` string,
  `listing_original_price` double,
  `listing_photo_count` int,
  `listing_postal_code` string,
  `listing_provider` string,
  `listing_raw_status` string,
  `listing_rental_price` double,
  `listing_sold_price` double,
  `listing_square_feet` double,
  `listing_state` string,
  `listing_status` string,
  `listing_style` string,
  `listing_type` string,
  `master_plan_id` string,
  `mls_id` string,
  `mls_property_id` string,
  `modified_by` string,
  `mst_created_datetime` timestamp,
  `mst_da_pending_datetime` timestamp,
  `mst_effective_from_datetime` timestamp,
  `mst_effective_to_datetime` timestamp,
  `mst_listing_end_datetime` timestamp,
  `mst_listing_price_increased_datetime` timestamp,
  `mst_listing_price_reduced_datetime` timestamp,
  `mst_listing_sold_datetime` timestamp,
  `mst_listing_start_datetime` timestamp,
  `mst_listing_status_changed_datetime` timestamp,
  `mst_modified_created_datetime` timestamp,
  `mst_modified_datetime` timestamp,
  `mst_open_house_end_datetime` timestamp,
  `mst_open_house_start_datetime` timestamp,
  `mst_system_creation_datetime` timestamp,
  `new_construction_flag` string,
  `non_mls_flag` int,
  `product_type_dimkey` string,
  `property_description` string,
  `property_id` string,
  `property_latitude` string,
  `property_longitude` string,
  `spec_id` string,
  `special_message` string)
PARTITIONED BY (
  `bucket` string,
  `year` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue'
TBLPROPERTIES (
  'PARQUET.COMPRESS'='SNAPPY',
  'transient_lastDdlTime'='1517699683')