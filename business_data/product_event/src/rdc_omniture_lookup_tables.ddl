create external table browser_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/browser/'

create external table browser_type_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/browser_type/'

create external table connection_type_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/connection_type/'

create external table color_depth_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/color_depth/'

create external table country_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/country/'

create external table event_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/event/'

create external table javascript_version_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/javascript_version/'

create external table languages_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/languages/'

create external table operating_systems_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/operating_systems/'

create external table plugins_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/plugins/'

create external table resolution_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/resolution/'

create external table search_engines_raw(
id string,
name string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/search_engines/'

create external table referrer_type_raw(
id string, 
referrer_type string,
referrer_grouping string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
location 's3://move-dataeng-omniture-prod/homerealtor/raw-data/referrer_type/'


---
CREATE EXTERNAL TABLE mobile_attributes_raw(
`col_00` string, 
`col_01` string, 
`col_02` string, 
`col_03` string, 
`col_04` string, 
`col_05` string, 
`col_06` string, 
`col_07` string, 
`col_08` string, 
`col_09` string, 
`col_10` string, 
`col_11` string, 
`col_12` string, 
`col_13` string, 
`col_14` string, 
`col_15` string, 
`col_16` string, 
`col_17` string, 
`col_18` string, 
`col_19` string, 
`col_20` string, 
`col_21` string, 
`col_22` string, 
`col_23` string, 
`col_24` string, 
`col_25` string, 
`col_26` string, 
`col_27` string, 
`col_28` string, 
`col_29` string, 
`col_30` string, 
`col_31` string, 
`col_32` string, 
`col_33` string, 
`col_34` string, 
`col_35` string, 
`col_36` string, 
`col_37` string, 
`col_38` string, 
`col_39` string, 
`col_40` string, 
`col_41` string, 
`col_42` string, 
`col_43` string, 
`col_44` string, 
`col_45` string, 
`col_46` string, 
`col_47` string, 
`col_48` string, 
`col_49` string)
stored as PARQUET
LOCATION
's3://move-dataeng-temp-prod/glue-etl/omniture/lookups_raw/mobile_attributes/'
