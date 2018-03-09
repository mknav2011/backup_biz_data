CREATE EXTERNAL TABLE biz_data.community_management(
billingcontact string,
createdate string,
createdate_datetime timestamp,
lastupdated string,
lastupdated_datetime timestamp,
mgmtaddress1 string,
mgmtaddress2 string,
mgmtcity string,
mgmtcontact string,
mgmtemail string,
mgmtfax string,
mgmthomepage string,
mgmtid string,
mgmtlicense string,
mgmtlogo string,
mgmtname string,
mgmtphone string,
mgmtpostalcode string,
mgmtstate string,
pushreports string,
sourcesystem string,
sourcesystemtype string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://move-dataeng-lstp-dev/edw/business-data/communitymanagemnt'
TBLPROPERTIES (
  'PARQUET.COMPRESS'='SNAPPY')

