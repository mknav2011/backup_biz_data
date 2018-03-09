CREATE EXTERNAL TABLE `biz_data.geography`(
Country string,
CountryID string,
County string,
countyfips string,
CountyID string,
CountyType string,
CountyTypeID string,
dst string,
LatCentroid string,
LatMax string,
LatMin string,
LongCentroid string,
LongMax string,
LongMin string,
Neighborhood string,
NeighborhoodID string,
postalcode string,
state string,
statefips string,
stateid string,
timezone string,
type string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://move-dataeng-geo-dev/xgeo/business-data/geography'
TBLPROPERTIES (
  'PARQUET.COMPRESS'='SNAPPY',
  'transient_lastDdlTime'='1519870171')