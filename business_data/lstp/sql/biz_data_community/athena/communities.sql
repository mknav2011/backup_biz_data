
CREATE EXTERNAL TABLE `communities`(
`aahsaflag` string,
`address1` string,
`address2` string,
`att_tollfree` string,
`city` string,
`clientpeoplesoftid` string,
`clientpropertyid` string,
`confirmallcalls` string,
`createdate` timestamp,
`createdby` string,
`email` string,
`fax` string,
`feedsource` string,
`hspscontractid` string,
`hspscustomerid` string,
`ivr_contact_type_id` string,
`ivrdisplocal` string,
`ivrwhisper` string,
`lastupdated` timestamp,
`latitude` string,
`listingtypeid` string,
`longitude` string,
`mgmtid` string,
`mlgflag` string,
`modifiedby` string,
`modifieddate` timestamp,
`multipleleadtypeid` string,
`numunits` string,
`phone` string,
`postalcode` string,
`productid` string,
`propid` string,
`propname` string,
`propstatusid` string,
`sourcesystem` string,
`sourcesystemtype` string,
`state` string,
`tfnassignmentid` string
 )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://move-dataeng-lstp-dev/edw/business-data/communities'
TBLPROPERTIES (
  'PARQUET.COMPRESS'='SNAPPY',
  'transient_lastDdlTime'='1518738822')
