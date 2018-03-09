SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
-- SET parquet.block.size=268435456;
SET parquet.block.size=536870912;
insert overwrite table  hit_data_parquet_perf 
select *
from hit_data_parquet_128 
;
-- where hour = '12' ;
