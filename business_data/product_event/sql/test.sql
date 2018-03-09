 WITH dataset as (
 SELECT  regexp_split(s3_data,
         '\t' ) AS s3_data ,cardinality(regexp_split(s3_data, '\t' ) ) AS col_num ,year, month, day
    FROM move_dl.omtr_column_headers_raw   m
WHERE m.year = '2017'
        AND m.month = '11'
        AND m.day = '01' 
   ) 
select col_num, concat('''', cast(col_num as varchar) ,'''') as col_str,    year, month, day
from dataset
