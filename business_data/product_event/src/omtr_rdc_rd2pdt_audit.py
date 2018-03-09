from move_dl_common_api.athena_util import AthenaUtil

import boto3, sys, json, time, uuid, datetime, botocore, re, uuid
import pandas as pd
import json


def get_hour_from_filename(in_filename):
    date, time = in_filename.split('/')[-1] .split('.')[0].split('_')[1].split('-')
    return date+time[:2]

env='dev'
s3_location_target = 's3://move-dataeng-temp-%s/apillai/audit-rd-pdt' %(env)
util = AthenaUtil(s3_staging_folder = s3_location_target)  

##-- 
from argparse import ArgumentParser
parser = ArgumentParser(description="Audit  program to be called with manadatory Input Arguments")
parser.add_argument('--input_date', help='RD Input date in yyyy-mm-dd format')
#input_date = '2017-12-01'
ar = parser.parse_args()
input_date = ar.input_date
s3_base_location = 's3://move-dataeng-omniture-prod/homerealtor/raw-data-uncompressed/hit_data'
y,m,d = input_date.split('-')
table_name = 'move_dl.hit_data_raw'
sql_text = """
ALTER TABLE %s ADD IF NOT EXISTS PARTITION  (year='%s',month='%s',day='%s') location '%s/year=%s/month=%s/day=%s';
""" %(table_name, y, m, d, s3_base_location, y, m, d)
print sql_text
util.execute_query(sql_text)
s3_base_location = 's3://move-dataeng-omniture-prod/homerealtor/raw-data-uncompressed/column_headers'
table_name = 'move_dl.omtr_column_headers_raw'
sql_text = """
ALTER TABLE %s ADD IF NOT EXISTS PARTITION  (year='%s',month='%s',day='%s') location '%s/year=%s/month=%s/day=%s';
""" %(table_name, y, m, d, s3_base_location, y, m, d)
print sql_text
util.execute_query(sql_text)

data = { 'year': y,'month':m, 'day': d}
rd_audit_query = """
WITH dataset as(
SELECT d.source_filename,
         map( m.s3_data ,
         d.s3_data ) AS query_map
FROM 
    (SELECT regexp_split(s3_data,
         '\t' ) AS s3_data ,cardinality(regexp_split(s3_data, '\t' ) ) AS col_num ,"$PATH" AS source_filename ,year, month, day
    FROM move_dl.hit_data_raw   ) d
JOIN 
    (SELECT DISTINCT regexp_split(s3_data,
         '\t' ) AS s3_data ,cardinality(regexp_split(s3_data, '\t' ) ) AS col_num ,year, month, day
    FROM move_dl.omtr_column_headers_raw  ) m
    ON ( d.year = m.year
        AND d.month=m.month
        AND d.day=m.day
        AND   d.col_num = m.col_num )
WHERE m.year = '{year}'
        AND m.month = '{month}'
        AND m.day = '{day}' 
  )
select source_filename, count(1) as rd_ct
from dataset
group by 1
order by 1 
""".format(**data)
print rd_audit_query

df_rd = util.get_pandas_frame(util.execute_query(rd_audit_query))
data = { 'input_date': input_date}
pdt_audit_query = """ 
select etl_source_filename, count(1) as pdt_ct 
from cnpd_omtr_pdt.hit_data_forqa
where cast (concat(year, '-', month, '-', day) as date) 
between  date_add('day', -2, cast( '{input_date}' as date)) 
and date_add('day', 2, cast( '{input_date}' as date))
group by 1
order by 1           
""".format(**data)
print pdt_audit_query

df_pdt = util.get_pandas_frame(util.execute_query(pdt_audit_query))

df_rd_clean = df_rd[1:]
df_pdt_clean = df_pdt[1:]
df_audit = pd.merge(df_rd_clean, df_pdt_clean, how='left', left_on='source_filename', right_on='etl_source_filename')
df_audit.info()
df_audit['hour'] = df_audit['source_filename'].apply(get_hour_from_filename)
df_audit['rd2pdt_delta'] = (df_audit.rd_ct.astype('int') - df_audit.pdt_ct.astype('int') )/df_audit.rd_ct.astype('int')
df_audit['pdt2rd_delta'] = (df_audit.pdt_ct.astype('int') - df_audit.rd_ct.astype('int') )/df_audit.pdt_ct.astype('int')
for idx, row  in df_audit[['source_filename', 'hour', 'rd2pdt_delta', 'pdt2rd_delta', 'rd_ct', 'pdt_ct' ]].iterrows():
    print idx,row.to_json()
