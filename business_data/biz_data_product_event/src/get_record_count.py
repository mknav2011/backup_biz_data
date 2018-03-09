from move_dl_common_api.athena_util import AthenaUtil
from argparse import ArgumentParser
import boto3, sys, json, time, uuid, datetime, botocore, re, uuid

def get_record_count(table_name, env):
    s3_location_target = 's3://move-dataeng-temp-%s/athena_ctas/tmp/' %(env)
    util = AthenaUtil(s3_staging_folder = s3_location_target) 
    sql_query = """select count(1) as ct from %s""" %(table_name) 
    df_delta = util.get_pandas_frame(util.execute_query(sql_query) ) 
    return ''.join(list(df_delta['ct']))

def main():
    parser = ArgumentParser(description="Program to be called with manadatory Input Arguments")
    parser.add_argument('--tablename', help='Fully Qualified Tablename with DBname. eg: dbname.table_name')
    parser.add_argument('--env',  help='Execution Environment (dev, qa, prod). Default=dev ', default='dev' )
    ar = parser.parse_args()
    table_name = ar.tablename
    env = ar.env
    record_count = get_record_count(table_name, env)
    print "%s,%s" %(table_name, record_count )
    
if __name__ == '__main__':
    main()
