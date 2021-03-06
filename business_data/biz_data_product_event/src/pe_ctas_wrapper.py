# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
 
## ProductEvent BD layer : 
## Daily partitions event_date = YYYYMMDD

# Call AthenUtil from common lib package
# Install common lib: https://wiki.move.com/display/DataEng/Using+Move+Common+Python+Library
# sudo pip install --upgrade  --force-reinstall https://s3-us-west-2.amazonaws.com/move-dl-common-binary-distrubution/python/move_dl_common_api-3.1.1.tar.gz

#
from move_dl_common_api.athena_util import AthenaUtil

import boto3, sys, json, time, uuid, datetime, botocore, re, uuid
class AthenaCTAS(object):
    region_name = 'us-west-2'
    client = boto3.client(service_name='glue', region_name='us-west-2',
                          endpoint_url='https://glue.us-west-2.amazonaws.com') 


    glue_role = 'move-dataeng-emr-role' 
    def __init__(self, sql_query=None ,target_table_name=None, env =None, s3_target_folder_parquet=None, 
                 glue_script_location=None, glue_dpu_instance_count=None, glue_custom_schema_location=None ):    
        """ Derive default  parameters based on inputs: target_table_name 
        """
                  
        self.env = env
        self.sql_query = sql_query
        self.target_table_name = target_table_name  
        assert self.sql_query, 'Required argument `sql_query` not found.'
        assert self.target_table_name, 'Required argument `target_table_name` not found.'



        #--- Stage Env
        s3_bucket_name = 'move-dataeng-temp' 
        etl_layer = 'glue-etl/athena_csv'
        run_instance = str( uuid.uuid1())
        self.s3_staging_folder_csv = "s3://%s-%s/%s/%s/%s" %(s3_bucket_name, self.env, etl_layer,  target_table_name,run_instance )
          
        etl_layer = 'glue-etl/parquet_data'
        #----
        if s3_target_folder_parquet is None:
            self.s3_target_folder_parquet = "s3://%s-%s/%s/%s" %(s3_bucket_name, self.env, etl_layer,  target_table_name)
        else: 
            self.s3_target_folder_parquet = s3_target_folder_parquet
 
        if glue_script_location is None:
            ## Replacing the old glue script with new script  support for custom schema( using column_suffix)
            #self.glue_script_location = 's3://move-dataeng-temp-%s/glue-etl/scripts/athena_ctas_part2.py' %(self.env)
            #self.glue_script_location = 's3://move-dataeng-code-%s/glue-etl/scripts/athena_ctas_csv2parquet.py' %(self.env)
            self.glue_script_location = 's3://move-dataeng-code-%s/glue-etl/scripts/athena_ctas_csv2parquet_perf.py' %(self.env)
        else:
            self.glue_script_location = glue_script_location
            
        if glue_custom_schema_location is None:
            self.glue_custom_schema_location = 's3://move-dataeng-code-%s/glue-etl/scripts/ctas_custom_schema.json' %(self.env)
        else:
            self.glue_custom_schema_location = glue_custom_schema_location            
            
        #glue_dpu_instance_count
        if glue_dpu_instance_count is None:
            self.glue_dpu_instance_count = 10 
        else:
            self.glue_dpu_instance_count = glue_dpu_instance_count

        job_name = '.'.join([re.sub('[^0-9a-zA-Z]+', '',x).title()  for x in self.s3_target_folder_parquet.replace('s3://', '').split('/')[1:] ])
        self.glue_job_name = job_name  

    def athena_query_execute_save_s3(self )  :
        text = "AthenaUtil Initialization .."
        print text 
        util = AthenaUtil(s3_staging_folder = self.s3_staging_folder_csv)
        text = "Started athena_query_execute_save_s3 .."
        print text 
        print 'athena_ctas_query= ',  self.sql_query 
        if util.execute_save_s3(self.sql_query, self.s3_staging_folder_csv ):
            return True
        else:
            return False
    

    
    def  wait_for_job_to_complete(self, JobRunId):
        """ waits for Job to execute """
        text =  'Waiting for JobName = %s and \n JobId=%s  to Complete processing ...' %(self.glue_job_name, JobRunId)
        print text
#         self.banner(text)
        status = "STARTING"  # assumed
        error_count = 0
        response = None
        response =  self.client.get_job_run(JobName=self.glue_job_name, RunId=JobRunId)
        status = response["JobRun"]["JobRunState"] 
        while (status in ("QUEUED','RUNNING, STARTING")):  # 'JobRunState': 'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED',
            try:
                response =  self.client.get_job_run(JobName=self.glue_job_name, RunId=JobRunId)
                status = response["JobRun"]["JobRunState"] 
                # my_print(status)
                time.sleep(50)
            except botocore.exceptions.ClientError as ce:

                error_count = error_count + 1
                if (error_count > 3):
                    status = "FAILED"
                    print(str(ce))
                    break  # out of the loop
                if "ExpiredTokenException" in str(ce):
                    self.client = boto3.session.Session(region_name=self.region_name).client('glue')

        if (status == "FAILED" or status == "STOPPED"):
            # print(response)
            pass

        if response is None:
            return {"SUCCESS": False,
                    "STATUS": status }
        else:
            return  response
        
    def glue_etl_execute_csv2parquet(self):
        text =  'Starting glue_etl_execute_csv2parquet process  for %s ....' %(self.glue_job_name)
        print text
        print 's3_staging_folder_csv=', self.s3_staging_folder_csv
        print 's3_target_folder_parquet = ', self.s3_target_folder_parquet
        print ''
#       # Delete if job already exist
        self.client.delete_job(JobName = self.glue_job_name)
        #TODO Exception handling
        glue_job = self.client.create_job(Name = self.glue_job_name,  Role = self.glue_role, 
                                          AllocatedCapacity = self.glue_dpu_instance_count,
                                           Command={'Name': 'glueetl',
                                                    'ScriptLocation': self.glue_script_location}
                                         )
        #TODO Exception handling
        num_partitions = '10' 
        response = self.client.start_job_run(JobName = self.glue_job_name , Arguments = {
                 '--s3_location_csv_file':   self.s3_staging_folder_csv,
                 '--s3_location_parquet_file' : self.s3_target_folder_parquet, 
                 '--s3_location_custom_schema_json_file' : self.glue_custom_schema_location,
                 '--table_name' :  self.target_table_name,
                 '--num_partitions' : num_partitions })
        return response
    
    def job_cleanup(self):
        print  'Job Cleanup step' 
        return self.client.delete_job(JobName = self.glue_job_name)
    
    def run_job(self):
        print 's3_staging_folder_csv=', self.s3_staging_folder_csv
        print 
        print 's3_target_folder_parquet = ', self.s3_target_folder_parquet
        print 
        print 'glue_script_location = ', self.glue_script_location
        print 
        print 'glue_custom_schema_location = ', self.glue_custom_schema_location
        print
        print 'Starting Athena Query Execution ...'
        print 
        if self.athena_query_execute_save_s3():
            ## Exit If Athena Query Fails 
            print 'Completed Athena Query Execution'
            print 
            print 'Starting Glue Job Execution ...'
            print 
            response = self.glue_etl_execute_csv2parquet()
            self.wait_for_job_to_complete(response['JobRunId'] )
            print 'Completed Glue Job Execution'
            print 
            self.job_cleanup()
            print 'Athena CTAS Done!'
            print 
    


# Call AthenUtil from common lib package
# Install common lib: https://wiki.move.com/display/DataEng/Using+Move+Common+Python+Library
# sudo pip install --upgrade  --force-reinstall https://s3-us-west-2.amazonaws.com/move-dl-common-binary-distrubution/python/move_dl_common_api-3.1.1.tar.gz

#
from move_dl_common_api.athena_util import AthenaUtil

#-- Helper functions
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
debug = 0
#---------

def ctas_process_wrapper(input_tuple):
    
    sql_file,input_date,s3_target_folder_parquet_base,env = input_tuple
    
    start_time = time.time() 
    assert sql_file, 'Required argument `sql_file` not found.'
    assert input_date, 'Required argument `input_date` not found.'
    assert s3_target_folder_parquet_base, 'Required argument `s3_target_folder_parquet_base` not found.'

    if input_date is  None:
        athena_ctas_query = open(sql_file).read()
    else: 
        
        y = input_date[:4]
        m = input_date[4:6]
        d = input_date[6:8]
        data = { 'year': y,'month':m, 'day': d}
        sql_text = open(sql_file).read()
        athena_ctas_query = sql_text.format(**data).replace('\n', '    ') + ';'
    text =  'athena_ctas_query= %s' %(athena_ctas_query)
    print banner('-') 
    print banner(text) 
    print banner('-')  
    target_table_name = (sql_file.split('/')[-1].split('.')[0]).lower()
#     glue_dpu_instance_count = 20
    partition_name = '%s%s%s' %(y, m , d)
    s3_target_folder_parquet = '%s/event_date=%s' %(s3_target_folder_parquet_base, partition_name)
    ctas = AthenaCTAS(sql_query = athena_ctas_query, target_table_name = target_table_name, s3_target_folder_parquet = s3_target_folder_parquet, env=env )
    ctas.run_job() 
        
    print 
    total_run_time =   time.time() - start_time  
    text =  "Process   took   %f seconds for \n target %s" % ( total_run_time, ctas.s3_target_folder_parquet)

    print banner('-') 
    print banner(text) 
    print 'Process Done!'
    print banner('-') 
    return ctas.s3_target_folder_parquet
#-----
def ctas_process_wrapper_hourly(input_tuple):
    
    sql_file,input_date,s3_target_folder_parquet_base,env = input_tuple
    
    start_time = time.time() 
    assert sql_file, 'Required argument `sql_file` not found.'
    assert input_date, 'Required argument `input_date` not found.'
    assert s3_target_folder_parquet_base, 'Required argument `s3_target_folder_parquet_base` not found.'
    # sample input_date = '2018011901'
    if input_date is  None:
        athena_ctas_query = open(sql_file).read()
    else: 
        
        y = input_date[:4]
        m = input_date[4:6]
        d = input_date[6:8]
        h = input_date[8:10]
        data = { 'year': y,'month':m, 'day': d, 'hour': h }
        sql_text = open(sql_file).read()
        athena_ctas_query = sql_text.format(**data).replace('\n', '    ') + ';'
    text =  'athena_ctas_query= %s' %(athena_ctas_query)
    print banner('-') 
    print banner(text) 
    print banner('-')  
    target_table_name = (sql_file.split('/')[-1].split('.')[0]).lower()
#     glue_dpu_instance_count = 20
    #partition_name = '%s%s%s %s' %(y, m , d, h)
    #s3_target_folder_parquet = '%s/event_date=%s' %(s3_target_folder_parquet_base, partition_name)
    s3_target_folder_parquet = '%s/year=%s/month=%s/day=%s/hour=%s' %(s3_target_folder_parquet_base, y, m , d, h)
    print banner('-') 
    print banner(s3_target_folder_parquet) 
    print banner('-')  
    #sys.exit(0)
    ctas = AthenaCTAS(sql_query = athena_ctas_query, target_table_name = target_table_name, s3_target_folder_parquet = s3_target_folder_parquet, env=env )
    ctas.run_job() 
        
    print 
    total_run_time =   time.time() - start_time  
    text =  "Process   took   %f seconds for \n target %s" % ( total_run_time, ctas.s3_target_folder_parquet)

    print banner('-') 
    print banner(text) 
    print 'Process Done!'
    print banner('-') 
    return ctas.s3_target_folder_parquet

def banner(text, ch='=', length=78):
    spaced_text = ' %s ' % text
    banner = spaced_text.center(length, ch)
    return banner
def exec_ssh_cmd(cmd  ):
    """
    Executes cmd Input 
    Also prints std out & std error
    """    
    out_buffer_list =[]
    if debug:
        print( " Started running with  cmd:\n" +  cmd ) 
    try:
        proc = subprocess.Popen(cmd, shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        )
        stdout_value, stderr_value = proc.communicate()
       
        for line in stdout_value.split('\n'):
            if line:
                out_buffer_list.append(line) 
        return  out_buffer_list 
        if stderr_value:
              ret_str = "Error in command: exiting  with  ...%s  Command=%s" %( repr(stderr_value), cmd ) 
              sys.stderr.write( "Error in command: exiting  with  ...%s" %( repr(stderr_value) ) ) 
              return ret_str 
    except Exception, e:
        ret_str = "Exception %s  raised within subprocess.Popen call for :%s" %( e, cmd  )
        sys.stderr.write( "Exception raised within subprocess.Popen call for :", e, cmd  )  
        return ret_str


def get_pdt_partitions2process(source_table, target_table, env='dev' ):
    '''
    Use Athena or LGK to derive to-be processed partitions
    Source PDT table partition in year/month/day/hour format(Hive)
    Target table partition in YYYYMMDD format
    '''
    from move_dl_common_api.athena_util import AthenaUtil
    import json
    import sys
    s3_location_target = 's3://move-dataeng-temp-%s/glue-ctas/athena-results' %(env)
    util = AthenaUtil(s3_staging_folder = s3_location_target)
    data = { 'source_table': source_table,'target_table':target_table}
    query_str_a = '''
       WITH src as(select concat(year, month, day) as event_date, cardinality(array_agg(distinct  hour  ) )as hours_list 
                    from {source_table}  
                    where  year  =  split(cast(current_date as varchar), '-')[1]
                    and ( month  =  split(cast(current_date as varchar), '-')[2]  
                       OR month  =  split(cast(date_add('day', -30, current_date) as varchar), '-')[2] 
                        )
                    group by 1 
                    having cardinality(array_agg(distinct  hour  ) ) = 24 ),
            tgt as ( select distinct event_date
                       from {target_table} )
       select src.event_date 
       from src left outer join tgt on (src.event_date = tgt.event_date )
       where tgt.event_date IS NULL
       order by src.event_date desc '''.format(**data)
   

    query_str_b = '''
       select concat(year, month, day) as event_date, cardinality(array_agg(distinct  hour  ) )as hours_list 
                    from %s
                    where  year  =  split(cast(current_date as varchar), '-')[1]
                    and ( month  =  split(cast(current_date as varchar), '-')[2]  
                       OR month  =  split(cast(date_add('day', -30, current_date) as varchar), '-')[2] 
                        )
                    group by 1 
                    having cardinality(array_agg(distinct  hour  ) ) = 24  
                    order by event_date desc ''' %(source_table)   
    
    
    
    # If no target, return the source list
    try:
        print 'query_str_a=', query_str_a
        df_delta = util.get_pandas_frame(util.execute_query(query_str_a) )
        return sorted(list(df_delta['event_date'] ), reverse=True)
    except:
        print 'Inc Query failed! Falling back to query_str_b=', query_str_b
        df_delta = util.get_pandas_frame(util.execute_query(query_str_b) )
        return sorted(list(df_delta['event_date'] ), reverse=True)

def get_partitions2process(source_table, target_table, env='dev' ):
    '''
    Use Athena or LGK to derive to-be processed partitions
    Source PDT table partition in year/month/day/hour format(Hive)
    Target table partition in YYYYMMDD format
    '''
    from move_dl_common_api.athena_util import AthenaUtil
    import json
    import sys
    s3_location_target = 's3://move-dataeng-temp-%s/glue-ctas/athena-results' %(env)
    util = AthenaUtil(s3_staging_folder = s3_location_target)
    data = { 'source_table': source_table,'target_table':target_table}
    query_str_a = '''
      WITH src as(SELECT DISTINCT event_date  
                  FROM  {source_table}  ),
          tgt as( SELECT DISTINCT event_date  
                  FROM {target_table} )
    select   src.event_date
    from src left outer join  tgt
    ON (src.event_date = tgt.event_date )
    WHERE tgt.event_date IS NULL
    ORDER BY  src.event_date DESC '''.format(**data)
   

    query_str_b = '''
       select distinct  event_date 
                    from %s
                    order by event_date desc ''' %(source_table)   
    
    
    
    # If no target, return the source list
    try:
        print 'query_str_a=', query_str_a
        df_delta = util.get_pandas_frame(util.execute_query(query_str_a) )
        return sorted(list(df_delta['event_date'] ), reverse=True)
    except:
        print 'Inc Query failed! Falling back to query_str_b=', query_str_b
        df_delta = util.get_pandas_frame(util.execute_query(query_str_b) )
        return sorted(list(df_delta['event_date'] ), reverse=True)

def get_pdt_partitions2process_show_partitions(source_table, target_table, env='dev' ):
    '''
    Use Athena or LGK to derive to-be processed partitions
    Source PDT table partition in year/month/day/hour format(Hive)
    Target table partition in YYYYMMDD format
    '''
    from move_dl_common_api.athena_util import AthenaUtil
    import json
    import sys
    s3_location_target = 's3://move-dataeng-temp-%s/glue-ctas/athena-results' %(env)
    util = AthenaUtil(s3_staging_folder = s3_location_target)  
    df_src = util.get_pandas_frame(util.execute_query('''show partitions %s;''' %(source_table))) 
    df_src['date_partition'] = df_src['partition'].apply(lambda part_str: ''.join([ x.split('=')[1] for x in part_str.split('/')][:-1]))
    s = df_src.groupby('date_partition').size() 
    src_list = [y for x, y in zip(s,s.index) if x==24 ]
    #----
    # If no target, return the source list
    try:
        df_tgt = util.get_pandas_frame(util.execute_query(''' show partitions %s;''' %(target_table))) 
        df_tgt['date_partition'] = df_tgt['partition'].apply(lambda x: x.split('=')[1].replace('-', ''))
        return sorted(list(set(src_list) - set(df_tgt['date_partition'] )), reverse=True)
    except:
        return sorted(list(set(src_list)) , reverse=True)

def  update_start_and_wait_for_crawler_to_complete(crawler_name, s3_target_path, db_name):
    """ Updates and waits for Crawler to execute """
    import boto3, sys, json, time, uuid, datetime, botocore, re, uuid
    region_name = 'us-west-2'
    glue = boto3.client(service_name='glue', region_name= region_name,
              endpoint_url='https://glue.us-west-2.amazonaws.com') 
    # don't start crawler, if it is already in READY state
    ctr = 0
    max_wait_ctr = 100
    sleep_time_in_seconds = 10
    while True:
        response = glue.get_crawler(   Name=crawler_name )
        if ctr > max_wait_ctr:
            print 'Sorry! Crawler=%s is not Available yet. Exiting' %(crawler_name)
            return True
        if response['Crawler']['State'] == 'READY' :
            break
        else:
            print 'Crawler %s Busy .. Please wait..' %(crawler_name)
            print "Attempt %d out of %d" %(ctr, max_wait_ctr)
            ctr += 1
            time.sleep(sleep_time_in_seconds)
    response = glue.get_crawler(   Name=crawler_name )        
    if response['Crawler']['State'] == 'READY' :
        #TODO exception handling
        response = glue.update_crawler(Name=crawler_name, DatabaseName=db_name, Targets={'S3Targets': [{ 'Path': s3_target_path} ]})
        print 'Crawler %s Updated with \n S3 Path: %s ' %(crawler_name, s3_target_path)
        print 'Table is available in DBname: %s ' %(db_name)
        glue.start_crawler( Name=crawler_name )
    else:
        print 'Crawler %s Busy .. Please wait..' %(crawler_name)
        print 'Crawler %s \n Details %s..' %(crawler_name, repr(response['Crawler']))
        return True

    
    text =  'Waiting for Crawler = %s   to Complete processing ...' %(crawler_name)
    print text 
    status = "STARTING"  # assumed
    error_count = 0
    response = None
    response =  glue.get_crawler(Name=crawler_name)
    #print response
    status = response['Crawler']['State'] 
    while (status in ("STOPPING','RUNNING, STARTING")):  # 'State': 'READY'|'RUNNING'|'STOPPING',
        try:
            response =  glue.get_crawler(Name=crawler_name)
            status = response['Crawler']['State']
            # my_print(status)
            time.sleep(0.5)
        except botocore.exceptions.ClientError as ce:

            error_count = error_count + 1
            if (error_count > 3):
                status = "FAILED"
                print(str(ce))
                break  # out of the loop
            if "ExpiredTokenException" in str(ce):
                client = boto3.session.Session(region_name=region_name).client('glue')

    if (status == "FAILED" or status == "STOPPED"):
        # print(response)
        pass

    if response is None:
        return {"SUCCESS": False,
                "STATUS": status }
    else:
        print 'Finished Running Crawler = %s ' %(crawler_name) 
        return  response['Crawler']


#----
import re
import datetime
import sys, os
import traceback
import pandas as pd
debug = 0

def date_range(start, end):
     r = (end+datetime.timedelta(days=1)-start).days
     return [start+datetime.timedelta(days=i) for i in range(r)]
def generate_hourly_partitions(start_partition, end_partition):
    if  not end_partition:
        end_partition = str(datetime.datetime.now() - datetime.timedelta(minutes = 60))
        if debug:    
            print start_partition, end_partition
            print start_partition.replace('-','|').replace(':','|').replace(' ', '|')
#     print end_partition.replace('-','|').replace(':','|').replace(' ', '|')
    start = start_partition.replace('-','|').replace(':','|').replace(' ', '|')
    end = end_partition.replace('-','|').replace(':','|').replace(' ', '|')
    if debug:
      print 'Inside main'
      print start, end
      print len(end.split('|') ) 
    year, month, day, hour = map( lambda x: int(x), start.split('|')[:4] ) 
    begin = pd.datetime( year ,  month,  day ,  hour ,0) 
    #-- build end date parts from Thrive partitions
    year, month, day, hour = map( lambda x: int(x), end.split('|')[:4] ) 
    end = pd.datetime(year , month, day , hour,0)
    if debug:
       print 'begin=', begin
       print 'end=', end 
    rng = pd.DatetimeIndex(start=begin, end=end, freq=pd.tseries.offsets.Hour(1))
    partition_clause_list = []
    for idx, dt in   enumerate(rng): 
        if debug:
           print 'dt=', dt
           print '%s%02d%02d%02d' %(dt.year, dt.month, dt.day, dt.hour)
#            print dt.year, dt.month, dt.day, dt.hour
        partition_clause_list.append( ('%s%02d%02d%02d' %(dt.year, dt.month, dt.day, dt.hour)))
    return  partition_clause_list
# =================================================
### Main
# =================================================
from argparse import ArgumentParser
def main():

    parser = ArgumentParser(description="CTAS  program to be called with manadatory Input Arguments\n Prerequisite if Table needs to be created in Athena schema:Pre-create Glue Crawler: crawler_name")
    parser.add_argument('--sql_file', help='SQL File in Local Filesystem')
    parser.add_argument('--source_table', help='Main Source Table/Driver  (Athena) Table for Query, needed for Incremental logic')
    parser.add_argument('--target_table',  help='Target Table in Athena, needed for Incremental logic')
    parser.add_argument('--s3_target_folder_parquet_base',  help='Target Athena Table S3 location; Parquet files created under this location ')
    parser.add_argument('--input_date',  help='(Optional) input_date to run job for a specific date, Date Input in YYYY-MM-DD format ')
    parser.add_argument('--days_offset',  help='(Optional) offset(Number of Days) from input_date to run job ; Use -ve integers for back dates ', default=0, type=int)
    parser.add_argument('--env',  help='Execution Environment (dev, qa, prod). Default=dev ', default='dev' )
    parser.add_argument('--crawler_name',  help='Default crawlername: ctas_etl_crawler; override with passing another crawler ', default='ctas_etl_crawler')
    parser.add_argument('--is_crawler_enabled',  help='Enable Crawler(1=True/ 0=False).  Default(1=True): override with passing 0', default=1, type=int )
    parser.add_argument('--throttle_limit',  help='Number of Concurrent Glue ETL Jobs to process/incremental load. Default 3', default=3, type=int)
    parser.add_argument('--number_of_days2process',  help='Number of days to process/incremental load.  Default= throttle_limit * 3', type=int)
    parser.add_argument('--etl_type',  help='Skinny table from PDT vs Summary table from Skinny tables(Required for Incremental logic only). Values: pdt,summary  ')
    ar = parser.parse_args()

    print
    print banner('-') 
    print 'sql_file                        =', ar.sql_file
    print 'source_table                    =', ar.source_table
    print 'target_table                    =', ar.target_table
    print 's3_target_folder_parquet_base   =', ar.s3_target_folder_parquet_base
    print 'input_date                      =', ar.input_date
    print 'days_offset                     =', ar.days_offset
    print 'env                             =', ar.env
    print 'crawler_name                    =', ar.crawler_name
    print 'is_crawler_enabled              =', ar.is_crawler_enabled
    print 'number_of_days2process          =', ar.number_of_days2process
    print 'throttle_limit                  =', ar.throttle_limit
    print 'etl_type                        =', ar.etl_type
    print
    print banner('-') 

    #if ar.is_crawler_enabled == 1 :
    #   print 'Flag is crawler_flag=', ar.is_crawler_enabled

    #throttle_limit = 3
    if ar.number_of_days2process is None:
        max_number_days = ar.throttle_limit * 3
    else:
        max_number_days = ar.number_of_days2process 
    print 'number_of_days2process=', ar.number_of_days2process
    print 'max_number_days=', max_number_days
   
    #max_number_days = 1 #FOR TESTING ONLY
    sql_file = ar.sql_file
    input_date = ar.input_date
    source_table = ar.source_table
    target_table = ar.target_table
    s3_target_folder_parquet_base = ar.s3_target_folder_parquet_base
    env = ar.env
    etl_type = ar.etl_type
    days_offset = ar.days_offset

    #sys.exit(0)
    assert sql_file, 'Required argument `sql_file` not found.'
    #assert input_date, 'Required argument `input_date` not found.'
    assert source_table, 'Required argument `source_table` not found.'
    assert target_table, 'Required argument `target_table` not found.'
    assert s3_target_folder_parquet_base, 'Required argument `s3_target_folder_parquet_base` not found.'

    #input_date_list =  generate_hourly_partitions(input_date+ ' 00', input_date+ ' 23')
    if input_date is None:
    # Derive days to process from Source /Target tables
        if etl_type == 'pdt':
            input_date_list =  get_pdt_partitions2process(source_table, target_table, env=env )
        else:
            input_date_list =  get_partitions2process(source_table, target_table, env=env)
    elif days_offset > 0:
          end_date = datetime.datetime.strptime(input_date, '%Y-%m-%d') + datetime.timedelta(days=days_offset)
          #input_date_list =  generate_hourly_partitions(input_date+ ' 00:00:00', end_date.strftime('%Y%m%d') + ' 00:00:00' )
          input_date_hour_list =  generate_hourly_partitions(input_date+ ' 00:00:00', end_date.strftime('%Y-%m-%d') + ' 00:00:00' )
          input_date_list = sorted(list(set([ x[:8] for x in input_date_hour_list])))
    else:
        input_date_list = [input_date.replace('-','')]  
    input_tuple_list = [] 
    for input_date in input_date_list[:max_number_days]:
        print 'Adding Partition %s to processing Queue' %(input_date)
        input_tuple_list.append((sql_file, input_date, s3_target_folder_parquet_base, env))

    #sys.exit(0)
    print 'Job Concurrency=%d' %(ar.throttle_limit)
    print 'Number of Days to processed=%d' %(max_number_days)
    parent_start_time = time.time()
    with ThreadPoolExecutor(max_workers= ar.throttle_limit) as executor:
        results = executor.map(ctas_process_wrapper, input_tuple_list)

    #sys.exit(0)
    for result in results: 
        print "All Partitions loaded"
    if ar.is_crawler_enabled == 1 :
        crawler_name = ar.crawler_name
        #Derive db_name from target table db_name.table_name
        db_name = target_table.split('.')[0] #'ctas_views_db'
        update_start_and_wait_for_crawler_to_complete(crawler_name, s3_target_folder_parquet_base, db_name)
        print 
        print banner('-')  
        text =  '\n Athena Table = %s is ready for use in Database = %s \n S3 Location: %s' %(target_table, db_name, s3_target_folder_parquet_base)
        print banner(text)
        print 

    print banner('-')  
    parent_total_run_time =   time.time() - parent_start_time
    text =  "Parent Process   took   %f seconds for \n target %s" % ( parent_total_run_time, s3_target_folder_parquet_base)
    print 'Job Concurrency=%d' %(ar.throttle_limit)
    print 'Number of Days processed=%d' %(max_number_days)
    print banner(text)
    print banner('-')  
    print     


if __name__ == '__main__':
    main()
