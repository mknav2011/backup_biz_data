# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from argparse import ArgumentParser
import re
import sys
 
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
            self.glue_script_location = 's3://move-dataeng-code-%s/glue-etl/scripts/athena_ctas_csv2parquet.py' %(self.env)
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
                time.sleep(0.5)
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
        response = self.client.start_job_run(JobName = self.glue_job_name , Arguments = {
                 '--s3_location_csv_file':   self.s3_staging_folder_csv,
                 '--s3_location_parquet_file' : self.s3_target_folder_parquet, 
                 '--s3_location_custom_schema_json_file' : self.glue_custom_schema_location,
                 '--table_name' :  self.target_table_name })
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
    

#####
### HELPER FUNCTIONS!!
#####

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
debug = 1

#--
def ctas_sql_stream_wrapper(input_tuple):
    
    athena_ctas_query, db_name, target_table_name, s3_target_folder_parquet, crawler_name,env = input_tuple
    
    start_time = time.time() 
    assert athena_ctas_query, 'Required argument `athena_ctas_query` not found.' 
    assert db_name, 'Required argument `db_name` not found.' 
    assert target_table_name, 'Required argument `target_table_name` not found.' 
    assert s3_target_folder_parquet, 'Required argument `s3_target_folder_parquet` not found.'
    assert env, 'Required argument `env` not found.' 
    
    text =  'athena_ctas_query= %s' %(athena_ctas_query)
    print banner('-') 
    print banner(text) 
    print banner('-')     
    print banner('-') 
    print banner(s3_target_folder_parquet) 
    print banner('-')  
    #sys.exit(0)
    ctas = AthenaCTAS(sql_query = athena_ctas_query, target_table_name = target_table_name, s3_target_folder_parquet = s3_target_folder_parquet, env=env )
    ctas.run_job() 
    
    # Drop table before creating the Athena Table
    text = 'Dropping Table=> %s.%s' %(db_name, target_table_name)
    print banner('-') 
    print banner(text) 
    print banner('-')
    drop_table('%s.%s' %(db_name, target_table_name), env)
    update_start_and_wait_for_crawler_to_complete(crawler_name, s3_target_folder_parquet, db_name)

    
    print 
    total_run_time =   time.time() - start_time  
    text =  "Process   took   %f seconds for \n target %s" % ( total_run_time, ctas.s3_target_folder_parquet)

    print banner('-') 
    print banner(text) 
    print 'Process Done!'
    print banner('-') 
    return ctas.s3_target_folder_parquet


#---
def ctas_process_wrapper_v1(input_tuple):
    
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
                    group by 1 
                    having cardinality(array_agg(distinct  hour  ) ) = 24  
                    order by event_date desc ''' %(source_table)   
    
    
    
    # If no target, return the source list
    try:
        print 'query_str_a=', query_str_a
        df_delta = util.get_pandas_frame(util.execute_query(query_str_a) )
        return sorted(list(df_delta['event_date'][1:] ), reverse=True)
    except:
        print 'Inc Query failed! Falling back to query_str_b=', query_str_b
        df_delta = util.get_pandas_frame(util.execute_query(query_str_b) )
        return sorted(list(df_delta['event_date'][1:] ), reverse=True)

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
        return sorted(list(df_delta['event_date'][1:] ), reverse=True)
    except:
        print 'Inc Query failed! Falling back to query_str_b=', query_str_b
        df_delta = util.get_pandas_frame(util.execute_query(query_str_b) )
        return sorted(list(df_delta['event_date'][1:] ), reverse=True)

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

def prepare_sql(lookup_tablename):
    
    sql_template = """
    WITH data as(
    select distinct    
     concat(regexp_split(split(reverse(split(reverse("$PATH") ,'/')[1]), '.')[1] , '-|_')[2]
    ,regexp_split(split(reverse(split(reverse("$PATH") ,'/')[1]), '.')[1] , '-|_')[3]
            ) as created_seq
    , id, name
    from {table_name}
    ), ranked as(
    select *, row_number() over(partition by id order by created_seq desc) as rnk
    from data
    )
    select distinct id, name, created_seq
    from ranked
    where rnk = 1
    """
    sql_referrer_type = """
    WITH data as(
    select distinct    
     concat(regexp_split(split(reverse(split(reverse("$PATH") ,'/')[1]), '.')[1] , '-|_')[2]
    ,regexp_split(split(reverse(split(reverse("$PATH") ,'/')[1]), '.')[1] , '-|_')[3]
            ) as created_seq
    , id, referrer_type, referrer_grouping
    from {table_name}
    ), ranked as(
    select *, row_number() over(partition by id order by created_seq desc) as rnk
    from data
    )
    select distinct id, referrer_type, referrer_grouping, created_seq
    from ranked
    where rnk = 1 
    """
    sql_mobile_attributes = """
    select distinct 
    col_00 as "mobile_id",
    col_01 as "manufacturer",
    col_02 as "device",
    col_03 as "device_type",
    col_04 as "operating_system",
    col_05 as "diagonal_screen_size",
    col_06 as "screen_height",
    col_07 as "screen_width",
    col_08 as "cookie_support",
    col_09 as "color_depth",
    col_10 as "mp3_audio_support",
    col_11 as "aac_audio_support",
    col_12 as "amr_audio_support",
    col_13 as "midi_monophonic_audio_support",
    col_14 as "midi_polyphonic_audio_support",
    col_15 as "qcelp_audio_support",
    col_16 as "gif87_image_support",
    col_17 as "gif89a_image_support",
    col_18 as "png_image_support",
    col_19 as "jpg_image_support",
    col_20 as "3gpp_video_support",
    col_21 as "mpeg4_video_support",
    col_22 as "3gpp2_video_support",
    col_23 as "wmv_video_support",
    col_24 as "mpeg4_part_2_video_support",
    col_25 as "stream_mp4_aac_lc_video_support",
    col_26 as "stream_3gp_h264_level_10b_video_support",
    col_27 as "stream_3gp_aac_lc_video_support",
    col_28 as "3gp_aac_lc_video_support",
    col_29 as "stream_mp4_h264_level_11_video_support",
    col_30 as "stream_mp4_h264_level_13_video_support",
    col_31 as "stream_3gp_h264_level_12_video_support",
    col_32 as "stream_3gp_h264_level_11_video_support",
    col_33 as "stream_3gp_h264_level_10_video_support",
    col_34 as "stream_3gp_h264_level_13_video_support",
    col_35 as "3gp_amr_nb_video_support",
    col_36 as "3gp_amr_wb_video_support",
    col_37 as "mp4_h264_level_11_video_support",
    col_38 as "3gp_h263_video_support",
    col_39 as "mp4_h264_level_13_video_support",
    col_40 as "stream_3gp_h263_video_support",
    col_41 as "stream_3gp_amr_wb_video_support",
    col_42 as "3gp_h264_level_10b_video_support",
    col_43 as "mp4_acc_lc_video_support",
    col_44 as "stream_3gp_amr_nb_video_support",
    col_45 as "3gp_h264_level_10_video_support",
    col_46 as "3gp_h264_level_13_video_support",
    col_47 as "3gp_h264_level_11_video_support",
    col_48 as "3gp_h264_level_12_video_support",
    col_49 as "stream_http_live_streaming_video_support"
    from  {table_name} 
    """
    if lookup_tablename.endswith("referrer_type_raw"):
        data = { 'table_name': lookup_tablename}
        return sql_referrer_type.format(**data)
    elif     lookup_tablename.endswith("mobile_attributes_raw"):
        data = { 'table_name': lookup_tablename}
        return sql_mobile_attributes.format(**data)
    else:
        data = { 'table_name': lookup_tablename}
        return sql_template.format(**data)

def drop_table(table_name, env):
    s3_location_target = 's3://move-dataeng-temp-%s/athena_ctas/tmp/' %(env)
    util = AthenaUtil(s3_staging_folder = s3_location_target) 
    sql_query = """drop table %s""" %(table_name)
    print sql_query
    QueryExecutionId = util.start_query_execution(sql_query=sql_query)
    results = util.get_results(QueryExecutionId)
    print results
    #print_resultset(results)

def banner(text, ch='=', length=78):
    spaced_text = ' %s ' % text
    banner = spaced_text.center(length, ch)
    return banner

#--- Main ---
def main():
    parser = ArgumentParser(description="program to be called with manadatory Input Arguments")
    parser.add_argument('--s3_target_folder_parquet_base', help='Target S3 Base Location for Lookup table')
    parser.add_argument('--db_name', help='Target DataBase for Lookup tables')
    parser.add_argument('--crawler_name', help='Crawler to use for creating Tables')
    parser.add_argument('--throttle_limit', help='Concurrency rate')
    parser.add_argument('--env', help='Run Environment')
    ar = parser.parse_args()
    #------
    
    print
    print banner('-') 
    print 's3_target_folder_parquet_base                        =', ar.s3_target_folder_parquet_base
    print 'db_name                                              =', ar.db_name
    print 'crawler_name                                         =', ar.crawler_name
    print 'throttle_limit                                       =', ar.throttle_limit
    print 'env                                                  =', ar.env
    print
    print banner('-') 

    #--------

    input_list = ['mobile_attributes','referrer_type','browser','browser_type', 'color_depth', 'connection_type', 'country',
               'event', 'javascript_version', 'languages', 'operating_systems', 'plugins', 'resolution', 'search_engines']
    input_tuple_list = [] 
    #TODO: Parameterize
    #s3_target_folder_parquet_base = 's3://move-dataeng-temp-dev/glue-etl/parquet_data/product_event/lookup'
    #db_name = 'apillai'
    #env = 'dev'
    #crawler_name = 'ctas_etl_crawler' 
    #throttle_limit = 2
    for lkp in input_list:     
        athena_ctas_query =  prepare_sql('move_dl.%s_raw' %(lkp))
        target_table_name = lkp
        s3_target_folder_parquet = '%s/%s' %(ar.s3_target_folder_parquet_base, lkp)
        input_tuple_list.append((athena_ctas_query, ar.db_name, target_table_name, s3_target_folder_parquet, ar.crawler_name, ar.env))
    with ThreadPoolExecutor(max_workers= int(ar.throttle_limit) ) as executor:
        results = executor.map(ctas_sql_stream_wrapper, input_tuple_list)

    for result in results: 
        print "All Partitions loaded"

if __name__ == '__main__':
    main()
