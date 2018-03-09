# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from future.utils import iteritems
 
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

#     sql_query = None 
#     target_table_name= None
#     s3_target_folder_parquet = None
#     s3_staging_folder_csv = None
#     glue_script_location = None
#     glue_job_name = None 
    glue_role = 'move-dataeng-emr-role' 
    def __init__(self, sql_query=None ,target_table_name=None, env =None, s3_target_folder_parquet=None, 
                 glue_script_location=None, glue_dpu_instance_count=None ):    
        """ Derive default  parameters based on inputs: target_table_name 
        """
                  
        self.sql_query = sql_query
        self.target_table_name = target_table_name  
        assert self.sql_query, 'Required argument `sql_query` not found.'
        assert self.target_table_name, 'Required argument `target_table_name` not found.'



        #--- Stage Env
        s3_bucket_name = 'move-dataeng-temp' 
        etl_layer = 'glue-etl/athena_csv'
        if env is None:
            self.env = 'dev'
        else:
            self.env = env
        run_instance = str( uuid.uuid1())
        self.s3_staging_folder_csv = "s3://%s-%s/%s/%s/%s" %(s3_bucket_name, self.env, etl_layer,  target_table_name,run_instance )
        etl_layer = 'glue-etl/parquet_data'
        
        #----
        if s3_target_folder_parquet is None:
            self.s3_target_folder_parquet = "s3://%s-%s/%s/%s" %(s3_bucket_name, self.env, etl_layer,  target_table_name)
        else: 
            self.s3_target_folder_parquet = s3_target_folder_parquet
 
        if glue_script_location is None:
            self.glue_script_location = 's3://move-dataeng-temp-%s/glue-etl/scripts/athena_ctas_part2.py' %(self.env)
        else:
            self.glue_script_location = glue_script_location
            
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
        util.execute_save_s3(self.sql_query, self.s3_staging_folder_csv )
        return True
    

    
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
        glue_job = self.client.create_job(Name = self.glue_job_name,  Role = self.glue_role, 
                                          AllocatedCapacity = self.glue_dpu_instance_count,
                                           Command={'Name': 'glueetl',
                                                    'ScriptLocation': self.glue_script_location}
                                         )
        response = self.client.start_job_run(JobName = self.glue_job_name , Arguments = {
                 '--s3_location_csv_file':   self.s3_staging_folder_csv,
                 '--s3_location_parquet_file' : self.s3_target_folder_parquet, 
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
        print 'Starting Athena Query Execution ...'
        print 
        self.athena_query_execute_save_s3()
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
def ctas_process_wrapper(input_tuple):
#     if debug:
#         print banner( ' task called with '+ str(len(input_tuple)) )
#     athena_ctas_query, target_table_name, s3_target_folder_parquet = input_tuple
#     if debug:
#         print 'Inside ctas_process_wrapper: s3_target_folder_parquet', s3_target_folder_parquet
#     ctas_process(athena_ctas_query, target_table_name, s3_target_folder_parquet  )
    sql_file,input_date,s3_target_folder_parquet_base = input_tuple
    
    start_time = time.time() 
    assert sql_file, 'Required argument `sql_file` not found.'

    if input_date is  None:
        athena_ctas_query = open(sql_file).read()
    else: 
#         y, m , d = input_date.split('-')
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
    ctas = AthenaCTAS(sql_query = athena_ctas_query, target_table_name = target_table_name, s3_target_folder_parquet = s3_target_folder_parquet )
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
    s3_location_target = 's3://move-dataeng-temp-%s/apillai/ctas-test' %(env)
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

def  update_start_and_wait_for_crawler_to_complete(crawler_name, s3_target_path):
    """ Updates and waits for Crawler to execute """
    import boto3, sys, json, time, uuid, datetime, botocore, re, uuid
    region_name = 'us-west-2'
    glue = boto3.client(service_name='glue', region_name= region_name,
              endpoint_url='https://glue.us-west-2.amazonaws.com') 
    # don't start crawler, if it is already in READY state
    ctr = 0
    max_wait_ctr = 20
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
        response = glue.update_crawler(Name=crawler_name, Targets={'S3Targets': [{ 'Path': s3_target_path} ]})
        print 'Crawler %s Updated with \n S3 Path: %s ' %(crawler_name, s3_target_path)
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
        return  response['Crawler']
 

# =================================================
### Main
# =================================================
from argparse import ArgumentParser
def main():

    parser = ArgumentParser(description="CTAS  program to be called with manadatory Input Arguments")
    parser.add_argument('--sql_file', help='SQL File in Local Filesystem')
    parser.add_argument('--source_table', help='Output S3 Folder Location')
    parser.add_argument('--target_table',  help='RunId for this Instance, genarted from Driver script')
    parser.add_argument('--s3_target_folder_parquet_base',  help='Appilcation Name of the pipeline, eg:raw-to-processed-granular')
    ar = parser.parse_args()


    throttle_limit = 5
    max_number_days = throttle_limit * 3
    #max_number_days = 1 #FOR TESTING ONLY
    # sql_file = '/Users/apillai/move-git/athena-ctas/product_event/src/rdc_visitor_aggregate.sql'
    sql_file = ar.sql_file
    source_table = ar.source_table
    target_table = ar.target_table
    s3_target_folder_parquet_base = ar.s3_target_folder_parquet_base
    input_date_list =  get_pdt_partitions2process(source_table, target_table, env='dev')
    input_tuple_list = [] 
    for input_date in input_date_list[:max_number_days]:
        print 'Adding Partition %s to processing Queue' %(input_date)
        input_tuple_list.append((sql_file, input_date, s3_target_folder_parquet_base))

    #sys.exit(0)
    with ThreadPoolExecutor(max_workers= throttle_limit) as executor:
        results = executor.map(ctas_process_wrapper, input_tuple_list)

    for result in results: 
        print "All Partitions loaded"
    
    # Refresh Athena Target table
    #     print refresh_partitions(target_table)
    crawler_name = 'ctas_views_crawler'
    db_name = 'ctas_views_db'
    update_start_and_wait_for_crawler_to_complete(crawler_name, s3_target_folder_parquet_base)
    print 
    text =  'Athena Table = %s is ready for use in Databse = %s \n S3 Location: %s' %(target_table, db_name, s3_target_folder_parquet_base)
    print banner(text)
    print banner('-')  
    print     


if __name__ == '__main__':
    main()
