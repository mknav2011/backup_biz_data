# from move_dl_common_api.athena_util import AthenaUtil
# import json
# import sys
#-----
import boto3, sys, json, time, uuid, datetime, botocore, re
import threading
from multiprocessing.pool import ThreadPool
import traceback

'''
Created on Aug, 2017
@author: sbellad
'''


def my_print(msg):
    print("{} {}".format(datetime.datetime.utcnow(), msg))


# thread local data
thread_data = threading.local()


class AthenaUtil(object):
    region_name = 'us-west-2'
    client = boto3.session.Session(region_name=region_name).client('athena')

    s3_staging_folder = None

    def __init__(self, s3_staging_folder):
        """ constructor requires s3 staging folder for storing results
        Parameters:
        s3_staging_folder = s3 folder with write permissions for storing results
        """
        self.s3_staging_folder = s3_staging_folder

    def execute_save_s3(self, sql_query, s3_output_folder):
        QueryExecutionId = self.start_query_execution(sql_query=sql_query, s3_output_folder=s3_output_folder)
        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)
        if result_status_result["SUCCESS"]:
            return True
        else:
            raise Exception(result_status_result)

    def execute_query(self, sql_query, use_cache=False):
        """ executes query and returns results
        Parameters:
        sql_query = SQL query to execute on Athena
        use_cache = to reuse previous results if found (might give back stale results)

        Returns:
        ResultSet see http://boto3.readthedocs.io/en/latest/reference/services/athena.html#Athena.Client.get_query_results
        """
        QueryExecutionId = self.start_query_execution(sql_query=sql_query, use_cache=use_cache)
        return self.get_results(QueryExecutionId=QueryExecutionId)

    def start_query_execution(self, sql_query,  use_cache=False, s3_output_folder=None):
        """ starts  query execution
        Parameters:
        sql_query = SQL query to execute on Athena
        use_cache = to reuse previous results if found (might give back stale results)

        Returns:
        QueryExecutionId that  identifies the query, can be used to get_results or get_results_by_page
        """
        outputLocation = self.s3_staging_folder if s3_output_folder is None else s3_output_folder
        print(outputLocation)
        response = self.client.start_query_execution(
            QueryString=sql_query,
            ClientRequestToken=str(uuid.uuid4()) if not use_cache else sql_query[:64].ljust(32) + str(hash(sql_query)),
            ResultConfiguration={
                'OutputLocation': outputLocation,
            }
        )
        return response["QueryExecutionId"]
        # return self.get_results(QueryExecutionId=response["QueryExecutionId"])

    def get_results(self, QueryExecutionId):
        """ waits for query to complete and returns results
        Parameters:
        QueryExecutionId that  identifies the query


        Returns:
        ResultSet see http://boto3.readthedocs.io/en/latest/reference/services/athena.html#Athena.Client.get_query_results
        or Exception in case of error
        """
        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)
        # print(result_status_result)
        result = None
        if result_status_result["SUCCESS"]:
            paginator = self.client.get_paginator('get_query_results')
            page_response = paginator.paginate(QueryExecutionId=QueryExecutionId)
            # PageResponse Holds 1000 objects at a time and will continue to repeat in chunks of 1000.

            for page_object in page_response:

                if result is None:
                    result = page_object
                    if result_status_result["QUERY_TYPE"] == "SELECT":
                        if len(result["ResultSet"]["Rows"]) > 0:
                            # removes column header from 1st row (Athena returns 1st row as col header)
                            del result["ResultSet"]["Rows"][0]
                else:
                    result["ResultSet"]["Rows"].extend(page_object["ResultSet"]["Rows"])
            result["ResponseMetadata"]["HTTPHeaders"]["content-length"] = None
            return result
        else:
            raise Exception(result_status_result)

    def __wait_for_query_to_complete(self, QueryExecutionId):
        """ private do not user, waits for query to execute """
        status = "QUEUED"  # assumed
        error_count = 0
        response = None
        while (status in ("QUEUED','RUNNING")):  # can be QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
            try:
                response = self.client.get_query_execution(QueryExecutionId=QueryExecutionId)
                status = response["QueryExecution"]["Status"]["State"]
                # my_print(status)
                time.sleep(0.5)
            except botocore.exceptions.ClientError as ce:

                error_count = error_count + 1
                if (error_count > 3):
                    status = "FAILED"
                    print(str(ce))
                    break  # out of the loop
                if "ExpiredTokenException" in str(ce):
                    self.client = boto3.session.Session(region_name=self.region_name).client('athena')

        if (status == "FAILED" or status == "CANCELLED"):
            # print(response)
            pass

        if response is None:
            return {"SUCCESS": False,
                    "STATUS": status
                , "QUERY_TYPE": response["QueryExecution"]["Query"].strip().upper().split(" ")[0]
                , "QUERY": response["QueryExecution"]["Query"]
                , "StateChangeReason": response["QueryExecution"]["Status"][
                    "StateChangeReason"] if "StateChangeReason" in response["QueryExecution"]["Status"] else None}
        else:
            return {"SUCCESS": True if response["QueryExecution"]["Status"]["State"] == "SUCCEEDED" else False,
                    "STATUS": response["QueryExecution"]["Status"]["State"]
                , "QUERY_TYPE": response["QueryExecution"]["Query"].strip().upper().split(" ")[0]
                , "QUERY": response["QueryExecution"]["Query"]
                , "StateChangeReason": response["QueryExecution"]["Status"][
                    "StateChangeReason"] if "StateChangeReason" in response["QueryExecution"]["Status"] else None}

    def get_results_by_page(self, QueryExecutionId, NextToken=None, MaxResults=1000):
        """ waits for query to complete and returns result based on NextToken and MaxResults per page.
        Parameters:
        QueryExecutionId that  identifies the query
        NextToken from previous page of result set
        MaxResults max records per page

        Returns:
        ResultSet for a Page see http://boto3.readthedocs.io/en/latest/reference/services/athena.html#Athena.Client.get_query_results
        or Exception in case of error
        """
        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)
        # print(result_status_result)
        if result_status_result["SUCCESS"]:
            if NextToken is None:
                MaxResults = MaxResults + 1
                result = self.client.get_query_results(QueryExecutionId=QueryExecutionId, MaxResults=MaxResults)
                # removes column header from 1st row (Athena returns 1st row as col header)

                if result_status_result["QUERY_TYPE"] == "SELECT":
                    if len(result["ResultSet"]["Rows"]) > 0:
                        del result["ResultSet"]["Rows"][0]
            else:
                result = self.client.get_query_results(QueryExecutionId=QueryExecutionId, NextToken=NextToken,
                                                       MaxResults=MaxResults)
            return result

    def get_table_partitions(self, table_name):
        """ queries and returns partitions of a table in Athena
        Parameters:
        table_name name of table name with database prefix

        Returns:
        array of string partitions
        """
        sql_query = "SHOW PARTITIONS " + table_name
        my_print("executing  athena SQL {}".format(sql_query))
        QueryExecutionId = self.start_query_execution(sql_query=sql_query)
        result = self.get_results(QueryExecutionId=QueryExecutionId)
        output = []
        for row in result["ResultSet"]["Rows"]:
            for data in row["Data"]:
                output.append(data["VarCharValue"])
        athena_table_partitions = sorted(output)
        return athena_table_partitions



    def start_query_execution_and_wait_for_completion(self, sql):
        """ starts a query execution an waits for it to complete, use this for last queries where u need a query id and want to download results page by page.
        Parameters:
        sql  query to execute

        Returns:
        query_status_result dictionary with following structure:
                    {"SUCCESS" : False | True,
                    "STATUS" :  status
                    , "QUERY_TYPE" : "FIRST WORD OF QUERY e.g. SELECT or INSERT or ALTER"
                    , "QUERY" : "ACTUAL QUERY"
                    , "StateChangeReason" : None | "Error string if any"}
        """
        query_status_result = None
        for attempt in range(3):
            try:
                util = None
                if hasattr(thread_data, 'util') == False:
                    thread_data.util = AthenaUtil(self.s3_staging_folder)

                util = thread_data.util
                result = util.start_query_execution(sql_query=sql)
                query_status_result = util.__wait_for_query_to_complete(result)
                # print(query_status_result)
                if query_status_result["SUCCESS"] == True:
                    return query_status_result
                else:
                    pass
                    # print("attempt",attempt,query_status_result, sql)
            except Exception as e:
                print("attempt", attempt, str(e), sql)
                time.sleep((attempt + 1) ** 2)
        return None

    def execute_sqls_threaded(self, sql_queries, thread_pool_size=5):
        """ executes a array of SQLs using threads and returns results, useful for threaded batch operations
        Parameters:
        sql_queries array of SQL queries to execute
        thread_pool_size pool size to use, MAX a/c limit in PROD is 50 so its recommended to keep it around 2-5.

        Returns:
        True if all SQLs have been executed successfully, else False
        """
        if len(sql_queries) == 0:
            return True

        start_time = time.time()

        if (thread_pool_size < 1):
            thread_pool_size = 1
        POOL_SIZE = thread_pool_size

        if (len(sql_queries) < POOL_SIZE):
            POOL_SIZE = len(sql_queries)
        # Make the Pool of workers
        pool = ThreadPool(POOL_SIZE)

        print("Using pool size of {}".format(POOL_SIZE))

        count = 0
        failed_count = 0
        OPERATIONS = len(sql_queries)
        result = True
        while ((count + failed_count) < OPERATIONS):
            # print(count,failed_count,OPERATIONS)
            try:
                for i, r in enumerate(
                        pool.imap_unordered(self.start_query_execution_and_wait_for_completion, sql_queries), 1):
                    try:
                        # print(i,r)

                        if r is None:
                            failed_count = failed_count + 1
                            result = False
                        elif "SUCCESS" in r and r["SUCCESS"] == False:
                            failed_count = failed_count + 1
                            result = False
                            # break
                        else:
                            print(r["QUERY"])
                            count += 1
                            # your code
                        # elapsed_time = time.time() - start_time
                        # sys.stderr.write('\r{0:%} {} {}'.format((count*1.0/OPERATIONS),count,elapsed_time))
                        sys.stderr.write(
                            '\r{0:%} completed {1}, failed {2}, TOTAL: {3}'.format((count * 1.0 / OPERATIONS), count,
                                                                                   failed_count, OPERATIONS))
                    except Exception as e:
                        # print(traceback.format_exc())
                        print("#", str(e))
                        failed_count += 1
                        # print('#',sys.exc_info()[1])
                        # pass
            except Exception as e:
                # print(traceback.format_exc())
                print(str(e))
                failed_count += 1
                # print('$',sys.exc_info()[1])
                pass
        print("test_threaded_metric_log --- %s seconds ---for %s get ops using %s threads" % (
        (time.time() - start_time), OPERATIONS, POOL_SIZE))
        print("total: " + str(OPERATIONS) + ", failed: " + str(failed_count))

        # close the pool and wait for the work to finish
        pool.close()
        pool.join()

        if ((result == True and count == OPERATIONS)):
            print("Operation successful")
            return True
        else:
            print("Operation had errors")
            raise Exception("Operation had errors")

    # utilities to convert result object in pandas dataframe

    def get_header(self, result):
        col = result['ResultSet']['ResultSetMetadata']['ColumnInfo']
        header = list(map(lambda x: x['Name'], col))
        # print(header)
        return header


    def get_pandas_frame(self, result):
        '''
        utilities to convert result object in pandas dataframe
        :param result: object returned by boto athena client containing data & meta data
        :return: pandas dataframe
            - df with column header & data
            - empty dataframe with columns header only in case of resultset is empty
            - none if irregular shape result object
            - throws error in any other case
        '''
        import pandas
        try:
            header = self.get_header(result)
            data = result['ResultSet']['Rows']
            print(data)
            if len(data) > 0 :
                df = pandas.io.json.json_normalize(data)
                df[header] = df['Data'].apply(pandas.Series)
                df = df.drop(['Data'], axis=1)
                df = df.applymap(lambda x: x['VarCharValue'])
            else:
                df = pandas.DataFrame(columns=header)

            return df
        except KeyError:
            print('No Data!')
            print(traceback.format_exc())
            return None


#------
def print_resultset(  resultset, limit=100000):
    print(json.dumps(resultset["ResponseMetadata"]))
    if "ResultSet" in resultset:
        if "ResultSetMetadata" in resultset["ResultSet"]:
            if "ColumnInfo" in resultset["ResultSet"]["ResultSetMetadata"]:
                for column in resultset["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]:
                    sys.stdout.write(column["Label"] if "Label" in column else column["Name"])
                    sys.stdout.write("\n")
#                 print()
        
        counter = 0
        out_list = []
        if "Rows" in resultset["ResultSet"]:
            
            for row in resultset["ResultSet"]["Rows"]:
                counter += 1
                if limit is not None and counter > limit:
                    print("Limiting output of result set to first {} rows".format(limit))
                    break
                row_list = []    
                for keydict in row["Data"]:
#                     print keydict
                    if not keydict:
#                         row_list.append('-1')
                        row_list.append(None)
                    for key in keydict:
                        val = ''
                        try:
                           val =  keydict[key]
#                            sys.stdout.write(val)
#                            sys.stdout.write("\n")
                           
                        except:
                            val = '-1'
                        row_list.append(val)
#                 print row_list
                out_list.append(row_list)           
        return  out_list 
#---------------------
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
    
#------
import re
class AthenaCTAS(object):
    region_name = 'us-west-2'
    client = boto3.client(service_name='glue', region_name='us-west-2',
                          endpoint_url='https://glue.us-west-2.amazonaws.com') 

    s3_staging_folder_csv = None
    s3_target_folder_parquet = None
    glue_script_location = None
    glue_job_name = None
    glue_role = None
    util = None
    glue_job = None

    def __init__(self,  s3_staging_folder_csv, s3_target_folder_parquet, glue_role, glue_script_location):
        """ constructor requires s3 staging folder for storing results
        Parameters:
        s3_staging_folder = s3 folder with write permissions for storing results
        """
        print 's3_staging_folder_csv=', s3_staging_folder_csv
        print 's3_target_folder_parquet = ', s3_target_folder_parquet
        self.s3_staging_folder_csv = s3_staging_folder_csv
        self.s3_target_folder_parquet = s3_target_folder_parquet
#         self.sql_query = sql_query
        # Athena Initialization 
        self.util = AthenaUtil(s3_staging_folder = self.s3_staging_folder_csv)
        #Glue Create Job derive from s3_target_folder_parquet
        job_name = '.'.join([re.sub('[^0-9a-zA-Z]+', '',x).title()  for x in s3_target_folder_parquet.replace('s3://', '').split('/')[1:] ])
        self.glue_job_name = job_name #'athena_ctas_part2'
        glue_job = self.client.create_job(Name= self.glue_job_name,  Role = glue_role,  
                      Command={'Name': 'glueetl',
                               'ScriptLocation': glue_script_location})
        
        self.glue_script_location = glue_script_location # 's3://move-dataeng-temp-dev/glue-etl/scripts/athena_ctas_part2.py'
    
    def athena_query_execute_save_s3(self, sql_query )  :
        text = "Started athena_query_execute_save_s3 .."
        print text
#         self.banner(text)
        print 'athena_ctas_query= ',  sql_query 
        self.util.execute_save_s3(sql_query, self.s3_staging_folder_csv )
        return True
    

    
    def  wait_for_job_to_complete(self, JobRunId):
        """ waits for query to execute """
        text =  'Waiting for JobName = %s and  JobId=%s  to Complete processing ...' %(self.glue_job_name, JobRunId)
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
        
    def glue_etl_execute_csv2parquet(self, target_table_name):
        text =  'Starting glue_etl_execute_csv2parquet process  for %s ....' %(self.glue_job_name)
        print text
        print 's3_staging_folder_csv=', self.s3_staging_folder_csv
        print 's3_target_folder_parquet = ', self.s3_target_folder_parquet
        print ''
#         self.banner(text)
        response = self.client.start_job_run(JobName = self.glue_job_name , Arguments = {
                 '--s3_location_csv_file':   self.s3_staging_folder_csv,
                 '--s3_location_parquet_file' : self.s3_target_folder_parquet, 
                 '--table_name' :  target_table_name })
        return response
#         if self.wait_for_job_to_complete(response['JobRunId']):
#             return True
#         else:
#             return False
    def job_cleanup(self):
        self.banner("Job Cleanup")
        return self.client.delete_job(JobName = self.glue_job_name)
    
    @staticmethod
    def banner(text, ch='=', length=78):
        spaced_text = ' %s ' % text
        banner = spaced_text.center(length, ch)
        return banner
#----- 
def get_partitions2process(source_table, target_table, env='dev' ):
    '''
    Use Athena or LGK to derive to-be processed partitions
    '''
    from move_dl_common_api.athena_util import AthenaUtil
    import json
    import sys
    s3_location_target = 's3://move-dataeng-temp-%s/apillai/ctas-test' %(env)
    util = AthenaUtil(s3_staging_folder = s3_location_target)
    athena_ctas_query = '''show partitions %s;''' %(source_table)
    print 'athena_ctas_query= ', athena_ctas_query
    result =  util.execute_query(athena_ctas_query)
    df_src = util.get_pandas_frame(result) 
    df_src['date_partition'] = df_src['partition'].apply(lambda part_str: '-'.join([ x.split('=')[1] for x in part_str.split('/')][:-1]))
    #----
    athena_ctas_query = ''' show partitions %s;''' %(target_table)
    print 'athena_ctas_query= ', athena_ctas_query 
    result =  util.execute_query(athena_ctas_query)
    df_tgt = util.get_pandas_frame(result) 
    df_tgt['date_partition'] = df_tgt['partition'].apply(lambda x: x.split('=')[1])
    return sorted(list(set(df_src['date_partition'] ) - set(df_tgt['date_partition'] )), reverse=True)

def get_pdt_partitions2process(source_table, target_table ):
    '''
    Use Athena or LGK to derive to-be processed partitions
    Source PDT table partition in year/month/day/hour format(Hive)
    Target table partition in YYYYMMDD format
    '''
    from move_dl_common_api.athena_util import AthenaUtil
    import json
    import sys
    s3_location_target = 's3://move-dataeng-temp-dev/apillai/ctas-test'
    util = AthenaUtil(s3_staging_folder = s3_location_target)  
    df_src = util.get_pandas_frame(util.execute_query('''show partitions %s;''' %(source_table))) 
    df_src['date_partition'] = df_src['partition'].apply(lambda part_str: ''.join([ x.split('=')[1] for x in part_str.split('/')][:-1]))
    #----
    df_tgt = util.get_pandas_frame(util.execute_query(''' show partitions %s;''' %(target_table))) 
    df_tgt['date_partition'] = df_tgt['partition'].apply(lambda x: x.split('=')[1].replace('-', ''))
    return sorted(list(set(df_src['date_partition'] ) - set(df_tgt['date_partition'] )), reverse=True)


def refresh_partitions(target_table, env = 'dev' ):
    '''
    Use Athena or LGK to derive to-be processed partitions
    '''
    from move_dl_common_api.athena_util import AthenaUtil
    import json
    import sys
    s3_location_target = 's3://move-dataeng-temp-%s/apillai/ctas-test' %(env)
    util = AthenaUtil(s3_staging_folder = s3_location_target)
    athena_ctas_query = '''msck repair table   %s;''' %(target_table)
    print 'athena_ctas_query= ', athena_ctas_query
    result =  util.execute_query(athena_ctas_query)
    return result


#------------
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
debug = 0
def ctas_process_wrapper(input_tuple):
    if debug:
        print banner( ' task called with '+ str(len(input_tuple)) )
    athena_ctas_query, target_table_name, s3_target_folder_parquet = input_tuple
    if debug:
        print 'Inside ctas_process_wrapper: s3_target_folder_parquet', s3_target_folder_parquet
    ctas_process(athena_ctas_query, target_table_name, s3_target_folder_parquet  )

def banner(text, ch='=', length=78):
    spaced_text = ' %s ' % text
    banner = spaced_text.center(length, ch)
    return banner


def ctas_process (sql_query, target_table_name, s3_target_folder_parquet ):
    import uuid
    import time
    # at the beginning:
    start_time = time.time()
    #--- Stage Env
    run_instance = str( uuid.uuid1())
    print 'run_instance=', run_instance
    s3_bucket_name = 'move-dataeng-temp'
    env = 'dev' 
    etl_layer = 'glue-etl/athena_csv'
    run_instance = str( uuid.uuid1())
    s3_staging_folder_csv = "s3://%s-%s/%s/%s/%s" %(s3_bucket_name, env, etl_layer,  target_table_name, run_instance )
    print 's3_staging_folder_csv= ', s3_staging_folder_csv 
    glue_role = 'move-dataeng-emr-role' 
    glue_script_location = 's3://move-dataeng-temp-dev/glue-etl/scripts/athena_ctas_part2.py'
    ctas = AthenaCTAS(s3_staging_folder_csv, s3_target_folder_parquet, glue_role, glue_script_location)
    text =  'Starting Athena Query Execution for target %s ...' %(s3_target_folder_parquet)
    print ctas.banner('-')
    print ctas.banner(text)
    ctas.athena_query_execute_save_s3(sql_query)
    text =  'Completed Athena Query Execution for csv file = %s  and target = %s ...' %(s3_staging_folder_csv, s3_target_folder_parquet)
    print ctas.banner('-')
    print ctas.banner(text)
    text = 'Starting Glue Job Execution for target %s ...' %(s3_target_folder_parquet)
    print ctas.banner(text)
    response = ctas.glue_etl_execute_csv2parquet(target_table_name)
    glue_job_run_id = response['JobRunId']
    ctas.wait_for_job_to_complete(glue_job_run_id )
    text =  'Completed Glue Job Execution for target %s ...' %(s3_target_folder_parquet)
    print ctas.banner('-')
    print ctas.banner(text)
    # Do not delete Job Logs; Needed for diagnosis
    ctas.job_cleanup() 
    text = 'Athena CTAS Done for target %s ...' %(s3_target_folder_parquet)
    print ctas.banner(text)
    # at the end of the program:
    total_run_time =   time.time() - start_time  
    print "Glue Job glue_job_run_id = %s took   %f seconds for target %s" % (glue_job_run_id, total_run_time, s3_target_folder_parquet)
    print ctas.banner('-')
    out_str = '|'.join([target_table_name, s3_target_folder_parquet, glue_job_run_id,  str(total_run_time)]) 
    print ctas.banner('result= ' + out_str)
    if debug:
        print ctas.banner('result= ' + out_str)
    #return '|'.join([target_table_name, s3_target_folder_parquet, glue_job_run_id,  str(total_run_time)])
    return out_str

def done(n):
    print("Done: {}".format(n))

def main():
    from datetime import date, timedelta
    yesterday = date.today() - timedelta(1)
    #print yesterday.strftime('%m%d%y')
    print yesterday.strftime('%Y-%m-%d')
    input_date = yesterday.strftime('%Y-%m-%d')  # T-1
    throttle_limit = 5
    max_number_days = throttle_limit * 3
    max_number_days = 5 #FOR TESTING ONLY
    sql_file = 'hiren_visitor_aggregate.sql' # SQL file needs to be in the same location as this script
    source_tables = ['pdt_master.pdt_hit_data'] # Check LGK dependency for each of these tables
    target_table_name = 'hm_rdc_visitor_aggregate'
    input_date_list = ['2017-11-10', '2017-11-11', '2017-11-12', '2017-11-13', '2017-11-14', '2017-11-15', '2017-11-16'   ]
    source_table = 'adobe_parity.home_realtor_hit_data'
    target_table = 'athena_ctas_poc.rdc_visitor_aggregate'
    input_date_list =  get_partitions2process(source_table, target_table)
    #input_date_list = [input_date]
    input_tuple_list = []
    for input_date in input_date_list[:max_number_days]:
        print banner ('_')
        print banner ('_')
        print banner ('Main Process Starting ....')
        print banner ('_')
        print banner ('_')
        print banner (input_date)
        y, m , d = input_date.split('-')
        data = { 'year': y,'month':m, 'day': d}
        sql_text = open(sql_file).read()
        athena_ctas_query = sql_text.format(**data).replace('\n', '    ') + ';'
        if debug:
             text =  'athena_ctas_query= %s' %(  athena_ctas_query)
             print banner(  text )
        partition_name = '%s%s%s' %(y, m , d)
        s3_target_folder_parquet = 's3://move-dataeng-temp-dev/glue-etl/parquet_data/%s/event_date=%s' %(target_table_name, partition_name) 
                               
        print banner( 's3_target_folder_parquet =' + s3_target_folder_parquet )
        input_tuple_list.append((athena_ctas_query, target_table_name, s3_target_folder_parquet))
        
    with ThreadPoolExecutor(max_workers= throttle_limit) as executor:
        results = executor.map(ctas_process_wrapper, input_tuple_list)

        for result in results:
            done(result) 
    print "All Partitions loaded"
    # Refresh Athena Target table
    print refresh_partitions(target_table)

if __name__ == '__main__':
    main()
