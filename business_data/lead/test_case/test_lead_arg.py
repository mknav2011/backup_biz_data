import sys, json
from urlparse import urlparse # For URL Parsing

from awsglue.utils import getResolvedOptions # For extracting Glue Arguments

# args = getResolvedOptions(sys.argv,
#                           ['JOB_NAME',
#                            'sql_param_variables'
#                            ])

args = getResolvedOptions(sys.argv,
                          ['job_name',
                           's3_target_path',
                           'data_frame_sources',
                           'sql_param_variables',
                           'source_sql_file_path',
                           'block_size_MB'])

#1. Variables
job_name = args['job_name']
#job_name = args['JOB_NAME']
s3_target_path = args['s3_target_path']
data_frame_sources = args['data_frame_sources']
if 'sql_param_variables' in args:
    sql_param_variables = args['sql_param_variables']
source_sql_path_list = args['source_sql_file_path']
source_sql_path_url_list = []
for source_sql_path in source_sql_path_list:
    source_sql_path_url_list.append(urlparse(source_sql_path))

#Parse Bucket and Prefix of the SQL File Path
s3_bucket_sql_list = []
s3_prefix_sql_list = []

for source_sql_path_url in source_sql_path_url_list:
    s3_bucket_sql_list.append(source_sql_path_url.netloc)
    s3_prefix_sql_list.append(source_sql_path_url.path.lstrip('/'))

if "block_size_MB" in args:
    block_size_MB = args['block_size_MB']


#Block size if required for data set with more number of columns
if block_size_MB is not None and type(block_size_MB) == int:
    if int(block_size_MB) > 0:
       block_size = str(1024 * 1024 * int(block_size_MB))
    else:
       block_size = str(1024 * 1024 * 128)
else:
    block_size = str(1024 * 1024 * 128)

#data_frame_sources = data_frame_sources.replace("'",'"')
#inputvariables=inputvariables.replace("'",'"')
print sql_param_variables
if sql_param_variables is not None:
    json_sql_param_variables = json.loads(sql_param_variables)
json_data_frame_sources = json.loads(data_frame_sources)

print json_data_frame_sources
print json_sql_param_variables
