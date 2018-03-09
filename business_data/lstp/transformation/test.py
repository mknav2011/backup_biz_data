#!/usr/bin/python
import os, sys, json, uuid
import time, datetime
from urlparse import urlparse # For URL Parsing


"""

python glue_wrapper.py --args '{"--JOB_NAME":"ListingDim_Full_Refresh-2010",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/code/custom_lstp/listing_data_refresh_glue_custom.py",
"--glue_dpu_instance_count" :"90",
"--s3_target_path":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/year=2010",
"--s3_latest_usps_geo_data":"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt",
"--s3_latest_property_dim_data":"s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest",
"--s3_source_historical_listing_dim_key_data":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/",
"--s3_source_pdt_listing_data":"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim",
"--source_sql_file_path":"s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/sqls/lstp/biz_data_listings/listing_dim_dedupe_full_refresh_glue_year2010.sql"}'

python glue_wrapper.py --args '{"--JOB_NAME":"ListingDim_Full_Refresh-latest",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/code/custom_lstp/listing_data_refresh_glue_custom.py",
"--glue_dpu_instance_count" :"90",
"--s3_target_path":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=latest/year=9999",
"--s3_latest_usps_geo_data":"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt",
"--s3_latest_property_dim_data":"s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest",
"--s3_source_historical_listing_dim_key_data":"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/",
"--s3_source_pdt_listing_data":"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim",
"--source_sql_file_path":"s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/sqls/lstp/biz_data_listings/listing_dim_dedupe_full_refresh_glue_latest.sql"}'



"""



# Method to read S3 file as a string
def getStringFromFile(bucket_name, key):
    s3_client = boto3.client('s3', region_name='us-west-2')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read()
    return data

def read_parquet_and_create_dataframe_table(s3_source_path = None, temp_table_name = None):
    # Read Source File into a data frame
    df_par_All = spark.read.option("mergeSchema", "true").parquet(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All

def read_textfile_and_create_dataframe_table(s3_source_path = None, flg_file_header="true", temp_table_name = None):
    # Read Source File into a data frame
    #df_par_All = spark.read.option("header", "true").csv("s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt")
    df_par_All = spark.read.option("header", flg_file_header).csv(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All

# Extract the Glue Job Arguments
args = {
  "--JOB_NAME": "ListingDim_Full_Refresh-latest",
  "--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/code/custom_lstp/listing_data_refresh_glue_custom.py",
  "--glue_dpu_instance_count": "90",
  "--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=latest/year=9999",
  "--data_frame_sources": "[{'s3_source_path':'s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt','source_file_type_parquet_or_text':'csv','temp_table_name':'edw_usps_geo_data'},{'s3_source_path':'s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest','source_file_type_parquet_or_text':'parquet','temp_table_name':'edw_properties_deduped'},{'s3_source_path':'s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/','source_file_type_parquet_or_text':'parquet','temp_table_name':'listing_dim_keys_historical'},{'s3_source_path':'s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim','source_file_type_parquet_or_text':'parquet','temp_table_name':'listing_dim_All'}]",
  "--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data/mk/sqls/lstp/biz_data_listings/listing_dim_dedupe_full_refresh_glue_latest.sql",
  "--inputvariables": "[{'<startdate_in_yyyymmdd>':'startdate_in_yyyymmdd','<enddate_in_yyyymmdd>':'enddate_in_yyyymmdd'}]"
}
#1. Variables
s3_target_path = args['--s3_target_path']
data_frame_sources = args['--data_frame_sources']
inputvariables = args['--inputvariables']
source_sql_path = urlparse(args['--source_sql_file_path'])

data_frame_sources = "[{'s3_source_path':'s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt','source_file_type_parquet_or_text':'csv','temp_table_name':'edw_usps_geo_data'},{'s3_source_path':'s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest','source_file_type_parquet_or_text':'parquet','temp_table_name':'edw_properties_deduped'},{'s3_source_path':'s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/','source_file_type_parquet_or_text':'parquet','temp_table_name':'listing_dim_keys_historical'},{'s3_source_path':'s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim','source_file_type_parquet_or_text':'parquet','temp_table_name':'listing_dim_All'}]"
data_frame_sources = data_frame_sources.replace("'",'"')
#Parse Bucket and Prefix of the SQL File Path
s3_bucket_sql_file = source_sql_path.netloc
s3_prefix_sql_file = source_sql_path.path.lstrip('/')

people = [
{'name': "Tom", 'age': 10},
{'name': "Mark", 'age': 5},
{'name': "Pam", 'age': 7}
]

def search(name):
    for p in people:
        if p['name'] == name:
            return p

#aa=json.loads(data_frame_sources)
json_data_frame_sources = json.loads(data_frame_sources)
#print json_data_frame_sources
# for var_data_frame in json_data_frame_sources:
#     if var_data_frame["source_file_type_parquet_or_text"] == "csv":
#         print "csv"
#         #read_textfile_and_create_dataframe_table(var_data_frame["s3_source_path"], "true", var_data_frame["temp_table_name"])
#     elif var_data_frame["source_file_type_parquet_or_text"] == "parquet":
#         print "parquet"
#         #read_parquet_and_create_dataframe_table(var_data_frame["s3_source_path"], var_data_frame["temp_table_name"])

inputvariables="[{startdate_in_yyyymmdd:startdate_in_yyyymmdd,enddate_in_yyyymmdd:enddate_in_yyyymmdd}]"
#print inputvariables
json_inputvariables=json.dumps(inputvariables)
print json_inputvariables
#print dict(inputvariables)
#inputvariables=inputvariables.replace("'",'"')
exit
sql_script_string="select * from tablename where startdate> '<startdate_in_yyyymmdd>' and enddate_in_yyyymmdd='<enddate_in_yyyymmdd>' "
json_inputvariables = json.loads(inputvariables)
for i in json_inputvariables:
    for key, value in i.iteritems():
        print key
        if value=='<GETDATE():format:%Y-%m-%d>':
            value=datetime.datetime.now().strftime("%Y-%m-%d")
        elif value=='<GETDATE():format:%Y-%m-%d %H:%M>':
            value=datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        sql_script_string= sql_script_string.replace(key,str(value))
        print sql_script_string

print sql_script_string



"""
s3_target_path="s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-latest"
s3_latest_usps_geo_data="s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt"
s3_latest_property_dim_data="s3://move-dataeng-lstp-dev/edw/business-data/propertydim/propertydim-latest"
s3_source_pdt_listing_data="s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim"
"""

def read_parquet_and_create_dataframe_table(s3_source_path = None, temp_table_name = None):
    # Read Source File into a data frame
    df_par_All = spark.read.option("mergeSchema", "true").parquet(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All

def read_textfile_and_create_dataframe_table(s3_source_path = None, flg_file_header="true", temp_table_name = None):
    # Read Source File into a data frame
    #df_par_All = spark.read.option("header", "true").csv("s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt")
    df_par_All = spark.read.option("header", flg_file_header).csv(s3_source_path)
    # Create Temp Table for the data frame
    df_par_All.createOrReplaceTempView(temp_table_name)
    return df_par_All


