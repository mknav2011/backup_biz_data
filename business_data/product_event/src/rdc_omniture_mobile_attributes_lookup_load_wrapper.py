from move_dl_common_api.glue_util import GlueUtil
from argparse import ArgumentParser
import re
import sys

#s3_location_tgz = 's3://move-dataeng-dropbox-prod/adobe/omniture/mobilelookup'
#s3_location_parquet_file = 's3://move-dataeng-temp-dev/glue-etl/omniture/lookups_raw/mobile_attributes/'
#lookup_filename = 'mobile_attributes.tsv'
#glue_job_name = 'rdc_omniture_mobile_attributes_lookup_load'
#glue_script_location = 's3://move-dataeng-code-dev/glue-etl/utils/glueetl_extract_lookup_from_tarball.py'
#glue_dpu_instance_count = 100


def banner(text, ch='=', length=78):
    spaced_text = ' %s ' % text
    banner = spaced_text.center(length, ch)
    return banner

def main():

    parser = ArgumentParser(description="program to be called with manadatory Input Arguments")
    parser.add_argument('--s3_location_tgz', help='Tarball loaction on S3')
    parser.add_argument('--s3_location_parquet_file', help='Target File location for output file genarted in Parquet format')
    parser.add_argument('--lookup_filename', help='Filename of Lookup data in the Tarball')
    parser.add_argument('--glue_script_location', help='Glue ETL script s3 location saved on S3')
    parser.add_argument('--glue_dpu_instance_count', help='Total DPU required for the ETL')



    ar = parser.parse_args()
    print
    print banner('-') 
    print 's3_location_tgz                        =', ar.s3_location_tgz
    print 's3_location_parquet_file               =', ar.s3_location_parquet_file
    print 'lookup_filename                        =', ar.lookup_filename
    print 'glue_script_location                   =', ar.glue_script_location
    print 'glue_dpu_instance_count                =', ar.glue_dpu_instance_count
    print
    print banner('-') 
    args_dict ={ '--s3_location_tgz': ar.s3_location_tgz,
           '--s3_location_parquet_file': ar.s3_location_parquet_file,
           '--lookup_filename': ar.lookup_filename,}
    glue_util = GlueUtil()
    glue_job_name = '.'.join([re.sub('[^0-9a-zA-Z]+', '',x).title()  for x in ar.s3_location_parquet_file.replace('s3://', '').split('/')[1:] ])
    print 'glue_job_name=', glue_job_name
    #sys.exit(0)
    glue_util.execute_job(glue_job_name=glue_job_name,
                          glue_script_location=ar.glue_script_location,
                          glue_dpu_instance_count=int(ar.glue_dpu_instance_count),
                          script_args=args_dict)
    
if __name__ == '__main__':
    main()
