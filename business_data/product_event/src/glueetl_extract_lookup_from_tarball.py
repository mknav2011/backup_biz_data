import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
#--
import sys
import zipfile, tarfile, StringIO
import io
#---
def extractall_tarfile(x):
    out_list = []
    compressed_file = StringIO.StringIO(x[1])
    try:
       tar = tarfile.open(mode="r:gz", fileobj = compressed_file)
    except Exception, e:
       print >>sys.stderr, str(e)
       sys.exit(-1)
    for member in tar.getmembers():
          f=tar.extractfile(member.name)
          out_list.append((member.name,f.read()))
    return   out_list
#---
def parse_log(data):
    """
    Generate a dict from data and schema string
    """
    delimiter='\t'
     
    #colnames = ['id', 'name']
    colnames = [ 'col_%02d'%idx for idx, x in  enumerate(data.split(delimiter)) ]
    log      =  (dict(zip(colnames, data.split(delimiter) )))
 
    return log
#----

## @params: [JOB_NAME]
 
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_location_tgz', 
                           's3_location_parquet_file',
                            'lookup_filename'] )

s3_location_source = args['s3_location_tgz']
s3_location_target = args['s3_location_parquet_file']
lookup_filename = args['lookup_filename']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
print 'Starting Parquet Conversion ...'
input_file = '%s/*.tar.gz' %(s3_location_source)
output_folder = s3_location_target # With absolute path

print 'input_file= %s' %(input_file)
print 'output_folder= %s' %(output_folder)
print 'lookup_filename= %s' %(lookup_filename)

zips = sc.binaryFiles(input_file) 
files_data = zips.map(extractall_tarfile) 


tsv_filename = lookup_filename
output_rdd = files_data.flatMap(lambda x: [ el for el in x]).filter(lambda x: x[0] == tsv_filename ).map(lambda x: x[1]).flatMap(lambda x: x.split('\n')) 

print output_rdd.count()
df = output_rdd.map(parse_log).toDF() 
 

df.distinct().write.mode('overwrite').parquet(output_folder)
print 'Done Parquet Conversion !'
df.printSchema()
job.commit()
