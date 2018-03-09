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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
filename= 's3://move-dataeng-dropbox-prod/adobe/omniture/mobilelookup/*.tar.gz' #test_tgz/test.tar.gz' #homerealtor_20151123-000000.tar.gz' #instru_ods.tar.gz' #test_tgz.tar.gz' # mysql-connector-java-5.1.39.tar.gz' # instru_ods.tar.gz
zips = sc.binaryFiles(filename) 
files_data = zips.map(extractall_tarfile) 


tsv_filename_base = 'mobile_attributes'
tsv_filename = 'mobile_attributes.tsv'
output_rdd = files_data.flatMap(lambda x: [ el for el in x]).filter(lambda x: x[0] == tsv_filename ).map(lambda x: x[1]).flatMap(lambda x: x.split('\n')) 

print output_rdd.count()
df = output_rdd.map(parse_log).toDF() 
 
bucket_name = 's3://move-dataeng-temp-dev/glue-etl/omniture/lookups'
out_filename = "%s/%s" %(bucket_name,tsv_filename_base)

df.distinct().write.mode('overwrite').parquet(out_filename)
job.commit()
