#!/bin/bash
set -xv

ENV=$1

function error_exit()
{
ret_val=$1
process_step="${2}"
log_filename="${3}"
if [ $ret_val -ne 0 ]
then
echo -e "$(date '+%y%m%d %H:%M')\t Script errored at - ${process_step} step..."     ## >>${LOG_FILE}
exit -1
fi
}

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null
export BIN_DIR=$SCRIPTPATH

SCRIPT_NAME=$(basename $0 .sh)
DATE_STR=`date '+%Y-%m-%d_%H%M%S'`
LOG_DIR=/tmp
LOG_FILE=${LOG_DIR}/${SCRIPT_NAME}.${DATE_STR}.out
#--- Mobile Attributes ETL
S3_LOCATION_TGZ=s3://move-dataeng-dropbox-prod/adobe/omniture/mobilelookup
S3_LOCATION_PARQUET_FILE=s3://move-dataeng-omniture-${ENV}/homerealtor/lookups_raw/mobile_attributes/
LOOKUP_FILENAME=mobile_attributes.tsv
GLUE_SCRIPT_LOCATION=s3://move-dataeng-code-${ENV}/glue-etl/utils/glueetl_extract_lookup_from_tarball.py
GLUE_DPU_INSTANCE_COUNT=100
time -p python -u $BIN_DIR/rdc_omniture_mobile_attributes_lookup_load_wrapper.py --s3_location_tgz ${S3_LOCATION_TGZ} --s3_location_parquet_file ${S3_LOCATION_PARQUET_FILE} --lookup_filename ${LOOKUP_FILENAME} --glue_script_location ${GLUE_SCRIPT_LOCATION} --glue_dpu_instance_count ${GLUE_DPU_INSTANCE_COUNT}

error_exit $? " ${SCRIPT_NAME}=> Job failed  " $LOG_FILE
#-- Lookup Loads
S3_TARGET_FOLDER_PARQUET_BASE=s3://move-dataeng-omniture-${ENV}/homerealtor/biz_data_product_event/lookup
DB_NAME=biz_data_product_event
CRAWLER_NAME=ctas_etl_crawler
time -p python -u $BIN_DIR/rdc_omniture_lookup_CTAS_ETL.py --s3_target_folder_parquet_base ${S3_TARGET_FOLDER_PARQUET_BASE}  --db_name ${DB_NAME} --crawler_name ${CRAWLER_NAME}  --throttle_limit 2 --env ${ENV}


error_exit $? " ${SCRIPT_NAME}=> Job failed  " $LOG_FILE
#-------------------

