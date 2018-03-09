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
export SQL_DIR=$SCRIPTPATH

SCRIPT_NAME=$(basename $0 .sh)
DATE_STR=`date '+%Y-%m-%d_%H%M%S'`
LOG_DIR=/tmp
LOG_FILE=${LOG_DIR}/${SCRIPT_NAME}.${DATE_STR}.out
starttime=`date`
#----------------------------------------
#---RDC Summary Incremental:
#----------------------------------------
step="product_event_web_summary"
echo "-----------------------------------"
echo " Running $step ......"
echo "-----------------------------------"
SQL_FILE=${SQL_DIR}/product_event_web_summary.sql
SOURCE_TABLE=product_event.rdc_biz_data
TARGET_TABLE=product_event.web_summary
S3_TARGET_FOLDER_PARQUET_BASE=s3://move-dataeng-omniture-dev/homerealtor/product_event/web_summary
THROTTLE_LIMIT=1
NUMBER_OF_DAYS2PROCESS=1
ETL_TYPE=summary
ENV=${1}
time -p python -u ${BIN_DIR}/pe_ctas_wrapper.py --sql_file ${SQL_FILE} --source_table ${SOURCE_TABLE} --target_table ${TARGET_TABLE} --s3_target_folder_parquet_base ${S3_TARGET_FOLDER_PARQUET_BASE} --throttle_limit ${THROTTLE_LIMIT} --etl_type ${ETL_TYPE} --env ${ENV} --number_of_days2process=${NUMBER_OF_DAYS2PROCESS}

step="product_event_web_visit_summary"
echo "-----------------------------------"
echo " Running $step ......"
echo "-----------------------------------"
SQL_FILE=${SQL_DIR}/product_event_web_visit_summary.sql
SOURCE_TABLE=product_event.rdc_biz_data
TARGET_TABLE=product_event.web_visit_summary
S3_TARGET_FOLDER_PARQUET_BASE=s3://move-dataeng-omniture-dev/homerealtor/product_event/web_visit_summary
THROTTLE_LIMIT=1
NUMBER_OF_DAYS2PROCESS=1
ETL_TYPE=summary
ENV=${1}
time -p python -u ${BIN_DIR}/pe_ctas_wrapper.py --sql_file ${SQL_FILE} --source_table ${SOURCE_TABLE} --target_table ${TARGET_TABLE} --s3_target_folder_parquet_base ${S3_TARGET_FOLDER_PARQUET_BASE} --throttle_limit ${THROTTLE_LIMIT} --etl_type ${ETL_TYPE} --env ${ENV} --number_of_days2process=${NUMBER_OF_DAYS2PROCESS}
#--------------------------------------------
#error_exit $? " ${SCRIPT_NAME}=> Job failed  " $LOG_FILE

#----------------------------------------
#--- MAPI Summary Incremental
#----------------------------------------
step="product_event_mobile_app_summary"
echo "-----------------------------------"
echo " Running $step ......"
echo "-----------------------------------"

SQL_FILE=${SQL_DIR}/product_event_mobile_app_summary.sql
SOURCE_TABLE=product_event.mapi_biz_data
TARGET_TABLE=product_event.mobile_app_summary
S3_TARGET_FOLDER_PARQUET_BASE=s3://move-dataeng-mapi-dev/product_event/mobile_app_summary
THROTTLE_LIMIT=1
NUMBER_OF_DAYS2PROCESS=1
ETL_TYPE=summary
ENV=${1}
time -p python -u ${BIN_DIR}/pe_ctas_wrapper.py --sql_file ${SQL_FILE} --source_table ${SOURCE_TABLE} --target_table ${TARGET_TABLE} --s3_target_folder_parquet_base ${S3_TARGET_FOLDER_PARQUET_BASE} --throttle_limit ${THROTTLE_LIMIT} --etl_type ${ETL_TYPE} --env ${ENV} --number_of_days2process=${NUMBER_OF_DAYS2PROCESS}

#error_exit $? " ${SCRIPT_NAME}=> Job failed  " $LOG_FILE
#-------------------
step="product_event_mobile_app_visit_summary"
echo "-----------------------------------"
echo " Running $step ......"
echo "-----------------------------------"

SQL_FILE=${SQL_DIR}/product_event_mobile_app_visit_summary.sql
SOURCE_TABLE=product_event.mapi_biz_data
TARGET_TABLE=product_event.mobile_app_visit_summary
S3_TARGET_FOLDER_PARQUET_BASE=s3://move-dataeng-mapi-dev/product_event/mobile_app_visit_summary
THROTTLE_LIMIT=1
NUMBER_OF_DAYS2PROCESS=1
ETL_TYPE=summary
ENV=${1}
time -p python -u ${BIN_DIR}/pe_ctas_wrapper.py --sql_file ${SQL_FILE} --source_table ${SOURCE_TABLE} --target_table ${TARGET_TABLE} --s3_target_folder_parquet_base ${S3_TARGET_FOLDER_PARQUET_BASE} --throttle_limit ${THROTTLE_LIMIT} --etl_type ${ETL_TYPE} --env ${ENV} --number_of_days2process=${NUMBER_OF_DAYS2PROCESS}


echo "Finished Running Job" >> $LOG_FILE
endtime=`date`
echo "------------------<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>-------------------------" 
echo "---------------@ Process Started ---  at  $starttime " 
echo "---------------@ Process Completed --- at $endtime"
echo "------------------<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>-------------------------" 
