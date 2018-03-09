#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null
export BIN_DIR=$SCRIPTPATH
SCRIPT_NAME=$(basename $0 .sh)
DATE_STR=`date '+%Y-%m-%d_%H%M%S'`
LOG_DIR=/tmp
LOG_FILE=${LOG_DIR}/${SCRIPT_NAME}.${DATE_STR}.out
ERROR_FILE=${LOG_DIR}/${SCRIPT_NAME}.${DATE_STR}.err

echo > $LOG_FILE

starttime=`date`
#---------------------------METADATA SETTINGS----------------------
#------- TWO types of lookup tables
# default lookups have only TWO columns
# referrer_type has THREE columns
#-- set metadata for regular lookups and referer_type table
if [ "${lookup_tablename}" = "referrer_type" ]
then
meta_string=" 'id\treferrer_type\treferrer_grouping' "
else
meta_string=" 'id\tname' "
fi
#---- end of metadata settings ----------------

#--- ET creation
lookup_tablename_raw=${lookup_tablename}
step="CREATE_TABLE_${lookup_tablename_raw}"
echo " Running $step ......"
HIVE_SCRIPT_FILE=/tmp/${step}_dynamic_hive_script.hql
cat > $HIVE_SCRIPT_FILE << @EOF
drop table ${lookup_tablename_raw};
create external table ${lookup_tablename_raw}( 
s3_data string
)
partitioned by ( year STRING,  month STRING, day STRING, hour STRING) 
STORED AS TEXTFILE
LOCATION    '${RD_LOOKUP_BASE}'
;

@EOF
cat $HIVE_SCRIPT_FILE
hive -v -f $HIVE_SCRIPT_FILE
#-----

DB_NAME=apillai
SQL_FILE=/tmp/browser.sql
TARGET_TABLE_NAME=browser
TARGET_S3_LOCATION="s3://move-dataeng-temp-dev/apillai/lookups/browser_new"
$BIN_DIR/AthenaCTAS.sh ${SQL_FILE} browser apillai s3://move-dataeng-temp-dev/apillai/lookups/browser_new
#$BIN_DIR/AthenaCTAS.sh browser.sql browser apillai s3://move-dataeng-temp-dev/apillai/lookups/browser_new

