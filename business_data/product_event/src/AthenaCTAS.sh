#!/bin/bash -e
#### Check if Required Arguments are provided ##########
USER_NAME=`aws sts get-caller-identity --output text | cut -d $'\t' -f2 | cut -d '/' -f3 | sed 's/@corp.homestore.net//'`
UUID=$(uuidgen)
AWSACCOUNT=`aws sts get-caller-identity --output text | cut -d $'\t' -f2 | cut -d ":" -f5`
FILENAME=$1
CTAS_TABLE=$2
GLUE_DPUS=10
echo "### Setting up AWS Environment########################################"
if [ "$AWSACCOUNT" = "289154003759" ]; then
        export ENV=dev
        echo "Running in $ENV Environment"
elif [ "$AWSACCOUNT" = "609158398538" ]; then
        export ENV=qa
        echo "Running in $ENV Environment"
elif [ "$AWSACCOUNT" = "057425096214" ]; then
        export ENV=prod
        echo "Running in $ENV Environment"
else
        echo "No env Spcified"
        exit -1
fi
echo ""
echo ""

if [ -z $1 ]; then
    echo "No SQL File Provided"
    echo""
    echo "Usage sh AthenaCTAS.sh <SQL FILE PATH> <CTAS_TABLE_NAME> OPTIONAL[<DB_NAME>] OPTIONAL[<TARGET_FOLDER>]"
    echo ""
    echo "Example sh AthenaCTAS.sh ctas.sql information_schema_table"
    echo ""
    exit 2
fi
if [ -z $2 ]; then
    echo "No CTAS Table Name Provided"
    echo""
    echo "Usage sh AthenaCTAS.sh <SQL FILE PATH> <CTAS_TABLE_NAME> OPTIONAL[<DB_NAME>] OPTIONAL[<TARGET_FOLDER>]"
    echo ""
    echo "Example sh AthenaCTAS.sh ctas.sql information_schema_table"
    echo ""
    exit 2
fi
if [ -z $3 ]; then
    echo "No Database Name Provided, Default DB Name will be used"
    echo ""
    DATABASE_NAME=$USER_NAME	
else
    DATABASE_NAME=$3
fi
if [ -z $4 ]; then
    echo "No Taget Folder Provided, Default Taget Path will be Used"
    echo ""
    TARGET_FOLDER=s3://move-dataeng-temp-$ENV/ctas/parquet/$USER_NAME/$UUID/$CTAS_TABLE/
else
    TARGET_FOLDER=$4
fi
echo ""

FILEPATH=s3://move-dataeng-code-$ENV/ctas/$USER_NAME/$UUID/$FILENAME
GLUE_SCRIPT=s3://move-dataeng-code-$ENV/glue-etl/scripts/csv_to_parquet2.py

echo "CHECKING IF AWS CLI IS INSTALLED"

which aws > /dev/null 2>&1
if [ $? != 0 ]; then
	echo "This utility requires the AWS CLI to be installed"\n
	echo ""
	echo "Installing AWS CLI............"
	echo ""
	echo "Please Provide your Sudo Password when asked"
	echo ""
	sudo pip install awscli --upgrade
	if [ $? != 0 ]; then
	    echo "AWS CLI Installation Failed, Please Try again Manually....."
	    exit -2
	fi
else
    echo "AWS CLI already Installed.. Proceeding....."
fi
echo "###### ATHENA CTAS UTILITY ###################"


if [ -f $FILENAME ]; then
   echo "SQL File Path is $FILENAME"
   echo "COPYING FILE........."
   aws s3 cp $FILENAME $FILEPATH
else
   echo "SQL FILE $FILE does not exist."
   echo "exiting......."
   exit -1
fi
echo "###################################"
echo""

echo "SUBMITTING CTAS JOB..................."

aws batch submit-job --job-name $CTAS_TABLE --job-queue athena-ctas-queue --job-definition athena-ctas-definition --parameters DATABASE_NAME=$DATABASE_NAME,FILENAME=$FILENAME,FILEPATH=$FILEPATH,CTAS_TABLE=$CTAS_TABLE,GLUE_SCRIPT=$GLUE_SCRIPT,GLUE_DPUS=$GLUE_DPUS,TARGET_FOLDER=$TARGET_FOLDER --container-overrides command=["ctas.sh","Ref::FILENAME","Ref::CTAS_TABLE","Ref::GLUE_SCRIPT","Ref::GLUE_DPUS","Ref::FILEPATH","Ref::DATABASE_NAME","Ref::TARGET_FOLDER"] --output text > jobid.txt

JOBID=`cat jobid.txt | cut -d $'\t' -f1`

STATUS="RUNNING"

while true; do
STATUS=$(aws batch describe-jobs --jobs $JOBID --query "jobs[].status" --output text)
echo $STATUS
sleep 10
if [[ "${STATUS}" == 'RUNNING' ]]; then
	break
elif [[ "${STATUS}" == 'FAILED' ]]; then 
	echo "Job Submission Failed"
	break 
fi
done

echo "Waiting for Log Stream to get Created"
echo ""
sleep 10
aws batch describe-jobs --jobs $JOBID --query "jobs[].container.logStreamName" --output text > stream.txt
STREAMNAME=`cat stream.txt`
#echo $STREAMNAME
echo "Here is Log Stream URL to Monitor your CTAS Job. Please login to respective AWS Management Console and open Below URL"
echo ""
echo "######################################################################################################################################"
echo ""
echo "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logEventViewer:group=/aws/batch/job;stream=$STREAMNAME"
echo ""
echo "######################################################################################################################################"
echo ""
echo " Finished Submitting CTAS Job"
