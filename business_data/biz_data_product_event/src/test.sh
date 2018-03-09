#!/bin/bash
set -xv
ENV=dev

S3_LOCATION_TGZ=s3://move-dataeng-dropbox-prod/adobe/omniture/mobilelookup
S3_LOCATION_PARQUET_FILE=s3://move-dataeng-omniture-${ENV}/homerealtor/lookups_raw/mobile_attributes/
LOOKUP_FILENAME=mobile_attributes.tsv
GLUE_SCRIPT_LOCATION=s3://move-dataeng-code-${ENV}/glue-etl/utils/glueetl_extract_lookup_from_tarball.py
GLUE_DPU_INSTANCE_COUNT=20
time -p python -u  rdc_omniture_mobile_attributes_lookup_load_wrapper.py --s3_location_tgz ${S3_LOCATION_TGZ} --s3_location_parquet_file ${S3_LOCATION_PARQUET_FILE} --lookup_filename ${LOOKUP_FILENAME} --glue_script_location ${GLUE_SCRIPT_LOCATION} --glue_dpu_instance_count ${GLUE_DPU_INSTANCE_COUNT}
