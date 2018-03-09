#!/usr/bin/env bash

env=dev
aws s3 cp ../sql/leads_bd_drop_external_table.sql s3://move-dataeng-code-$env/business-data/sql/leads_bd_drop_external_table.sql
aws s3 cp ../sql/leads_bd_create_external_table.sql s3://move-dataeng-code-$env/business-data/sql/leads_bd_create_external_table.sql
aws s3 cp ../sql/leads_business_dedupe.sql s3://move-dataeng-code-$env/business-data/sql/leads_business_dedupe.sql

aws s3 cp ../script/lead_generic_data_refresh.py s3://move-dataeng-code-$env/business-data/lead_generic_data_refresh.py

aws dynamodb put-item --table-name move-dataeng-common-$env-dl-config --item file://../config/application.leads.edw.bd.leads.json


# Ajay need to use the tools to catch up on the one time first full load