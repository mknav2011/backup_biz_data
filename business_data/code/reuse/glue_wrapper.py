#!/usr/bin/python
import json
from argparse import ArgumentParser
from move_dl_common_api.glue_util import GlueUtil
"""

==>
a. Working
python glue_wrapper.py --args '{
"--JOB_NAME": "PropertyDim_historical_Refresh_2017",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "50",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/propertydim_mar02/bucket=historical/year=2017",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_edw_pdt__property_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_properties/property_dim_dedupe_full_refresh_glue_year2017.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'
b. Working, DPUs-20, 11 Mins
python glue_wrapper.py --args '{
"--JOB_NAME": "PropertyDim_historical_Refresh_2018",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "20",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/propertydim_mar02/bucket=historical/year=2018",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_edw_pdt__property_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_properties/property_dim_dedupe_full_refresh_glue_year2018.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

c. working, DPUs-40, 13 Mins
python glue_wrapper.py --args '{
"--JOB_NAME": "PropertyDim_latest_Refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "40",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/propertydim_mar02/bucket=latest/year=9999",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/processed-data-xact/property_dim/\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_edw_pdt__property_dim\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-dev/edw/business-data/propertydim/bucket=historical/\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"property_dim_keys_historical\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_properties/property_dim_dedupe_full_refresh_glue_latest.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> Working, 15 Mins, DPUs-40
python glue_wrapper.py --args '{
"--JOB_NAME": "ListingDim_Full_Refresh-year2018",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "40",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/year=2018",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_edw_pdt__listing_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_listings/listing_dim_dedupe_full_refresh_glue_year2018.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

python glue_wrapper.py --args '{
"--JOB_NAME": "ListingDim_Full_Refresh-year2018",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "40",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical/year=2018",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_edw_pdt__listing_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_listings/listing_dim_dedupe_full_refresh_glue_year2018.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> Working, ~50 Mins, DPUs-80
python glue_wrapper.py --args '{
"--JOB_NAME": "ListingDim_Full_Refresh-latest",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "80",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=latest/year=9999",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-dropbox-dev/informatica/header/tblUSPSGeographyDim_jan18.txt\",\"source_file_type_parquet_or_text\":\"csv\",\"is_file_header_exists\":\"true\",\"temp_table_name\":\"edw_usps_geo_data\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-dev/edw/business-data/propertydim_mar02/bucket=latest/year=9999\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"edw_properties_deduped\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-dev/edw/business-data/listingdim/listingdim-glue/bucket=historical\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"listing_dim_keys_historical\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/processed-data-xact/listing_dim\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_edw_pdt__listing_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_listings/listing_dim_dedupe_full_refresh_glue_latest.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> Geo-BD Data (PostalCode, County, City, County, State, Country)  Refresh using generic, Glue Class calling Generic Spark code ==> Working, DPU-1 (~2 Mins)
python glue_wrapper.py --args '{
"--JOB_NAME": "geo_full_refresh-latest",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "1",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/uspsgeo-glue/latest",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_postal_code\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_postal_code\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_neighborhood\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_neighborhood\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_county\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_county\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_msa_minor\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_msa_minor\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_state\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_state\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_subregion\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_subregion\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_region\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_region\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_country\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_country\"},
{\"s3_source_path\":\"s3://move-dataeng-geo-prod/xgeo/processed-data-xact/geo_county_type\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"geo_xgeo_pdt__geo_county_type\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/geo/sql/usps_geo.sql",
"--inputvariables": "[{\"startdate_in_yyyymmdd\":\"startdate_in_yyyymmdd\",\"enddate_in_yyyymmdd\":\"enddate_in_yyyymmdd\"}]",
"--target_partition_by_column":"None"
}'

==> Omniture Lookup Table  Refresh using generic, Glue Class calling Generic Spark code ==> Working, DPU-1 (~9 Mins), DPUs-20
python glue_wrapper.py --args '{
"--JOB_NAME": "omtr_lkp_languages_refresh1",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "20",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/omtr_lookups/languages",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-dropbox-dev/adobe/adobe_omniture_homerealtor/raw-data-rd-uncompressed/languages\",\"source_file_type_parquet_or_text\":\"text\",\"header_file_path\":\"s3://move-dataeng-omniture-dev/raw-data-rd-uncompressed/column_headers/generic_look_up_header/generic_look_up_header.tsv\",\"header_data_delimiter\":\"\\t\",\"temp_table_name\":\"temp_tbl_two_column_lookup\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/omtr/lookuptables/sql/two_column_lookup.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'
--> 20 DPUs, ~20 Mins
python glue_wrapper.py --args '{
"--JOB_NAME": "omtr_lkp_browser_refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "20",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/omtr_lookups/browser",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-omniture-prod/homerealtor/raw-data-uncompressed/browser\",\"source_file_type_parquet_or_text\":\"text\",\"header_file_path\":\"s3://move-dataeng-omniture-dev/raw-data-rd-uncompressed/column_headers/generic_look_up_header/generic_look_up_header.tsv\",\"header_data_delimiter\":\"\\t\",\"temp_table_name\":\"temp_tbl_two_column_lookup\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/omtr/lookuptables/sql/two_column_lookup.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'
--> 20 DPUs, ~20 Mins
python glue_wrapper.py --args '{
"--JOB_NAME": "omtr_lkp_browser_type_refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "20",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/omtr_lookups/browser_type",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-omniture-prod/homerealtor/raw-data-uncompressed/browser_type\",\"source_file_type_parquet_or_text\":\"text\",\"header_file_path\":\"s3://move-dataeng-omniture-dev/raw-data-rd-uncompressed/column_headers/generic_look_up_header/generic_look_up_header.tsv\",\"header_data_delimiter\":\"\\t\",\"temp_table_name\":\"temp_tbl_two_column_lookup\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/omtr/lookuptables/sql/two_column_lookup.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'
--> 20 DPUs, ~10 Mins
python glue_wrapper.py --args '{
"--JOB_NAME": "omtr_lkp_operating_systems_refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "20",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/omtr_lookups/operating_systems",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-omniture-prod/homerealtor/raw-data-uncompressed/operating_systems\",\"source_file_type_parquet_or_text\":\"text\",\"header_file_path\":\"s3://move-dataeng-omniture-dev/raw-data-rd-uncompressed/column_headers/generic_look_up_header/generic_look_up_header.tsv\",\"header_data_delimiter\":\"\\t\",\"temp_table_name\":\"temp_tbl_two_column_lookup\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/omtr/lookuptables/sql/two_column_lookup.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> Working, 6 Mins, DPUs-10
python glue_wrapper.py --args '{
"--JOB_NAME": "Community_BD_Full_Refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "10",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/communities",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/datamanager/processed-data-xact/datamanager_property\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_datamanager_pdt__datamanager_property\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/datamanager/processed-data-xact/datamanager_property_ivr\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_datamanager_pdt__datamanager_property_ivr\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/senior/processed-data-xact/senior_propcontact\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_senior_pdt__senior_propcontact\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/senior/processed-data-xact/senior_property\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_senior_pdt__senior_property\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/senior/processed-data-xact/senior_msrcity\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_senior_pdt__senior_msrcity\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_community/bd_community_refresh.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> Working, 6 Mins, DPUs-10
python glue_wrapper.py --args '{
"--JOB_NAME": "CommunityManagement_BD_Full_Refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "5",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/communitymanagemnt",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/datamanager/processed-data-xact/datamanager_management\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_datamanager_pdt__datamanager_management\"},{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/senior/processed-data-xact/senior_management\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"lstp_senior_pdt__senior_management\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_community_management/bd_community_management_refresh.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> Builder Dim
python glue_wrapper.py --args '{
"--JOB_NAME": "edw_builder_dim_full_refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "1",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/builders",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/raw-data/data/MoveDM_EDW_tblBuilderDim\",\"source_file_type_parquet_or_text\":\"text\",\"header_file_path\":\"s3://move-dataeng-lstp-prod/edw/raw-data/header/MoveDM_EDW_tblBuilderDim/year=2018/month=02/day=01/hour=12/MoveDM_EDW_tblBuilderDim_20180201120106UTC.hdr\",\"header_data_delimiter\":\"^\",\"temp_table_name\":\"lstp_edw_pdt__builder_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_builder/bd_builder_refresh.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> NewHomes Plan
python glue_wrapper.py --args '{
"--JOB_NAME": "edw_newhomes_plan_dim_full_refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "5",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/newhomes_plan",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/raw-data/data/MoveDM_EDW_tblNewHomesPlanDim\",\"source_file_type_parquet_or_text\":\"text\",\"header_file_path\":\"s3://move-dataeng-lstp-prod/edw/raw-data/header/MoveDM_EDW_tblNewHomesPlanDim/year=2018/month=02/day=01/hour=12/MoveDM_EDW_tblNewHomesPlanDim_20180201120129UTC.hdr\",\"header_data_delimiter\":\"^\",\"temp_table_name\":\"lstp_edw_pdt__newhomes_plan_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/lstp/sql/biz_data_newhomes_plan/bd_new_homes_plan_refresh.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> Contract, SFDC, Account
python glue_wrapper.py --args '{
"--JOB_NAME": "edw_newhomes_sub_division_dim_full_refresh",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "5",
"--s3_target_path": "s3://move-dataeng-lstp-dev/edw/business-data/newhomes_sub_division",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-lstp-prod/edw/raw-data/data/MoveDM_EDW_tblNewHomesSubdivisionDim\",\"source_file_type_parquet_or_text\":\"text\",\"header_file_path\":\"s3://move-dataeng-lstp-prod/edw/raw-data/header/MoveDM_EDW_tblNewHomesSubdivisionDim/year=2018/month=02/day=01/hour=12/MoveDM_EDW_tblNewHomesSubdivisionDim_20180201120149UTC.hdr\",\"header_data_delimiter\":\"^\",\"temp_table_name\":\"lstp_edw_pdt__newhomes_sub_division_dim\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/cntr/sql/biz_data_newhomes_sub_division/bd_new_homes_sub_division_refresh.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> cntr_sfdc_accounts,DPUs-5,
python glue_wrapper.py --args '{
"--JOB_NAME": "cntr_sfdc_accounts",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "5",
"--s3_target_path": "s3://move-dataeng-cntr-dev/contracts/businessdata/sfdc_accounts",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-cntr-prod/contracts/processed-data-xact/es_dm_sfdc_account_vw\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"cntr_contract_pdt__es_dm_sfdc_account_vw\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/cntr/sql/cntr_sfdc_account/cntr_sfdc_account.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> cntr_sfdc_products,DPUs-1,
python glue_wrapper.py --args '{
"--JOB_NAME": "cntr_sfdc_products",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "1",
"--s3_target_path": "s3://move-dataeng-cntr-dev/contracts/businessdata/sfdc_products",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-cntr-prod/contracts/processed-data-xact/es_dm_sfdc_product_vw\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"cntr_contract_pdt__es_dm_sfdc_product_vw\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/cntr/sql/cntr_sfdc_product/cntr_sfdc_product.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

==> cntr_sfdc_contracts,DPUs-20,
python glue_wrapper.py --args '{
"--JOB_NAME": "cntr_sfdc_contracts",
"--glue_script_location": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/code/reuse/glue_data_refresh.py",
"--glue_dpu_instance_count": "20",
"--s3_target_path": "s3://move-dataeng-cntr-dev/contracts/businessdata/sfdc_contracts",
"--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-cntr-prod/contracts/processed-data-xact/es_dm_sfdc_asset_vw\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"cntr_contract_pdt__es_dm_sfdc_asset_vw\"},{\"s3_source_path\":\"s3://move-dataeng-cntr-dev/contracts/businessdata/sfdc_accounts\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"cntr_sfdc_accounts\"},{\"s3_source_path\":\"s3://move-dataeng-cntr-dev/contracts/businessdata/sfdc_products\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"cntr_sfdc_products\"}]",
"--source_sql_file_path": "s3://aws-glue-scripts-289154003759-us-west-2/biz_data_delete/mk/cntr/sql/cntr_sfdc_contract/cntr_sfdc_contract.sql",
"--inputvariables": "[]",
"--target_partition_by_column":"None"
}'

"""
#TODO
"""
#Dependancies
PDT-Listing
PDT-Properties
PDT-GeoData-Latest (No Avaiable)

PDT-BD-Properties

PDT-BD-Listings
#Final table Refresh Strategy
"""

def main():
    #Parse Input Argemnt
    parser = ArgumentParser(description="Glue Util  program to be called with required Input Arguments")
    parser.add_argument('--args', help='Glue Job Arguments')
    input_arguments = parser.parse_args()

    var_args = input_arguments.args

    var_args = json.loads(var_args)

    var_glue_job_name = var_args['--JOB_NAME']
    var_glue_script_location = var_args['--glue_script_location']
    var_glue_dpu_instance_count = var_args['--glue_dpu_instance_count']

    #Initialize Glue Class
    glueutil = GlueUtil()

    #Create and Excute Glue Job
    glueutil.execute_job(glue_job_name = var_glue_job_name,
                         glue_script_location = var_glue_script_location,
                         glue_dpu_instance_count = int(var_glue_dpu_instance_count),
                         script_args = var_args)
if __name__ == "__main__":
    main()
