WITH edw_properties AS
(
select
row_number() OVER (PARTITION BY propertydimkey ORDER BY  effective_to_datetime_mst DESC) AS row_number_seq,
propertydimkey,
propertyid,
propertytype,
propertyyearbuilt,
propertysquarefeet,
propertylotsquarefeet,
propertyaddress,
propertycity,
propertystate,
propertypostalcode,
propertylatitude,
propertylongitude,
propertynumberofbedrooms,
propertynumberofbathrooms,
propertynumberofstories,
propertyarchitecturestyle,
propertytaxassessedvalue,
propertytaxyear,
propertytaxpaidamount,
propertyzoning,
propertyestimatedvalue,
isforeclosure,
propertyforeclosuredate,
propertylastsaledate,
propertylastsaleprice,
effectivefrom,
effectiveto,
propertycountry,
macroneighborhood,
neighborhood,
subneighborhood,
residentialneighborhood,
etl_ztg_id,
etl_source_partition_timestamp,
etl_create_date,
etl_last_update_timestamp,
etl_partition_key_timestamp,
etl_rd_arrival_year,
etl_rd_arrival_month,
etl_rd_arrival_day,
etl_rd_arrival_hour,
etl_source_filename,
etl_invalid_criticality_ind,
etl_dedupe_md5_checksum,
etl_datatype_processed_ind,
property_latitude_double,
property_longitude_double,
property_number_of_bathrooms_double,
property_number_of_stories_double,
property_tax_assessed_value_double,
property_tax_paid_amount_double,
property_estimated_value_double,
property_foreclosure_datetime,
property_last_sale_datetime,
property_last_sale_price_double,
effective_from_datetime_mst,
effective_to_datetime_mst
from
property_dim_All
)
SELECT
propertydimkey,
propertyid,
propertytype,
propertyyearbuilt,
propertysquarefeet,
propertylotsquarefeet,
propertyaddress,
propertycity,
propertystate,
propertypostalcode,
propertylatitude,
propertylongitude,
propertynumberofbedrooms,
propertynumberofbathrooms,
propertynumberofstories,
propertyarchitecturestyle,
propertytaxassessedvalue,
propertytaxyear,
propertytaxpaidamount,
propertyzoning,
propertyestimatedvalue,
isforeclosure,
propertyforeclosuredate,
propertylastsaledate,
propertylastsaleprice,
effectivefrom,
effectiveto,
propertycountry,
macroneighborhood,
neighborhood,
subneighborhood,
residentialneighborhood,
etl_ztg_id,
etl_source_partition_timestamp,
etl_create_date,
etl_last_update_timestamp,
etl_partition_key_timestamp,
etl_rd_arrival_year,
etl_rd_arrival_month,
etl_rd_arrival_day,
etl_rd_arrival_hour,
etl_source_filename,
etl_invalid_criticality_ind,
etl_dedupe_md5_checksum,
etl_datatype_processed_ind,
property_latitude_double,
property_longitude_double,
property_number_of_bathrooms_double,
property_number_of_stories_double,
property_tax_assessed_value_double,
property_tax_paid_amount_double,
property_estimated_value_double,
property_foreclosure_datetime,
property_last_sale_datetime,
property_last_sale_price_double,
effective_from_datetime_mst,
effective_to_datetime_mst
--,lpad(cast(year(effective_to_datetime_mst) as string),4,'0') AS year,
--lpad(cast(month(effective_to_datetime_mst) as string),2,'0') AS month,
--lpad(cast(day(effective_to_datetime_mst) as string),2,'0') AS day,

--'00' as hour
FROM edw_properties WHERE row_number_seq = 1