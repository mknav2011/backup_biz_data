WITH edw_listings AS
(
select
row_number() OVER (PARTITION BY listingid ORDER BY  listingdimkey ASC) AS row_number_seq,
isaolsyndication,
masterplanid,
specid,
listingnumberofbedrooms,
listingaddress,
dapendingflag,
listingpricereduceddate,
buyeragentsourcealiasid,
listingsourcealiasid,
isvirtualtour,
buyerofficesourcealiasid,
ischoiceenabled,
listingsquarefeet,
hascobroke,
listingid,
openhouseenddate,
listingcoofficesourcealiasid,
listingofficesourcealiasid,
listingcounty,
listingenddate,
listingoriginalprice,
createdby,
ispricedropped,
producttypedimkey,
buyeragentofficedatasourceid,
listingcoagentsourcealiasid,
listingagentsourcealiasid,
listingprovider,
listingstatus,
systemcreationdate,
listingstatuschangeddate,
listingnumberofbathrooms,
listingpostalcode,
listingrentalprice,
listingcity,
listingcoagentofficedatasourceid,
listingpriceincreaseddate,
listinggeoapproximation,
newconstructionflag,
listingsolddate,
listingphotocount,
modifiedby,
rowversion,
listingrawstatus,
createddate,
listinglotsquarefeet,
dapendingdate,
modifieddate,
isvideo,
listingpercentagecompleteness,
effectivefrom,
listingsoldprice,
buyercoofficesourcealiasid,
ispriceincreased,
listingagentofficedatasourceid,
fulfillmentgroupbitmask,
listingphotourl,
hascobrokephone,
listingtype,
listingstate,
headline,
specialmessage,
isrental,
listingdatasourceid,
propertydescription,
isrdcsyndication,
isopenhouse,
listingstartdate,
nonmlsflag,
listingdimkey,
listingcountry,
openhousestartdate,
listingcurrentprice,
listingnumberofstories,
isforeclosure,
listingmarketingtype,
effectiveto,
buyercoagentofficedatasourceid,
headline2,
isdisplayed,
udblistingid,
listingstyle,
propertyid,
buyercoagentsourcealiasid,
etl_datatype_processed_ind,
etl_ztg_id,
etl_invalid_details,
etl_rd_arrival_year,
etl_source_partition_timestamp,
etl_partition_key_timestamp,
etl_last_update_timestamp,
etl_rd_arrival_month,
etl_source_filename,
etl_dedupe_md5_checksum,
etl_invalid_criticality_ind,
etl_rd_arrival_hour,
etl_rd_arrival_day,
etl_create_date,
listing_end_datetime,
effective_to_datetime,
created_datetime,
listing_number_of_bathrooms_double,
listing_sold_datetime,
listing_original_price_double,
system_creation_datetime,
listing_lot_square_feet_double,
da_pending_datetime,
effective_from_datetime,
listing_rental_price_double,
listing_square_feet_double,
modified_created_datetime,
listing_status_changed_datetime,
listing_start_datetime,
listing_price_reduced_datetime,
open_house_start_datetime,
open_house_end_datetime,
listing_current_price_double,
listing_price_increased_datetime,
listing_percentage_completeness_double,
listing_sold_price_double,
modified_datetime,
listing_number_of_stories_double
from
listing_dim_All
)
SELECT
edw_listings.masterplanid,
edw_listings.specid,
edw_listings.listingnumberofbedrooms,
edw_listings.listingaddress,
edw_listings.dapendingflag,
edw_listings.listingpricereduceddate,
edw_listings.buyeragentsourcealiasid,
edw_listings.listingsourcealiasid,
edw_listings.isvirtualtour,
edw_listings.buyerofficesourcealiasid,
edw_listings.ischoiceenabled,
edw_listings.listingsquarefeet,
edw_listings.hascobroke,
edw_listings.listingid,
edw_listings.openhouseenddate,
edw_listings.listingofficesourcealiasid,
edw_listings.listingcounty,
edw_listings.listingenddate,
edw_listings.listingoriginalprice,
edw_listings.createdby,
edw_listings.ispricedropped,
edw_listings.producttypedimkey,
edw_listings.buyeragentofficedatasourceid,
edw_listings.listingagentsourcealiasid,
edw_listings.listingprovider,
edw_listings.listingstatus,
edw_listings.systemcreationdate,
edw_listings.listingstatuschangeddate,
edw_listings.listingnumberofbathrooms,
edw_listings.listingpostalcode,
edw_listings.listingrentalprice,
edw_listings.listingcity,
edw_listings.listingpriceincreaseddate,
edw_listings.listinggeoapproximation,
edw_listings.newconstructionflag,
edw_listings.listingsolddate,
edw_listings.listingphotocount,
edw_listings.modifiedby,
edw_listings.listingrawstatus,
edw_listings.createddate,
edw_listings.listinglotsquarefeet,
edw_listings.dapendingdate,
edw_listings.modifieddate,
edw_listings.isvideo,
edw_listings.listingpercentagecompleteness,
edw_listings.effectivefrom,
edw_listings.listingsoldprice,
edw_listings.buyercoofficesourcealiasid,
edw_listings.ispriceincreased,
edw_listings.listingagentofficedatasourceid,
edw_listings.fulfillmentgroupbitmask,
edw_listings.listingphotourl,
edw_listings.hascobrokephone,
edw_listings.listingtype,
edw_listings.listingstate,
edw_listings.headline,
edw_listings.specialmessage,
edw_listings.isrental,
edw_listings.listingdatasourceid,
edw_listings.propertydescription,
edw_listings.isopenhouse,
edw_listings.listingstartdate,
edw_listings.nonmlsflag,
edw_listings.listingdimkey,
edw_listings.listingcountry,
edw_listings.openhousestartdate,
edw_listings.listingcurrentprice,
edw_listings.listingnumberofstories,
edw_listings.isforeclosure,
edw_listings.listingmarketingtype,
edw_listings.effectiveto,
edw_listings.buyercoagentofficedatasourceid,
edw_listings.headline2,
edw_listings.isdisplayed,
edw_listings.udblistingid,
edw_listings.listingstyle,
edw_listings.propertyid,
edw_listings.buyercoagentsourcealiasid,
edw_listings.etl_datatype_processed_ind,
edw_listings.etl_ztg_id,
edw_listings.etl_invalid_details,
edw_listings.etl_rd_arrival_year,
edw_listings.etl_source_partition_timestamp,
edw_listings.etl_partition_key_timestamp,
edw_listings.etl_last_update_timestamp,
edw_listings.etl_rd_arrival_month,
edw_listings.etl_source_filename,
edw_listings.etl_dedupe_md5_checksum,
edw_listings.etl_invalid_criticality_ind,
edw_listings.etl_rd_arrival_hour,
edw_listings.etl_rd_arrival_day,
edw_listings.etl_create_date,
edw_listings.listing_end_datetime,
edw_listings.effective_to_datetime,
edw_listings.created_datetime,
edw_listings.listing_number_of_bathrooms_double,
edw_listings.listing_sold_datetime,
edw_listings.listing_original_price_double,
edw_listings.system_creation_datetime,
edw_listings.listing_lot_square_feet_double,
edw_listings.da_pending_datetime,
edw_listings.effective_from_datetime,
edw_listings.listing_rental_price_double,
edw_listings.listing_square_feet_double,
edw_listings.modified_created_datetime,
edw_listings.listing_status_changed_datetime,
edw_listings.listing_start_datetime,
edw_listings.listing_price_reduced_datetime,
edw_listings.open_house_start_datetime,
edw_listings.open_house_end_datetime,
edw_listings.listing_current_price_double,
edw_listings.listing_price_increased_datetime,
edw_listings.listing_percentage_completeness_double,
edw_listings.listing_sold_price_double,
edw_listings.modified_datetime,
edw_listings.listing_number_of_stories_double,
--edw_listings.year,
--edw_listings.month,
--edw_listings.day,
--edw_listings.hour,

edw_properties_deduped.propertylatitude,
edw_properties_deduped.propertylongitude

FROM edw_listings
left join edw_properties_deduped ON edw_listings.propertyid=edw_properties_deduped.propertyid
left join edw_usps_geo_data as usps_geo_data ON edw_listings.listingpostalcode=usps_geo_data.PostalCode
WHERE edw_listings.row_number_seq = 1