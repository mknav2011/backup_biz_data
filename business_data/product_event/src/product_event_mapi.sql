select cast((from_unixtime(floor(cast(params__querystring__tm AS double)/1000))-interval '7' hour) AS timestamp) mst_datetime
,a.params__querystring__visitor visitor_id
,a.params__querystring__session visit_id
,params__querystring__chnl property_status_detail
,params__querystring__page page_type
,params__querystring__pgvar page_type_detail
,(case when lower(trim(params__querystring__page)) IN ('ldp','ldp-nhplan') then 'ldp'
when lower(trim(params__querystring__page)) = 'srp' then 'srp'
when lower(trim(params__querystring__page)) = 'nfs' and lower(trim(params__querystring__chnl)) = 'rcsd' then 'ldp'
else 'others'
end) page_type_group
,(case when lower(trim(params__querystring__chnl)) in ('fah','nhms','fcls') and lower(trim(params__querystring__page)) <> 'nfs'  then 'for sale'
when lower(trim(params__querystring__chnl)) = 'rnt' then 'for rent'
when lower(trim(params__querystring__chnl)) = 'rcsd' then 'not for sale'
else 'Others' END) site_section
,(case when lower(trim(params__querystring__chnl)) = 'rcsd' then 'recently sold' 
ELSE 'not applicable' END) sub_site_section
,(case when lower(trim(params__querystring__src)) = 'rdc-android' then 'android core apps'
when lower(trim(params__querystring__src)) in ( 'rdc-iphone','rdc-ipad') then 'ios core apps'
when lower(trim(params__querystring__src)) = 'rdc-android-instant' then 'android instant apps'
else 'others' end) apps_type
,params__querystring__accid account_id
,params__querystring__adtsid advertiser_id
,params__querystring__src source_application_name
,params__querystring__adtyp product_type
,params__querystring__advantage_lid advantage_srp_listings
,params__querystring__agtaid agent_advertiser_id
,params__querystring__basicoo_lid basic_srp_listings
,params__querystring__comid community_id
,params__querystring__env environment
,params__querystring__pgno visit_page_number
,params__querystring__planid planid
,params__querystring__ppage parent_page
,params__querystring__ptnid event_type_id
,params__querystring__rank listing_rank
,params__querystring__schid search_id
,params__querystring__lnkel social_shares
,params__querystring__lid listing_id
,params__querystring__sver source_version
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss')   as etl_created_datetime , 'glue_etl' as etl_created_by
,etl_ztg_id, etl_source_filename  
from cnpd_mapi_pdt.mapi a
WHERE  1=1
AND year = '{year}'
AND month = '{month}'
AND day = '{day}'
