WITH events as( 
SELECT * 
from    
     cnpd_dtm_pdt.dtm
where   1=1
 AND year = '{year}'  
AND month =  '{month}'
  AND day =  '{day}'
), 
events_with_ptnid as(
select case 
     when regexp_like ( lower(params__querystring__ptnid), 'print_dtl_sell_sp')
     then 589
     when regexp_like ( lower(params__querystring__ptnid), 'result:photo|result:link')
          and    lower(params__querystring__pgvar) like '%srp-map%'
     then 237
     when lower(params__querystring__ptnid) like '%map-view%'
          or lower(params__querystring__ptnid) like '%list-view%'
     then 363
     when lower(params__querystring__ptnid) like '%srp_click%'
          and lower(params__querystring__chnl) like '%new_community%'
     then 508
     when lower(params__querystring__ptnid) like '%page_view%'
          and lower(params__querystring__pgvar) like '%rate_table%'
     then 615
     else b.ptnid
end as ptnid  , 
params__querystring__visitor as visitor_id,
 params__querystring__session as visit_id,
 params__querystring__page as page_type ,
 params__querystring__pgvar as page_variance_id ,
 params__querystring__chnl as channel_id,
 params__querystring__adtyp as rdc_adtype,
 params__querystring__listing_adtsid as listing_advertiser_id,
 params__querystring__adtsid as advertiser_id,
  params__querystring__subid as sub_division_id,
  params__querystring__planid as plan_id,
  params__querystring__comid as community_id ,
  params__querystring__basic_comid as basic_comid,
  context__user_agent,
  params__header__user_agent,
  params__querystring__lid as listing_id,
  params__querystring__dmn as domain_id,
  params__querystring__lnkel as link_element_id,
  params__querystring__accid as consumer_profile_id,
  params__querystring__src as source_application_id,
  params__querystring__env as environment_id,
  params__querystring__tm ,
  params__querystring__tz,
  params__querystring__zip as geo_zip,
  params__querystring__srnwid screen_width,
  params__querystring__starttime,
  concat(year,'-', month,'-', day,' ', hour, ':00:00') as mst_datetime,
  params__querystring__ptnid as raw_ptnid
, etl_source_filename, etl_source_partition_timestamp , etl_approximatearrivaltimestamp,  etl_recordid, etl_ztg_id
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss') as etl_created_datetime , 'glue_etl' as etl_created_by
from events a
  left outer join product_event.ptnid_lookup b on( regexp_like ( lower(a.params__querystring__ptnid), b.ptnid_regex) )
)
select *
from events_with_ptnid 
