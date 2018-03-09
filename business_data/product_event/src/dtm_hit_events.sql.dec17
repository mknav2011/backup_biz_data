
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
select  
case when lower(params__querystring__ptnid) like '%page_view%' 
     then  70
     when lower(params__querystring__ptnid) like '%cobroke:nearby%'  
     then 109  
     when lower(params__querystring__ptnid) like '%agent:site%'  or lower(params__querystring__ptnid) like '%agentsite%' 
     then 180 
     when lower(params__querystring__ptnid) like '%broker:site%'  or lower(params__querystring__ptnid) like '%brokersite%' 
     then 181 
     when lower(params__querystring__ptnid) like '%office:logo%'  
     then 183   
     when (lower(params__querystring__ptnid) like '%result:photo%'  or lower(params__querystring__ptnid) like '%result:link%') 
          and    lower(params__querystring__pgvar) like '%srp-list%'  
          and   ( lower(params__querystring__adtyp) like  '%basic%' or  lower(params__querystring__adtyp) like  '%cobroke%' )
     then 187  
     when lower(params__querystring__ptnid) like '%result:save%' 
          or lower(params__querystring__ptnid) like '%top:save%' 
          or lower(params__querystring__ptnid) like '%slideshow:save%' 
          or lower(params__querystring__ptnid) like '%persistant-cta:save%' 
          or lower(params__querystring__ptnid) like '%nearby-listing:save%' 
          or lower(params__querystring__ptnid) like '%additional-listings:save%' 
          or lower(params__querystring__ptnid) like '%other-communities:save%' 
          or lower(params__querystring__ptnid) like '%nearby-listings:save%' 
     then 188  
     when lower(params__querystring__ptnid) like '%send_to_friend%' 
     then 190  
     when (lower(params__querystring__ptnid) like '%result:photo%'  or lower(params__querystring__ptnid) like '%result:link%') 
          and    lower(params__querystring__pgvar) like '%srp-list%'  
          and    lower(params__querystring__adtyp) like  '%showcase%'  
     then 203   
     when lower(params__querystring__ptnid) like '%photogallery:photos%'   
     then 204
     when lower(params__querystring__ptnid) like '%get-directions%'   
     then 205 
     when lower(params__querystring__ptnid) like '%schools%'   
     then 206
     when lower(params__querystring__ptnid) like '%agent:otherlistings%'   
     then 207 
     when lower(params__querystring__ptnid) like '%broker:otherlistings%'   
     then 208 
     when lower(params__querystring__ptnid) like '%agentlistings:listings%'   
     then 209 
     when lower(params__querystring__ptnid) like '%virtual_tour%'   
     then 213 
     when lower(params__querystring__ptnid) like '%photogallery:street%'   
     then 223 
     when lower(params__querystring__ptnid) like '%photogallery:map%'   
     then 226 
     when lower(params__querystring__ptnid) like '%open-slideshow%'   
     then 213 
     when lower(params__querystring__ptnid) like '%slideshow:original%'   
     then 230 
     when lower(params__querystring__ptnid) like '%photo:next%'   
     then 231 
     when lower(params__querystring__ptnid) like '%photo:prev%'   
     then 232 
     when lower(params__querystring__ptnid) like '%propertyhistory%'   
     then 236 
     when (lower(params__querystring__ptnid) like '%result:photo%'  or lower(params__querystring__ptnid) like '%result:link%') 
          and    lower(params__querystring__pgvar) like '%srp-map%' 
     then 237
     when lower(params__querystring__ptnid) like '%agent:photo%'   
     then 265 
     when lower(params__querystring__ptnid) like '%broker:photo%'   
     then 266 
     when lower(params__querystring__ptnid) like '%prev_property%'   
     then 267 
     when lower(params__querystring__ptnid) like '%next_property%'   
     then 268 
     when lower(params__querystring__ptnid) like '%estimatepayment%'   
     then 269 
     when lower(params__querystring__ptnid) like '%view_rates%'   
     then 270 
     when lower(params__querystring__ptnid) like '%share:email%'   
     then 295
     when lower(params__querystring__ptnid) like '%share_on_social_media%'   
     then 301 
     when lower(params__querystring__ptnid) like '%my-account:login%'   
     then 310 
     when lower(params__querystring__ptnid) like '%buyer:agent:site%'   
     then 343 
     when lower(params__querystring__ptnid) like '%buyer:broker:site%'   
     then 344 
     when lower(params__querystring__ptnid) like '%more-filters%'   
     then 347 
     when lower(params__querystring__ptnid) like '%search:save%'   
     then 360 
     when lower(params__querystring__ptnid) like '%map-view%'   
          or lower(params__querystring__ptnid) like '%list-view%'  
     then 363 
     when lower(params__querystring__ptnid) like '%special_offer%'   
     then 497 
     when lower(params__querystring__ptnid) like '%green_offer%'   
     then 498 
     when lower(params__querystring__ptnid) like '%srp_click%'   
          and lower(params__querystring__chnl) like '%new_community%' 
     then 508 
     when lower(params__querystring__ptnid) like '%view_availability%'   
     then 515 
     when lower(params__querystring__ptnid) like '%featured-rental%'   
     then 517 
     when lower(params__querystring__ptnid) like '%view_builders_website%'   
     then 541 
     when lower(params__querystring__ptnid) like '%listingprovider:agentprofile%'   
     then 553 
     when lower(params__querystring__ptnid) like '%request_renovation_report%'   
     then 554 
     when lower(params__querystring__ptnid) like '%mini_map_agent_profile_view%'   
     then 572 
     when lower(params__querystring__ptnid) like '%advantage:headshot:mini-profile%'   
     then 573 
     when lower(params__querystring__ptnid) like '%advantage:view-full%'   
     then 575  
     when lower(params__querystring__ptnid) like '%agentprofile%'   
     then 307
     when lower(params__querystring__ptnid) like '%seller_lead_submit%'  
     then 584 
     when lower(params__querystring__ptnid) like '%photo:next%'  
     then 612
     when lower(params__querystring__ptnid) like '%page_view%'        
          and lower(params__querystring__pgvar) like '%rate_table%' 
     then 615 
end as ptnid,
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
,etl_source_filename, etl_source_partition_timestamp , etl_approximatearrivaltimestamp,  etl_recordid, etl_ztg_id 
from events
)
select  *
from events_with_ptnid 
