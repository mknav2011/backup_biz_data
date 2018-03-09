select cast (concat(year,'-', month,'-', day) as date) as date_mst
,a.params__querystring__visitor visitor_id
,a.params__querystring__session visit_id
,params__querystring__src source_application_name
,params__querystring__chnl property_status_detail
,params__querystring__page page_type
,params__querystring__pgvar page_type_detail
,(case when lower(trim(params__querystring__page)) IN ('ldp','ldp-nhplan') then 'LDP'
when lower(trim(params__querystring__page)) = 'srp' then 'SRP'
when lower(trim(params__querystring__page)) = 'nfs' and lower(trim(params__querystring__chnl)) = 'rcsd' then 'LDP'
else 'Others'
end) page_type_group
,(case when lower(trim(params__querystring__chnl)) in ('fah','nhms','fcls') and lower(trim(params__querystring__page)) <> 'nfs'  then 'For Sale'
when lower(trim(params__querystring__chnl)) = 'rnt' then 'For Rent'
when lower(trim(params__querystring__chnl)) = 'rcsd' then 'Not For Sale'
else 'Others' END) site_section
,(case when lower(trim(params__querystring__chnl)) = 'rcsd' then 'Recently Sold' 
ELSE 'Not Applicable' END) sub_site_section
,(case when lower(trim(params__querystring__src)) = 'rdc-android' then 'Android Core Apps'
when lower(trim(params__querystring__src)) in ( 'rdc-iphone','rdc-ipad') then 'iOS Core Apps'
when lower(trim(params__querystring__src)) = 'rdc-android-instant' then 'Android Instant Apps'
else 'Others' end) apps_type
,SUM(CASE WHEN params__querystring__ptnid = '70' THEN 1 ELSE 0 END) as pv_count
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss')   as etl_created_datetime , 'glue_etl' as etl_created_by
from cnpd_mapi_pdt.mapi a
WHERE  1=1
AND year = '{year}'
AND month = '{month}'
AND day = '{day}'
group by  
cast (concat(year,'-', month,'-', day) as date)  
,a.params__querystring__visitor 
,a.params__querystring__session  
,params__querystring__src  
,params__querystring__chnl  
,params__querystring__page  
,params__querystring__pgvar  
,(case when lower(trim(params__querystring__page)) IN ('ldp','ldp-nhplan') then 'LDP'
when lower(trim(params__querystring__page)) = 'srp' then 'SRP'
when lower(trim(params__querystring__page)) = 'nfs' and lower(trim(params__querystring__chnl)) = 'rcsd' then 'LDP'
else 'Others'
end)  
,(case when lower(trim(params__querystring__chnl)) in ('fah','nhms','fcls') and lower(trim(params__querystring__page)) <> 'nfs'  then 'For Sale'
when lower(trim(params__querystring__chnl)) = 'rnt' then 'For Rent'
when lower(trim(params__querystring__chnl)) = 'rcsd' then 'Not For Sale'
else 'Others' END)  
,(case when lower(trim(params__querystring__chnl)) = 'rcsd' then 'Recently Sold' 
ELSE 'Not Applicable' END)  
,(case when lower(trim(params__querystring__src)) = 'rdc-android' then 'Android Core Apps'
when lower(trim(params__querystring__src)) in ( 'rdc-iphone','rdc-ipad') then 'iOS Core Apps'
when lower(trim(params__querystring__src)) = 'rdc-android-instant' then 'Android Instant Apps'
else 'Others' end)  
