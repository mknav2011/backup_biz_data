WITH visit_aggregate as (
SELECT 
 substr(date_time, 1, 10)   as dt,
post_visid_high || '-' || post_visid_low AS visitor_id, 
post_visid_high || '-' || post_visid_low||'-'||visit_num as visit_id,
post_pagename AS page,
post_channel, 
post_prop40 as page_type,
va_closer_id   , 
b.name as traffic_source,
a.mobile_id, c.manufacturer, c.device_type, c.operating_system,   
SUM(CASE WHEN post_page_event = '0' THEN 1 ELSE 0 END) AS hit_cnt 
FROM omniture.hit_data a 
left outer join   apillai_test_db.va_closer b
on(a.va_closer_id  = cast(b.id as varchar) )
left outer join apillai_test_db.mobile_attributes c
on(a.mobile_id  = cast(c.mobile_id as varchar) ) 
WHERE exclude_hit = '0' 
 AND year = '{year}'  
AND month =  '{month}'
  AND day =  '{day}' 
 and a.post_prop17 like '%rdc-responsive%'
AND post_channel not in ('core-ios','core-android','core-windows') 
GROUP BY 1,2,3,4,5 , 6, 7, 8, 9, 10, 11 ,12
 
) 
select *
from visit_aggregate
