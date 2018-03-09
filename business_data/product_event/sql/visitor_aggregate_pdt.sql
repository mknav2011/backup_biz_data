WITH dataset as(
SELECT cast(post_cust_hit_datetime_mst as date)  as date_mst, 
post_visid_high || '-' || post_visid_low AS visitor_id, 
post_visid_high || '-' || post_visid_low||'-'||visit_num as visit_id,
post_pagename AS page,
post_prop40 as page_type,
va_closer_id   ,
va_closer_detail,
b.name as traffic_source,
a.mobile_id, c.manufacturer, c.device_type, c.operating_system, 
post_channel, 
SUM(CASE WHEN post_page_event = '0' THEN 1 ELSE 0 END) AS hit_cnt 
FROM pdt_master.pdt_hit_data a 
left outer join   adobe_parity.va_closer b
on(a.va_closer_id  = cast(b.id as varchar) )
left outer join adobe_parity.mobile_attributes c
on(a.mobile_id  = cast(c.mobile_id as varchar) ) 
WHERE exclude_hit = '0' 
AND hit_source not in ('5', '7', '8', '9')
AND post_channel not in ('core-ios','core-android','core-windows')
AND post_mobileappid = ''
 
 AND year = '{year}'  
AND month =  '{month}'
  AND day =  '{day}' 
GROUP BY 1,2,3,4,5 , 6, 7, 8, 9, 10, 11, 12,13
ORDER BY hit_cnt desc
)
select  *
from dataset 
