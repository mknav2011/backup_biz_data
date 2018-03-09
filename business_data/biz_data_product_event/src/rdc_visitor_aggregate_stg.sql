SELECT cast (concat(year,'-', month,'-', day) as date) as date_mst
,a.post_visid_high || '-' || a.post_visid_low AS visitor_id
,a.post_visid_high || '-' || a.post_visid_low||'-'||a.visit_num as visit_id
,a.post_evar17 
,a.post_prop31 
,a.post_prop32 
,a.post_prop40 
,a.post_prop67 
,a.post_channel 
,a.post_pagename 
,a.va_closer_id 
,a.va_closer_detail 
,a.mobile_id 
,a.user_agent 
,a.os os
,SUM(CASE WHEN post_page_event = '0' THEN 1 ELSE 0 END) AS pv_count
FROM cnpd_omtr_pdt.hit_data_forqa a
WHERE a.exclude_hit = '0'
 AND year = '{year}'  
AND month =  '{month}'
  AND day =  '{day}'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
