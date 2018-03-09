SELECT cast (concat(year,'-', month,'-', day) as date) as date_mst
,a.post_visid_high || '-' || a.post_visid_low AS visitor_id
,a.post_visid_high || '-' || a.post_visid_low||'-'||a.visit_num as visit_id
,a.post_evar17 brand_experience
,a.post_prop31 ldp_property_status_detail
,a.post_prop32 srp_property_status_detail
,a.post_prop40 page_type
,a.post_prop67 web_app_version
,a.post_channel site_section
,a.post_pagename page_name_or_url
,a.post_mobileappid mobileappid
,a.post_mobileosversion os_version
,a.va_closer_id marketing_channel
,a.va_closer_detail marketing_channel_detail
,a.mobile_id mobile_id
,a.user_agent user_agent
,a.os os
,a.hit_source hit_source
,SUM(CASE WHEN post_page_event = '0' THEN 1 ELSE 0 END) AS pv_count
,SUM(CASE WHEN post_page_event = '100' THEN 1 ELSE 0 END) AS click_count
FROM cnpd_omtr_pdt.hit_data_forqa a
WHERE a.exclude_hit = '0'
 AND year = '2017'  
AND month =  '12'
  AND day =  '01'
group by
 cast (concat(year,'-', month,'-', day) as date)
,a.post_visid_high
,a.post_visid_low
,a.visit_num
,a.post_evar17
,a.post_prop31
,a.post_prop32
,a.post_prop40
,a.post_prop67
,a.post_channel
,a.post_pagename
,a.post_mobileappid
,a.post_mobileosversion
,a.va_closer_id
,a.va_closer_detail
,a.mobile_id
,a.user_agent
,a.os
,a.hit_source
limit 1000
