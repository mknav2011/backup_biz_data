SELECT cast (concat(year,'-', month,'-', day) AS date) AS mst_date
,a.post_visid_high || '-' || a.post_visid_low AS visitor_id
,a.post_visid_high || '-' || a.post_visid_low||'-'||a.visit_num as visit_id
,lower(trim(a.post_evar17 )) brand_experience
, (case when
lower(trim(post_evar17))='rdc-responsive'
Then 'Web'
Else
(Case when
lower(trim(post_evar17))='rdc-mobile-core'
Then 'Mobile App'
Else 'Others'
END)
END) brand_experience_type
,lower(trim(a.post_prop31)) ldp_property_status_detail
,(case
WHEN lower(trim(post_prop31)) IN ('ldp:for_sale', 'ldp:for-sale', 'ldp:for_sale:null', 'ldp:for_sale:foreclosure', 'ldp:new_community:new_plan', 'ldp:new_home_plan' ,'ldp:new-communities','ldp:new_community', 'ldp:for-sale:new-homes','ldp:new-home-communities','ldp:new-home-communities:new-homes', 'ldp:new_community:null','ldp:new-home-communities')
OR lower(trim(post_prop32)) IN ('srp:for-sale:any', 'srp:for_sale', 'srp:for_sale:null', 'srp:for_sale:foreclosure','srp:for_sale:new_plan','srp:for-sale:new-homes', 'srp:new_community:null','srp:new_community','srp:for-sale:foreclosures','srp:new-home-communities','srp:new-home-communities:new-homes')
THEN 'For Sale'
ELSE (Case
WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:off_market', 'ldp:not_for_sale:recently_sold','ldp:not-for-sale','ldp:not_for_sale','ldp:recently_sold','ldp:not-for-sale:recently-sold','ldp:not_for_sale:foreclosure', 'ldp:just_taken_off_market')
OR lower(trim(post_prop32)) IN ('srp:not_for_sale:recently_sold','srp:not-for-sale','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold','srp:not-for-sale:any','srp:off_market','srp:recently_sold','srp:not_for_sale:null')
THEN 'Not For Sale'
ELSE (Case
WHEN lower(trim(post_prop31)) IN ('ldp:for_rent','ldp:for_rent:null','ldp:rental','ldp:rentals')
OR lower(trim(post_prop32)) IN ('srp:rentals','srp:for_rent', 'srp:for_rent:null')
THEN 'For Rent'
ELSE 'Others'
END)
END)
END) site_section
,(Case
WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:recently_sold','ldp:recently_sold','ldp:not-for-sale:recently-sold')
OR lower(trim(post_prop32)) IN ( 'srp:not_for_sale:recently_sold','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold' ,'srp:recently_sold','srp:off_market:recently_sold')
THEN 'Recently Sold'
ELSE (Case
WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:off_market','ldp:just_taken_off_market')
OR lower(trim(post_prop32)) IN ( 'srp:off_market','srp:off_market:recently_sold','srp:off_market:foreclosure','srp:off_market:new_plan')
THEN 'Off Market'
ELSE 'Not Applicable'
END)
END) sub_site_section
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '253,%'
OR lower(trim(post_event_list)) LIKE '%,253,%'
OR lower(trim(post_event_list)) LIKE '%,253'
)
AND lower(trim(post_evar17)) = 'rdc-responsive'
then 'Y'
else 'N'
end
) socialshares
,(Case when
lower(trim(post_evar17)) = 'rdc-responsive'  and OS in (SELECT cast(ID as varchar) as ID FROM cnpd_omtr_pdt.operating_systems where lower(trim(name)) like 'android%' or lower(trim(name)) like 'ios%' )
Then 'Mobile Web'
End) mobile_web_status
,(Case when
(OS IN (SELECT cast(ID as varchar) as ID FROM cnpd_omtr_pdt.operating_systems WHERE lower(trim(name)) LIKE 'android%' OR lower(trim(name)) LIKE 'linux%' )  OR lower(trim(post_mobileosversion)) LIKE '%android%') AND (lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%')  AND lower(trim(post_evar17)) <> 'rdc-android-instant'
Then 'Android Core Apps'
when
(OS IN (SELECT cast(ID as varchar) as ID FROM cnpd_omtr_pdt.operating_systems WHERE lower(trim(name)) LIKE 'android%' OR lower(trim(name)) LIKE 'linux%' ) OR lower(trim(post_mobileosversion)) LIKE '%android%') AND (lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%') AND lower(trim(post_evar17)) = 'rdc-android-instant'
Then 'Android Instant Apps'
when
(OS IN (SELECT cast(ID as varchar) as ID FROM cnpd_omtr_pdt.operating_systems WHERE lower(trim(name)) LIKE 'ios%') OR lower(trim(post_mobileosversion)) LIKE '%ios%')
AND ( lower(trim(post_channel)) = 'core-ios' OR lower(trim(post_mobileappid)) LIKE 'realtor%' )
Then 'iOS Core Apps'
Else 'Others'
END) apps_type
,lower(trim(a.post_prop32)) srp_property_status_detail
,lower(trim(a.post_prop40)) page_type
,(case when
lower(trim(a.post_prop40)) in ('ldp','ldp-quickview','ldp-seller-summary')
Then 'LDP'
Else
(case when lower(trim(a.post_prop40)) in ('srp-summary-card','srp-photo','srp-map','srp-list-map','srp-list','srp-list-photo'
,'srp','map','srp-listmap','summary-card','ldp-map')
then 'SRP'
else
(case when lower(trim(a.post_prop40)) = 'home'
then 'HOME'
else 'Others'
END)
END)
END) page_type_group
,lower(trim(a.post_prop67)) web_app_version

,lower(trim(a.post_pagename)) page_name_or_url

,lower(trim(a.post_mobileosversion)) os_version

,(case when lower(trim(a.va_closer_id)) = '1' then 'Move Inc Sites'
when lower(trim(a.va_closer_id)) = '2' then 'Paid Search'
when lower(trim(a.va_closer_id)) = '3' then 'Organic Search'
when lower(trim(a.va_closer_id)) = '4' then 'Display'
when lower(trim(a.va_closer_id)) = '5' then 'Email'
when lower(trim(a.va_closer_id)) = '6' then 'Direct (Typed/Bookmarked)'
when lower(trim(a.va_closer_id)) = '7' then 'Session Refresh'
when lower(trim(a.va_closer_id)) = '8' then 'Owned Social'
when lower(trim(a.va_closer_id)) = '9' then 'Referring Domains'
when lower(trim(a.va_closer_id)) = '10' then 'Push Notifications'
when lower(trim(a.va_closer_id)) = '11' then 'Affiliates'
when lower(trim(a.va_closer_id)) = '12' then 'Other Campaigns'
when lower(trim(a.va_closer_id)) = '13' then 'Partner'
when lower(trim(a.va_closer_id)) = '14' then 'Paid Social'
when lower(trim(a.va_closer_id)) = '15' then 'Public Relations'
when lower(trim(a.va_closer_id)) = '16' then 'Content Syndication'
when lower(trim(a.va_closer_id)) = '17' then 'Vanity URL'
when lower(trim(a.va_closer_id)) = '18' then 'Owned Cross Platform'
when lower(trim(a.va_closer_id)) = '19' then 'Rich Media'
when lower(trim(a.va_closer_id)) = '20' then 'News Corp Sites'
else 'UNKNOWN'
END) marketing_channel
,(case when lower(trim(a.va_closer_id)) = '1' then 'Others'
when lower(trim(a.va_closer_id)) = '2' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '3' then 'Organic Search'
when lower(trim(a.va_closer_id)) = '4' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '5' then 'Email'
when lower(trim(a.va_closer_id)) = '6' then 'Direct'
when lower(trim(a.va_closer_id)) = '7' then 'Others'
when lower(trim(a.va_closer_id)) = '8' then 'Owned Social'
when lower(trim(a.va_closer_id)) = '9' then 'Others'
when lower(trim(a.va_closer_id)) = '10' then 'Others'
when lower(trim(a.va_closer_id)) = '11' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '12' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '13' then 'Others'
when lower(trim(a.va_closer_id)) = '14' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '15' then 'Others'
when lower(trim(a.va_closer_id)) = '16' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '17' then 'Others'
when lower(trim(a.va_closer_id)) = '18' then 'Others'
when lower(trim(a.va_closer_id)) = '19' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '20' then 'Others'
else 'UNKNOWN'
END) newscorp_view
,(case when lower(trim(a.va_closer_id)) = '1' then 'Others'
when lower(trim(a.va_closer_id)) = '2' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '3' then 'Organic Search'
when lower(trim(a.va_closer_id)) = '4' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '5' then 'Email'
when lower(trim(a.va_closer_id)) = '6' then 'Direct'
when lower(trim(a.va_closer_id)) = '7' then 'Others'
when lower(trim(a.va_closer_id)) = '8' then 'Owned Social'
when lower(trim(a.va_closer_id)) = '9' then 'Others'
when lower(trim(a.va_closer_id)) = '10' then 'Others'
when lower(trim(a.va_closer_id)) = '11' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '12' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '13' then 'Others'
when lower(trim(a.va_closer_id)) = '14' then 'Others'
when lower(trim(a.va_closer_id)) = '15' then 'Others'
when lower(trim(a.va_closer_id)) = '16' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '17' then 'Others'
when lower(trim(a.va_closer_id)) = '18' then 'Others'
when lower(trim(a.va_closer_id)) = '19' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '20' then 'Others'
else 'UNKNOWN'
END) kpi_channel_view
,(case when lower(trim(a.va_closer_id)) = '1' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '2' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '3' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '4' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '5' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '6' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '7' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '8' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '9' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '10' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '11' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '12' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '13' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '14' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '15' then 'Others'
when lower(trim(a.va_closer_id)) = '16' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '17' then 'Others'
when lower(trim(a.va_closer_id)) = '18' then 'Total Non-Paid'
when lower(trim(a.va_closer_id)) = '19' then 'Total Paid'
when lower(trim(a.va_closer_id)) = '20' then 'Total Non-Paid'
else 'UNKNOWN'
END) marketing_channel_group
,(case when lower(trim(a.va_closer_id)) = '1' then 'Organic'
when lower(trim(a.va_closer_id)) = '2' then 'Paid'
when lower(trim(a.va_closer_id)) = '3' then 'Organic'
when lower(trim(a.va_closer_id)) = '4' then 'Paid'
when lower(trim(a.va_closer_id)) = '5' then 'Organic'
when lower(trim(a.va_closer_id)) = '6' then 'Organic'
when lower(trim(a.va_closer_id)) = '7' then 'Organic'
when lower(trim(a.va_closer_id)) = '8' then 'Organic'
when lower(trim(a.va_closer_id)) = '9' then 'Organic'
when lower(trim(a.va_closer_id)) = '10' then 'Organic'
when lower(trim(a.va_closer_id)) = '11' then 'Paid'
when lower(trim(a.va_closer_id)) = '12' then 'Paid'
when lower(trim(a.va_closer_id)) = '13' then 'Others'
when lower(trim(a.va_closer_id)) = '14' then 'Paid'
when lower(trim(a.va_closer_id)) = '15' then 'Others'
when lower(trim(a.va_closer_id)) = '16' then 'Paid'
when lower(trim(a.va_closer_id)) = '17' then 'Organic'
when lower(trim(a.va_closer_id)) = '18' then 'Organic'
when lower(trim(a.va_closer_id)) = '19' then 'Paid'
when lower(trim(a.va_closer_id)) = '20' then 'Organic'
else 'UNKNOWN'
END) paid_vs_organic
,lower(trim(a.va_closer_detail)) marketing_channel_detail
,lower(trim(a.post_evar12)) neighborhood
,lower(trim(a.post_evar3)) city
,lower(trim(a.post_evar33)) product_type
,lower(trim(a.post_evar4)) state
,lower(trim(a.post_evar5)) zip
,lower(trim(a.post_evar81)) advertiser_id
,lower(trim(a.post_evar87)) listing_id
,lower(trim(a.post_evar88)) mpr_id
,lower(trim(a.post_event_list)) event_list
,lower(trim(a.post_prop26)) registered_user_activity
,lower(trim(a.post_prop68)) member_id
,lower(trim(a.ref_domain)) ref_domain
,lower(trim(a.visit_page_num)) visit_page_number
,lower(trim(a.user_agent)) user_agent
,lower(trim(a.os)) os
,lower(trim(a.post_prop70)) dap_visitor_id
,lower(trim(a.post_page_event)) post_page_event
FROM cnpd_omtr_pdt.hit_data a
WHERE a.exclude_hit = '0' and a.hit_source in ('1','2')
AND 1 = 1
AND year = '{year}'
AND month = '{month}'
AND day = '{day}'
and hour = '{hour}' 
