WITH rdc_biz_data_Feb11th2018 as
(
SELECT  post_cust_hit_datetime_mst as mst_datetime
,a.post_visid_high || '-' || a.post_visid_low AS visitor_id
,a.post_visid_high || '-' || a.post_visid_low||'-'||a.visit_num as visit_id
,a.visit_page_num visit_page_number
,lower(trim(a.post_evar17 )) experience
, (case
when lower(trim(post_evar17)) = 'rdc-responsive' AND mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes where lower(trim(operating_system)) = 'android' or lower(trim(operating_system)) = 'ios') then 'mobile web'
else
(case
when lower(trim(post_evar17))='rdc-responsive' then 'web'
else
(case
when
(mobile_id IN (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes
where lower(trim(operating_system)) = 'android' ) OR lower(trim(post_mobileosversion)) LIKE '%android%')
AND (lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%') AND lower(trim(post_evar17)) = 'rdc-android-instant' then 'android instant apps'
else
(case
when lower(trim(post_evar17)) IN ('rdc-mobile-core','rdc-mobile-rentals')  then 'mobile apps'
else 'others'
end)
end)
end)
END) experience_type
,lower(trim(a.post_prop31)) ldp_property_status_raw
,lower(trim(a.post_prop32)) srp_property_status_raw
 ,(case
 WHEN lower(trim(post_prop31)) IN ('ldp:for_sale', 'ldp:for-sale', 'ldp:for_sale:null', 'ldp:for_sale:foreclosure', 'ldp:new_community:new_plan', 'ldp:new_home_plan' ,'ldp:new-communities','ldp:new_community', 'ldp:for-sale:new-homes','ldp:new-home-communities','ldp:new-home-communities:new-homes', 'ldp:new_community:null','ldp:new-home-communities')
 OR lower(trim(post_prop32)) IN ('srp:for-sale:any', 'srp:for_sale', 'srp:for_sale:null', 'srp:for_sale:foreclosure','srp:for_sale:new_plan','srp:for-sale:new-homes', 'srp:new_community:null','srp:new_community','srp:for-sale:foreclosures','srp:new-home-communities','srp:new-home-communities:new-homes')
 THEN 'for sale'
 ELSE (Case
 WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:off_market', 'ldp:not_for_sale:recently_sold','ldp:not-for-sale','ldp:not_for_sale','ldp:recently_sold','ldp:not-for-sale:recently-sold','ldp:not_for_sale:foreclosure', 'ldp:just_taken_off_market')
 OR lower(trim(post_prop32)) IN ('srp:not_for_sale:recently_sold','srp:not-for-sale','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold','srp:not-for-sale:any','srp:off_market','srp:recently_sold','srp:not_for_sale:null')
 THEN 'not for sale'
 ELSE (Case
 WHEN lower(trim(post_prop31)) IN ('ldp:for_rent','ldp:for_rent:null','ldp:rental','ldp:rentals')
 OR lower(trim(post_prop32)) IN ('srp:rentals','srp:for_rent', 'srp:for_rent:null')
 THEN 'for rent'
 ELSE 'others'
 END)
 END)
 END) property_status_orig
 ,(Case
 WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:recently_sold','ldp:recently_sold','ldp:not-for-sale:recently-sold')
 OR lower(trim(post_prop32)) IN ( 'srp:not_for_sale:recently_sold','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold' ,'srp:recently_sold','srp:off_market:recently_sold')
 THEN 'recently sold'
 ELSE (Case
 WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:off_market','ldp:just_taken_off_market')
 OR lower(trim(post_prop32)) IN ( 'srp:off_market','srp:off_market:recently_sold','srp:off_market:foreclosure','srp:off_market:new_plan')
 THEN 'off market'
ELSE (case
 WHEN lower(trim(post_prop31)) =  'ldp:for_sale:foreclosure'
 OR lower(trim(post_prop32)) IN ( 'srp:for_sale:foreclosure','srp:for-sale:foreclosures')
then 'foreclosure'
ELSE (case
 WHEN lower(trim(post_prop31)) =  'ldp:new_community:new_plan'
 OR lower(trim(post_prop32)) = 'srp:for_sale:new_plan'
then 'new plan'
 ELSE 'not available'
END)
END)
 END)
 END) property_status_sub_orig
,lower(trim(post_evar117)) property_status_new_raw
,(case 
when length(lower(trim(post_evar117))) = 0  then null  
when lower(trim(post_evar117)) in ('for_sale','new_community','foreclosure','new_construction') then 'for sale'
when lower(trim(post_evar117)) = 'for_rent' then 'for rent'
when lower(trim(post_evar117)) = 'not_for_sale' then 'not for sale'
when lower(trim(post_evar117)) not in ('for_sale','new_community','foreclosure','new_construction','for_rent','not_for_sale') then 'others'
end) property_status_new
,lower(trim(post_evar118)) property_status_sub_new_raw
, (case
when length(lower(trim(post_evar117))) = 0 then null  
when lower(trim(post_evar117)) = 'not_for_sale' and lower(trim(post_evar118)) = 'off_market' then 'off market'
when lower(trim(post_evar117)) = 'not_for_sale' and  lower(trim(post_evar118)) = 'recently_sold' then 'recently sold'
when lower(trim(post_evar117)) in ('for_sale','new_community','foreclosure','new_construction') and lower(trim(post_evar118)) = 'foreclosure' then 'foreclosure'
when lower(trim(post_evar117)) in ('for_sale','new_community','foreclosure','new_construction') and  lower(trim(post_evar118)) = 'new_plan' then 'new plan'
when lower(trim(post_evar118)) not in ('off_market','recently_sold','foreclosure', 'new_plan') then  'not available'
end) property_status_sub_new
,(case when lower(trim(post_channel)) = 'homes' then 'buy sell'
when lower(trim(post_channel)) = 'rentals' then 'rentals'
when lower(trim(post_channel)) = 'realtors' then 'far'
when lower(trim(post_channel)) = 'mortgage' then 'mortgage'
when lower(trim(post_channel)) in ('news', 'advice') then 'news and insights'
when lower(trim(post_channel)) like 'marketing%' then 'marketing'
when lower(trim(post_channel)) like 'advertising%' then 'advertising'
when lower(trim(post_channel)) = 'local' then 'local'
else 'others'
end) site_section
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '253,%'
OR lower(trim(post_event_list)) LIKE '%,253,%'
OR lower(trim(post_event_list)) LIKE '%,253'
)
then 'y'
else 'n'
end
) social_shares
,(case when
mobile_id = '0' and lower(trim(post_channel)) not in ('core-ios','core-android','rentals-ios','rentals-android') then 'desktop' 
when mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes
where
lower(trim(device_type)) = 'tablet')      then 'tablet'
when mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes
where
lower(trim(device_type)) = 'mobile phone') then 'mobile phone'
else 'others'
end) device_type
,(Case
when
(
mobile_id IN (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes where lower(trim(operating_system)) = 'android')
OR lower(trim(post_mobileosversion)) LIKE '%android%'
)
AND
(
lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%'
)
AND lower(trim(post_evar17)) <> 'rdc-android-instant'  
then 'android core apps'
when
(mobile_id IN (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes
where lower(trim(operating_system)) = 'android' ) OR lower(trim(post_mobileosversion)) LIKE '%android%')
AND (lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%') AND lower(trim(post_evar17)) = 'rdc-android-instant' 
then 'android instant apps'
when
(mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes where lower(trim(operating_system)) = 'ios') OR lower(trim(post_mobileosversion)) LIKE '%ios%') AND (lower(trim(post_channel)) = 'core-ios' OR lower(trim(post_mobileappid)) LIKE 'realtor%' ) AND lower(trim(post_evar17)) = 'rdc-mobile-core'
then 'ios core apps'
when
(mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes where lower(trim(operating_system)) = 'android') OR lower(trim(post_mobileosversion)) LIKE '%android%')
AND ( lower(trim(post_channel)) = 'rentals-android' OR lower(trim(post_mobileappid)) LIKE 'rentals%')  AND lower(trim(post_evar17)) = 'rdc-mobile-rentals' 
then 'rentals android core apps'
when
(mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM biz_data_product_event.mobile_attributes where lower(trim(operating_system)) = 'ios') OR lower(trim(post_mobileosversion)) LIKE '%ios%')
AND ( lower(trim(post_channel)) = 'rentals-ios' OR lower(trim(post_mobileappid)) LIKE 'rentals%')  AND lower(trim(post_evar17)) = 'rdc-mobile-rentals' 
then 'rentals ios core apps'
Else 'others'
END) apps_type
, mobile_id
,lower(trim(a.post_prop40)) page_type
,(case when
lower(trim(a.post_prop40)) in ('ldp','ldp-quickview')
Then 'ldp'
Else
(case when lower(trim(a.post_prop40)) in ('srp-summary-card','srp-photo','srp-map','srp-list-map','srp-list','srp-list-photo'
,'srp','map','srp-listmap','summary-card','ldp-map','srp-fullmap')
then 'srp'
else
(case when lower(trim(a.post_prop40)) = 'home'
then 'home'
else 'others'
END)
END)
END) page_type_group
, post_prop37 search_city_and_state
,post_prop24  cid
, post_campaign campaign
,geo_city consumer_ip_city
,geo_region consumer_ip_region
,geo_zip consumer_ip_zip
,geo_dma consumer_ip_dma
,geo_country consumer_ip_country
,lower(trim(a.post_prop67)) web_app_version
,lower(trim(a.post_pagename)) page_name_or_url
,lower(trim(a.post_mobileosversion)) os_version
,(case when lower(trim(a.va_closer_id)) = '1' then 'move inc sites'
when lower(trim(a.va_closer_id)) = '2' then 'paid search'
when lower(trim(a.va_closer_id)) = '3' then 'organic search'
when lower(trim(a.va_closer_id)) = '4' then 'display'
when lower(trim(a.va_closer_id)) = '5' then 'email'
when lower(trim(a.va_closer_id)) = '6' then 'direct (typed/bookmarked)'
when lower(trim(a.va_closer_id)) = '7' then 'session refresh'
when lower(trim(a.va_closer_id)) = '8' then 'owned social'
when lower(trim(a.va_closer_id)) = '9' then 'referring domains'
when lower(trim(a.va_closer_id)) = '10' then 'push notifications'
when lower(trim(a.va_closer_id)) = '11' then 'affiliates'
when lower(trim(a.va_closer_id)) = '12' then 'other campaigns'
when lower(trim(a.va_closer_id)) = '13' then 'partner'
when lower(trim(a.va_closer_id)) = '14' then 'paid social'
when lower(trim(a.va_closer_id)) = '15' then 'public relations'
when lower(trim(a.va_closer_id)) = '16' then 'content syndication'
when lower(trim(a.va_closer_id)) = '17' then 'vanity url'
when lower(trim(a.va_closer_id)) = '18' then 'owned cross platform'  
when lower(trim(a.va_closer_id)) = '19' then 'rich media'
when lower(trim(a.va_closer_id)) = '20' then 'news corp sites'
else 'unknown'
END) marketing_channel
,(case when lower(trim(a.va_closer_id)) = '1' then 'others'
when lower(trim(a.va_closer_id)) = '2' then 'total paid'
when lower(trim(a.va_closer_id)) = '3' then 'organic search'
when lower(trim(a.va_closer_id)) = '4' then 'total paid'
when lower(trim(a.va_closer_id)) = '5' then 'email'
when lower(trim(a.va_closer_id)) = '6' then 'direct'
when lower(trim(a.va_closer_id)) = '7' then 'others'
when lower(trim(a.va_closer_id)) = '8' then 'owned social'
when lower(trim(a.va_closer_id)) = '9' then 'others'
when lower(trim(a.va_closer_id)) = '10' then 'others'
when lower(trim(a.va_closer_id)) = '11' then 'total paid'
when lower(trim(a.va_closer_id)) = '12' then 'total paid'
when lower(trim(a.va_closer_id)) = '13' then 'others'
when lower(trim(a.va_closer_id)) = '14' then 'total paid'
when lower(trim(a.va_closer_id)) = '15' then 'others'
when lower(trim(a.va_closer_id)) = '16' then 'total paid'
when lower(trim(a.va_closer_id)) = '17' then 'others'
when lower(trim(a.va_closer_id)) = '18' then 'others'
when lower(trim(a.va_closer_id)) = '19' then 'total paid'
when lower(trim(a.va_closer_id)) = '20' then 'others'
else 'unknown'
END) newscorp_view
,(case when lower(trim(a.va_closer_id)) = '1' then 'others'
when lower(trim(a.va_closer_id)) = '2' then 'total paid'
when lower(trim(a.va_closer_id)) = '3' then 'organic search'
when lower(trim(a.va_closer_id)) = '4' then 'total paid'
when lower(trim(a.va_closer_id)) = '5' then 'email'
when lower(trim(a.va_closer_id)) = '6' then 'direct'
when lower(trim(a.va_closer_id)) = '7' then 'others'
when lower(trim(a.va_closer_id)) = '8' then 'owned social'
when lower(trim(a.va_closer_id)) = '9' then 'others'
when lower(trim(a.va_closer_id)) = '10' then 'others'
when lower(trim(a.va_closer_id)) = '11' then 'total paid'
when lower(trim(a.va_closer_id)) = '12' then 'total paid'
when lower(trim(a.va_closer_id)) = '13' then 'others'
when lower(trim(a.va_closer_id)) = '14' then 'others'
when lower(trim(a.va_closer_id)) = '15' then 'others'
when lower(trim(a.va_closer_id)) = '16' then 'total paid'
when lower(trim(a.va_closer_id)) = '17' then 'others'
when lower(trim(a.va_closer_id)) = '18' then 'others'
when lower(trim(a.va_closer_id)) = '19' then 'total paid'
when lower(trim(a.va_closer_id)) = '20' then 'others'
else 'unknown'
END) kpi_channel_view
,(case when lower(trim(a.va_closer_id)) = '1' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '2' then 'total paid'
when lower(trim(a.va_closer_id)) = '3' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '4' then 'total paid'
when lower(trim(a.va_closer_id)) = '5' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '6' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '7' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '8' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '9' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '10' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '11' then 'total paid'
when lower(trim(a.va_closer_id)) = '12' then 'total paid'
when lower(trim(a.va_closer_id)) = '13' then 'total paid'
when lower(trim(a.va_closer_id)) = '14' then 'total paid'
when lower(trim(a.va_closer_id)) = '15' then 'others'
when lower(trim(a.va_closer_id)) = '16' then 'total paid'
when lower(trim(a.va_closer_id)) = '17' then 'others'
when lower(trim(a.va_closer_id)) = '18' then 'total non-paid'
when lower(trim(a.va_closer_id)) = '19' then 'total paid'
when lower(trim(a.va_closer_id)) = '20' then 'total non-paid'
else 'unknown'
END) marketing_channel_group
,(case when lower(trim(a.va_closer_id)) = '1' then 'organic'
when lower(trim(a.va_closer_id)) = '2' then 'paid'
when lower(trim(a.va_closer_id)) = '3' then 'organic'
when lower(trim(a.va_closer_id)) = '4' then 'paid'
when lower(trim(a.va_closer_id)) = '5' then 'organic'
when lower(trim(a.va_closer_id)) = '6' then 'organic'
when lower(trim(a.va_closer_id)) = '7' then 'organic'
when lower(trim(a.va_closer_id)) = '8' then 'organic'
when lower(trim(a.va_closer_id)) = '9' then 'organic'
when lower(trim(a.va_closer_id)) = '10' then 'organic'
when lower(trim(a.va_closer_id)) = '11' then 'paid'
when lower(trim(a.va_closer_id)) = '12' then 'paid'
when lower(trim(a.va_closer_id)) = '13' then 'others'
when lower(trim(a.va_closer_id)) = '14' then 'paid'
when lower(trim(a.va_closer_id)) = '15' then 'others'
when lower(trim(a.va_closer_id)) = '16' then 'paid'
when lower(trim(a.va_closer_id)) = '17' then 'organic'
when lower(trim(a.va_closer_id)) = '18' then 'organic'
when lower(trim(a.va_closer_id)) = '19' then 'paid'
when lower(trim(a.va_closer_id)) = '20' then 'organic'
else 'unknown'
END) paid_vs_organic
,lower(trim(a.va_closer_detail)) marketing_channel_raw
,lower(trim(a.post_evar12)) neighborhood
,lower(trim(a.post_evar3)) city
,lower(trim(a.post_evar33)) product_type_raw
,'placeholder_for_upcoming_lookup_value' as product_type
,lower(trim(a.post_evar4)) state
,lower(trim(a.post_evar5)) zip
,lower(trim(a.post_evar81)) advertiser_id_raw
, regexp_extract(lower(trim(a.post_evar81)), '[a].(\d+)', 1) as agent_advertiser_id
, regexp_extract(lower(trim(a.post_evar81)), '[b].(\d+)', 1) as broker_advertiser_id
, regexp_extract(lower(trim(a.post_evar81)), '[o].(\d+)', 1) as office_advertiser_id
,lower(trim(a.post_evar87)) persist_listing_id
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '186,%'
OR lower(trim(post_event_list)) LIKE '%,186,%'
OR lower(trim(post_event_list)) LIKE '%,186'
)
then lower(trim(a.post_evar87))
else null
end
) listing_id
,lower(trim(a.post_evar88)) persist_mpr_id
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '187,%'
OR lower(trim(post_event_list)) LIKE '%,187,%'
OR lower(trim(post_event_list)) LIKE '%,187'
)
then lower(trim(a.post_evar88))
else null
end
) mpr_id
,lower(trim(a.post_event_list)) event_list
,lower(trim(a.post_evar24)) registered_user_activity
,lower(trim(a.post_evar68)) member_id
,lower(trim(a.ref_domain)) ref_domain
,lower(trim(a.va_finder_id)) first_touch_marketing_channel_raw
,(case when lower(trim(a.va_finder_id)) = '1' then 'move inc sites'
when lower(trim(a.va_finder_id)) = '2' then 'paid search'
when lower(trim(a.va_finder_id)) = '3' then 'organic search'
when lower(trim(a.va_finder_id)) = '4' then 'display'
when lower(trim(a.va_finder_id)) = '5' then 'email'
when lower(trim(a.va_finder_id)) = '6' then 'direct (typed/bookmarked)'
when lower(trim(a.va_finder_id)) = '7' then 'session refresh'
when lower(trim(a.va_finder_id)) = '8' then 'owned social'
when lower(trim(a.va_finder_id)) = '9' then 'referring domains'
when lower(trim(a.va_finder_id)) = '10' then 'push notifications'
when lower(trim(a.va_finder_id)) = '11' then 'affiliates'
when lower(trim(a.va_finder_id)) = '12' then 'other campaigns'
when lower(trim(a.va_finder_id)) = '13' then 'partner'
when lower(trim(a.va_finder_id)) = '14' then 'paid social'
when lower(trim(a.va_finder_id)) = '15' then 'public relations'
when lower(trim(a.va_finder_id)) = '16' then 'content syndication'
when lower(trim(a.va_closer_id)) = '17' then 'vanity url'
when lower(trim(a.va_closer_id)) = '18' then 'owned cross platform' 
when lower(trim(a.va_closer_id)) = '19' then 'rich media'
when lower(trim(a.va_closer_id)) = '20' then 'news corp sites'
else 'unknown'
END) first_touch_marketing_channel
,a.post_prop70 rdc_visitor_id_raw
,lower(trim(a.post_prop70)) rdc_visitor_id
,lower(trim(a.post_evar112)) persist_rdc_visitor_id
,lower(trim(a.post_page_event)) post_page_event
,lower(trim(a.post_referrer)) referring_page_url
,lower(trim(a.post_page_url)) current_page_url
,lower(trim(a.user_agent)) user_agent
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '239,%'
OR lower(trim(post_event_list)) LIKE '%,239,%'
OR lower(trim(post_event_list)) LIKE '%,239'
)
then 'y'
else 'n'
end
) sign_up
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '240,%'
OR lower(trim(post_event_list)) LIKE '%,240,%'
OR lower(trim(post_event_list)) LIKE '%,240'
)
then 'y'
else 'n'
end
) sign_in
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '241,%'
OR lower(trim(post_event_list)) LIKE '%,241,%'
OR lower(trim(post_event_list)) LIKE '%,241'
)
then 'y'
else 'n'
end
) sign_out
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '251,%'
OR lower(trim(post_event_list)) LIKE '%,251,%'
OR lower(trim(post_event_list)) LIKE '%,251'
)
then 'y'
else 'n'
end
) saved_items
, lower(trim(post_evar52)) saved_items_raw
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '221,%'
OR lower(trim(post_event_list)) LIKE '%,221,%'
OR lower(trim(post_event_list)) LIKE '%,221'
)
then 'y'
else 'n'
end
) advantage_leads
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '224,%'
OR lower(trim(post_event_list)) LIKE '%,224,%'
OR lower(trim(post_event_list)) LIKE '%,224'
)
then 'y'
else 'n'
end
) far_leads
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '225,%'
OR lower(trim(post_event_list)) LIKE '%,225,%'
OR lower(trim(post_event_list)) LIKE '%,225'
)
then 'y'
else 'n'
end
) not_for_sale_leads
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '226,%'
OR lower(trim(post_event_list)) LIKE '%,226,%'
OR lower(trim(post_event_list)) LIKE '%,226'
)
then 'y'
else 'n'
end
) rcm_enabled_leads
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '227,%'
OR lower(trim(post_event_list)) LIKE '%,227,%'
OR lower(trim(post_event_list)) LIKE '%,227'
)
then 'y'
else 'n'
end
) cobroke_leads
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '228,%'
OR lower(trim(post_event_list)) LIKE '%,228,%'
OR lower(trim(post_event_list)) LIKE '%,228'
)
then 'y'
else 'n'
end
) advantage_choice_leads
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '230,%'
OR lower(trim(post_event_list)) LIKE '%,230,%'
OR lower(trim(post_event_list)) LIKE '%,230'
)
then 'y'
else 'n'
end
) turbo_leads
, lower(trim(post_evar42)) mal_lead_submission_raw
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '20110,%'
OR lower(trim(post_event_list)) LIKE '%,20110,%'
OR lower(trim(post_event_list)) LIKE '%,20110'
 )
then 'y'
else 'n'
end
) mal_lead_submission
,lower(trim(post_evar130)) lead_placement
, post_evar35 user_search_query
, a.browser browser_id
, lower(trim(b.name)) browser_name
, lower(trim(c.name)) browser_type
,post_evar65 lead_guid
, (case when lower(trim(post_evar111)) = 'true' then 'y' else 'n' end) login_status
, (case when lower(trim(post_evar100)) = 'true' then 'y' else 'n' end) basecamp
, lower(trim(post_evar91)) srp_columns
, lower(trim(post_prop10)) click_activity
, lower(trim(post_evar10)) persist_click_activity
, lower(trim(post_evar134)) language
, lower(trim(post_evar92)) real_tip_count
,lower(trim( post_channel)) post_channel 
,etl_ztg_id, etl_source_filename
FROM cnpd_omtr_pdt.hit_data a
left join biz_data_product_event.browser b on cast(b.id as varchar) = a.browser
left join biz_data_product_event.browser_type c on cast(c.id as varchar) = a.browser
WHERE a.exclude_hit = '0' and a.hit_source in ('1','2')
AND 1 = 1
AND year = '{year}'
AND month = '{month}'
AND day = '{day}'
)
SELECT  mst_datetime
,visitor_id
,visit_id
,visit_page_number
,experience
,experience_type
,ldp_property_status_raw
,srp_property_status_raw
,property_status_orig
,property_status_sub_orig
,property_status_new_raw
,property_status_new
,property_status_sub_new_raw
,property_status_sub_new
,(case when property_status_new is null then property_status_orig else property_status_new end) property_status
,(case when property_status_sub_new is null then property_status_sub_orig else property_status_sub_new end) property_status_sub 
,site_section
,social_shares
,device_type
,apps_type
, mobile_id
,page_type
,page_type_group
, search_city_and_state
,cid
,campaign
,consumer_ip_city
,consumer_ip_region
,consumer_ip_zip
,consumer_ip_dma
,consumer_ip_country
,web_app_version
,page_name_or_url
,os_version
,marketing_channel
,newscorp_view
,kpi_channel_view
,marketing_channel_group
,paid_vs_organic
,marketing_channel_raw
,neighborhood
, city
,product_type_raw
,product_type
,state
,zip
, advertiser_id_raw
,case when agent_advertiser_id is null
and lower(trim(post_channel)) = 'realtors'
and advertiser_id_raw <> 'null'
and REGEXP_LIKE(trim(advertiser_id_raw), '^[[:digit:]]+$')
then advertiser_id_raw
else agent_advertiser_id
end as agent_advertiser_id
,broker_advertiser_id
,office_advertiser_id
,persist_listing_id
,listing_id
,persist_mpr_id
,mpr_id
,event_list
,registered_user_activity
, member_id
,ref_domain
, first_touch_marketing_channel_raw
,first_touch_marketing_channel
,rdc_visitor_id_raw
, rdc_visitor_id
,persist_rdc_visitor_id
,post_page_event
,referring_page_url
,current_page_url
,user_agent
,sign_up
,sign_in
,sign_out
,saved_items
,saved_items_raw
,advantage_leads
,far_leads
,not_for_sale_leads
,rcm_enabled_leads
,cobroke_leads
,advantage_choice_leads
,turbo_leads
,mal_lead_submission_raw
,mal_lead_submission
,lead_placement
,user_search_query
,browser_id
,browser_name
,browser_type
, lead_guid
,login_status
,basecamp
,srp_columns
,click_activity
,persist_click_activity
, language
, real_tip_count
, year (mst_datetime) as year
, lpad(cast (month (mst_datetime) as varchar), 2, '0') as month
, lpad(cast (day (mst_datetime) as varchar), 2, '0') as day
, lpad(cast (hour (mst_datetime) as varchar), 2, '0') as hour
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss') as etl_created_datetime , 'glue_etl' as etl_created_by
,etl_ztg_id
from rdc_biz_data_Feb11th2018
limit 100000
