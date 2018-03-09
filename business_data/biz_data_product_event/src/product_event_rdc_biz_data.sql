SELECT  post_cust_hit_datetime_mst as mst_datetime
,a.post_visid_high || '-' || a.post_visid_low AS visitor_id
,a.post_visid_high || '-' || a.post_visid_low||'-'||a.visit_num as visit_id
,lower(trim(a.post_evar17 )) experience
, (case
when lower(trim(post_evar17)) = 'rdc-responsive' AND mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes where lower(trim(operating_system)) = 'android' or lower(trim(operating_system)) = 'ios') then 'mobile web'
else
(case
when lower(trim(post_evar17))='rdc-responsive' then 'web'
else
(case
when
(mobile_id IN (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes
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
,lower(trim(a.post_prop31)) ldp_property_status_detail
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
END) property_status
,(Case
WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:recently_sold','ldp:recently_sold','ldp:not-for-sale:recently-sold')
OR lower(trim(post_prop32)) IN ( 'srp:not_for_sale:recently_sold','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold' ,'srp:recently_sold','srp:off_market:recently_sold')
THEN 'recently sold'
ELSE (Case
WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:off_market','ldp:just_taken_off_market')
OR lower(trim(post_prop32)) IN ( 'srp:off_market','srp:off_market:recently_sold','srp:off_market:foreclosure','srp:off_market:new_plan')
THEN 'off market'
ELSE 'not applicable'
END)
END) sub_property_status 
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
then 'y'
else 'n'
end
) social_shares
,(case when
mobile_id = '0' and lower(trim(post_channel)) not in ('core-ios','core-android','rentals-ios','rentals-android') then 'desktop' 
when mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes
where
lower(trim(device_type)) = 'tablet')      then 'tablet'
when mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes
where
lower(trim(device_type)) = 'mobile phone') then 'mobile phone'
else 'others'
end) device_type
,(Case
when
(
mobile_id IN (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes where lower(trim(operating_system)) = 'android')
OR lower(trim(post_mobileosversion)) LIKE '%android%'
)
AND
(
lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%'
)
AND lower(trim(post_evar17)) <> 'rdc-android-instant'  
then 'android core apps'
when
(mobile_id IN (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes
where lower(trim(operating_system)) = 'android' ) OR lower(trim(post_mobileosversion)) LIKE '%android%')
AND (lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%') AND lower(trim(post_evar17)) = 'rdc-android-instant' 
then 'android instant apps'
when
(mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes where lower(trim(operating_system)) = 'ios') OR lower(trim(post_mobileosversion)) LIKE '%ios%') AND (lower(trim(post_channel)) = 'core-ios' OR lower(trim(post_mobileappid)) LIKE 'realtor%' ) AND lower(trim(post_evar17)) = 'rdc-mobile-core'
then 'ios core apps'
when
(mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes where lower(trim(operating_system)) = 'android') OR lower(trim(post_mobileosversion)) LIKE '%android%')
AND ( lower(trim(post_channel)) = 'rentals-android' OR lower(trim(post_mobileappid)) LIKE 'rentals%')  AND lower(trim(post_evar17)) = 'rdc-mobile-rentals' 
then 'rentals android core apps'
when
(mobile_id in (SELECT cast(mobile_ID as varchar) as ID FROM product_event.mobile_attributes where lower(trim(operating_system)) = 'ios') OR lower(trim(post_mobileosversion)) LIKE '%ios%')
AND ( lower(trim(post_channel)) = 'rentals-ios' OR lower(trim(post_mobileappid)) LIKE 'rentals%')  AND lower(trim(post_evar17)) = 'rdc-mobile-rentals' 
then 'rentals ios core apps'
Else 'others'
END) apps_type
, mobile_id
,lower(trim(a.post_prop32)) srp_property_status_detail
,lower(trim(a.post_prop40)) page_type
,(case when
lower(trim(a.post_prop40)) in ('ldp','ldp-quickview','ldp-seller-summary')
Then 'ldp'
Else
(case when lower(trim(a.post_prop40)) in ('srp-summary-card','srp-photo','srp-map','srp-list-map','srp-list','srp-list-photo'
,'srp','map','srp-listmap','summary-card','ldp-map')
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
,lower(trim(a.va_finder_id)) first_touch_marketing_channel_detail
,(case when lower(trim(a.va_finder_id)) = '1' then 'move inc sites'
when lower(trim(a.va_finder_id)) = '2' then 'paid search'
when lower(trim(a.va_finder_id)) = '3' then 'organic search'
when lower(trim(a.va_finder_id)) = '4' then 'display'
when lower(trim(a.va_finder_id)) = '5' then 'email'
when lower(trim(a.va_finder_id)) = '6' then 'direct (typed/bookmarked)'
when lower(trim(a.va_finder_id)) = '7' then 'session refresh'
when lower(trim(a.va_finder_id)) = '8' then 'qwned social'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
then 'y'
else 'n'
end
) saved_items
, lower(trim(post_evar52)) saved_items_detail
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '221,%'
OR lower(trim(post_event_list)) LIKE '%,221,%'
OR lower(trim(post_event_list)) LIKE '%,221'
)
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
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
AND lower(trim(post_evar17)) = 'rdc-responsive'
then 'y'
else 'n'
end
) turbo_leads
,(Case when
lower(trim(post_page_event)) IN ('100', '0')
AND (lower(trim(post_event_list )) LIKE '20110,%'
OR lower(trim(post_event_list)) LIKE '%,20110,%'
OR lower(trim(post_event_list)) LIKE '%,20110'
)
AND lower(trim(post_evar17)) = 'rdc-responsive'
then 'y'
else 'n'
end
) mal_lead_submission
, post_evar35 user_search_query
, post_evar52 saved_items_type
, a.browser browser_id
, b.name browser_name
, c.name browser_type
,post_evar65 lead_guid
, (case when lower(trim(post_evar111)) = 'true' then 'y' else 'n' end) login_status
, (case when lower(trim(post_evar100)) = 'true' then 'y' else 'n' end) basecamp
, lower(trim(post_evar91)) srp_columns
, lower(trim(post_prop10)) click_activity
, lower(trim(post_evar10)) persist_click_activity
, lower(trim(post_evar134)) language
, lower(trim(post_evar92)) real_tip_count
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss')   as etl_created_datetime , 'glue_etl' as etl_created_by
,etl_ztg_id, etl_source_filename
FROM cnpd_omtr_pdt.hit_data a
left join product_event.browser b on cast(b.id as varchar) = a.browser
left join product_event.browser_type c on cast(c.id as varchar) = a.browser
WHERE a.exclude_hit = '0' and a.hit_source in ('1','2')
AND 1 = 1
AND year = '{year}'
AND month = '{month}'
AND day = '{day}'
limit 1000
