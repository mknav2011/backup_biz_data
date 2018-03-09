SELECT cast (concat(year,'-', month,'-', day) as date) as date_mst
	,a.post_visid_high || '-' || a.post_visid_low AS visitor_id
	,a.post_visid_high || '-' || a.post_visid_low||'-'||a.visit_num as visit_id
,lower(trim(a.post_evar17 )) brand_experience
, (case when
lower(trim(post_evar17))='rdc-responsive'
Then 'RDC & Mobile Web Brand Experience'
Else 
(Case when 
lower(trim(post_evar17))='rdc-mobile-core'
Then 'Mobile App Brand Experience'
Else 'Others'
END)
END) brand_experience_type
	,lower(trim(a.post_prop31)) ldp_property_status_detail
 ,(case
        WHEN lower(trim(post_prop31)) IN ('ldp:for_sale', 'ldp:for-sale', 'ldp:for_sale:null', 'ldp:for_sale:foreclosure', 'ldp:new_community:new_plan', 'ldp:new_home_plan' ,'ldp:new-communities','ldp:new_community', 'ldp:for-sale:new-homes','ldp:new-home-communities','ldp:new-home-communities:new-homes', 'ldp:new_community:null','ldp:new-home-communities')
            OR lower(trim(post_prop32)) IN ('srp:for-sale:any', 'srp:for_sale', 'srp:for_sale:null', 'srp:for_sale:foreclosure','srp:for_sale:new_plan','srp:for-sale:new-homes', 'srp:new_community:null','srp:new_community','srp:for-sale:foreclosures','srp:new-home-communities','srp:new-home-communities:new-homes') THEN
        'For Sale'
        ELSE (Case
        WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:recently_sold','ldp:recently_sold','ldp:not-for-sale:recently-sold')
            OR lower(trim(post_prop32)) IN ( 'srp:not_for_sale:recently_sold','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold' ,'srp:recently_sold','srp:off_market:recently_sold') THEN
        'Recently Sold'
        ELSE (Case
        WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:off_market', 'ldp:not_for_sale:recently_sold','ldp:not-for-sale','ldp:not_for_sale','ldp:recently_sold','ldp:not-for-sale:recently-sold','ldp:not_for_sale:foreclosure', 'ldp:just_taken_off_market')
            OR lower(trim(post_prop32)) IN ('srp:not_for_sale:recently_sold','srp:not-for-sale','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold','srp:not-for-sale:any','srp:off_market','srp:recently_sold','srp:not_for_sale:null') THEN
        'Not For Sale'
        ELSE (Case
        WHEN lower(trim(post_prop31)) IN ('ldp:not_for_sale:off_market','ldp:just_taken_off_market')
            OR lower(trim(post_prop32)) IN ( 'srp:off_market','srp:off_market:recently_sold','srp:off_market:foreclosure','srp:off_market:new_plan') THEN
        'Off Market'
        ELSE (Case
        WHEN lower(trim(post_prop31)) IN ('ldp:for_rent','ldp:for_rent:null','ldp:rental','ldp:rentals')
            OR lower(trim(post_prop32)) IN ('srp:rentals','srp:for_rent', 'srp:for_rent:null') THEN
        'For Rent'
        ELSE 'Other' 
              END) 
              END) 
              END) 
              END) 
          END) chnlcat
    ,(Case when
lower(trim(post_evar17)) = 'rdc-responsive' and (OS in (select id from omniture.operating_systems where lower(trim(name)) like 'android%' or lower(trim(name)) like 'ios%'  or lower(trim(name)) like 'linux%' ) OR lower(trim(post_mobileosversion)) LIKE '%ios%')
Then  'Mobile Web'
End) MobileWebStatus
,(Case when
OS IN (SELECT ID FROM OMNITURE.OPERATING_SYSTEMS WHERE lower(trim(name)) LIKE 'android%' OR lower(trim(name)) LIKE 'linux%') 
AND ( lower(trim(post_channel)) = 'core-android' OR lower(trim(post_mobileappid)) LIKE 'realtor 8.5%' OR lower(trim(post_mobileappid)) LIKE 'realtor.com%' )
Then 'Android Apps'
Else 
(Case when
 (OS IN (SELECT ID FROM OMNITURE.OPERATING_SYSTEMS WHERE lower(trim(name)) LIKE 'ios%') OR lower(trim(post_mobileosversion)) LIKE '%ios%')
AND ( lower(trim(post_channel)) = 'core-ios' OR lower(trim(post_mobileappid)) LIKE 'realtor%' ) 
Then 'IOS Apps' 
Else 'Others'
END)
END) AppsType
	,lower(trim(a.post_prop32)) srp_property_status_detail
	,lower(trim(a.post_prop40)) page_type
,(case when
lower(trim(a.post_prop40)) in ('ldp-quickview','ldp','ldp-map','ldp-seller-summary') 
Then 'LDP'
Else
	(case when lower(trim(a.post_prop40)) in ('srp-summary-card','srp-photo','srp-map','srp-list-map','srp-list','srp-list-photo',
                                              'srp','map','srp-listmap','summary-card') 
     then 'SRP'
     else
          (case when lower(trim(a.post_prop40)) in ('home-landing','home-new','home-returning','home-value')
           then 'HOME'    
           else 'Others'
     END)
     END)
  END) page_type_group
	,lower(trim(a.post_prop67)) web_app_version
	,lower(trim(a.post_channel)) site_section
	,lower(trim(a.post_pagename)) page_name_or_url
	,lower(trim(a.post_mobileappid)) mobileappid
	,lower(trim(a.post_mobileosversion)) os_version
	,lower(trim(a.va_closer_id)) va_closer_id
,(case when lower(trim(a.va_closer_id))  = '1' then 'Move Inc Sites'
       when lower(trim(a.va_closer_id))  = '2' then 'Paid Search'
       when lower(trim(a.va_closer_id))  = '3' then 'Organic Search' 
       when lower(trim(a.va_closer_id))  = '4' then 'Display'
       when lower(trim(a.va_closer_id))  = '5' then 'Email'
       when lower(trim(a.va_closer_id))  = '6' then 'Direct (Typed/Bookmarked)'
       when lower(trim(a.va_closer_id))  = '7' then 'Session Refresh' 
       when lower(trim(a.va_closer_id))  = '8' then 'Owned Social'
       when lower(trim(a.va_closer_id))  = '9' then 'Referring Domains'
       when lower(trim(a.va_closer_id))  = '10' then 'Push Notifications'
       when lower(trim(a.va_closer_id))  = '11' then 'Affiliates' 
       when lower(trim(a.va_closer_id))  = '12' then 'Other Campaigns'
       when lower(trim(a.va_closer_id))  = '13' then 'Partner'
       when lower(trim(a.va_closer_id))  = '14' then 'Paid Social'
       when lower(trim(a.va_closer_id))  = '15' then 'Public Relations' 
       when lower(trim(a.va_closer_id))  = '16' then 'Content Syndication'
       when lower(trim(a.va_closer_id))  = '17' then 'Vanity URL'
       when lower(trim(a.va_closer_id))  = '18' then 'Owned Cross Platform'
       when lower(trim(a.va_closer_id))  = '19' then 'Rich Media' 
       when lower(trim(a.va_closer_id))  = '20' then 'News Corp Sites'
       else 'UNKNOWN'
  END) marketing_channel
,(case when lower(trim(a.va_closer_id))  = '1' then 'Others'
       when lower(trim(a.va_closer_id))  = '2' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '3' then 'Organic Search' 
       when lower(trim(a.va_closer_id))  = '4' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '5' then 'Email'
       when lower(trim(a.va_closer_id))  = '6' then 'Direct'
       when lower(trim(a.va_closer_id))  = '7' then 'Others' 
       when lower(trim(a.va_closer_id))  = '8' then 'Owned Social'
       when lower(trim(a.va_closer_id))  = '9' then 'Others'
       when lower(trim(a.va_closer_id))  = '10' then 'Others'
       when lower(trim(a.va_closer_id))  = '11' then 'Total Paid' 
       when lower(trim(a.va_closer_id))  = '12' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '13' then 'Others'
       when lower(trim(a.va_closer_id))  = '14' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '15' then 'Others' 
       when lower(trim(a.va_closer_id))  = '16' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '17' then 'Others'
       when lower(trim(a.va_closer_id))  = '18' then 'Others'
       when lower(trim(a.va_closer_id))  = '19' then 'Total Paid' 
       when lower(trim(a.va_closer_id))  = '20' then 'Others'
       else 'UNKNOWN'
  END) newscorp_view
,(case when lower(trim(a.va_closer_id))  = '1' then 'Others'
       when lower(trim(a.va_closer_id))  = '2' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '3' then 'Organic Search' 
       when lower(trim(a.va_closer_id))  = '4' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '5' then 'Email'
       when lower(trim(a.va_closer_id))  = '6' then 'Direct'
       when lower(trim(a.va_closer_id))  = '7' then 'Others' 
       when lower(trim(a.va_closer_id))  = '8' then 'Owned Social'
       when lower(trim(a.va_closer_id))  = '9' then 'Others'
       when lower(trim(a.va_closer_id))  = '10' then 'Others'
       when lower(trim(a.va_closer_id))  = '11' then 'Total Paid' 
       when lower(trim(a.va_closer_id))  = '12' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '13' then 'Others'
       when lower(trim(a.va_closer_id))  = '14' then 'Others'
       when lower(trim(a.va_closer_id))  = '15' then 'Others' 
       when lower(trim(a.va_closer_id))  = '16' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '17' then 'Others'
       when lower(trim(a.va_closer_id))  = '18' then 'Others'
       when lower(trim(a.va_closer_id))  = '19' then 'Total Paid' 
       when lower(trim(a.va_closer_id))  = '20' then 'Others'
       else 'UNKNOWN'
  END) kpi_channel_view
,(case when lower(trim(a.va_closer_id))  = '1' then 'Others'
       when lower(trim(a.va_closer_id))  = '2' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '3' then 'Organic Search' 
       when lower(trim(a.va_closer_id))  = '4' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '5' then 'Email'
       when lower(trim(a.va_closer_id))  = '6' then 'Direct'
       when lower(trim(a.va_closer_id))  = '7' then 'Others' 
       when lower(trim(a.va_closer_id))  = '8' then 'Owned Social'
       when lower(trim(a.va_closer_id))  = '9' then 'Others'
       when lower(trim(a.va_closer_id))  = '10' then 'Others'
       when lower(trim(a.va_closer_id))  = '11' then 'Total Paid' 
       when lower(trim(a.va_closer_id))  = '12' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '13' then 'Others'
       when lower(trim(a.va_closer_id))  = '14' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '15' then 'Others' 
       when lower(trim(a.va_closer_id))  = '16' then 'Total Paid'
       when lower(trim(a.va_closer_id))  = '17' then 'Others'
       when lower(trim(a.va_closer_id))  = '18' then 'Others'
       when lower(trim(a.va_closer_id))  = '19' then 'Total Paid' 
       when lower(trim(a.va_closer_id))  = '20' then 'Others'
       else 'UNKNOWN'
  END) marketingi_channel_group
,(case when lower(trim(a.va_closer_id))  = '1' then 'Organic'
       when lower(trim(a.va_closer_id))  = '2' then 'Paid'
       when lower(trim(a.va_closer_id))  = '3' then 'Organic' 
       when lower(trim(a.va_closer_id))  = '4' then 'Paid'
       when lower(trim(a.va_closer_id))  = '5' then 'Organic'
       when lower(trim(a.va_closer_id))  = '6' then 'Organic'
       when lower(trim(a.va_closer_id))  = '7' then 'Organic' 
       when lower(trim(a.va_closer_id))  = '8' then 'Organic'
       when lower(trim(a.va_closer_id))  = '9' then 'Organic'
       when lower(trim(a.va_closer_id))  = '10' then 'Organic'
       when lower(trim(a.va_closer_id))  = '11' then 'Paid' 
       when lower(trim(a.va_closer_id))  = '12' then 'Paid'
       when lower(trim(a.va_closer_id))  = '13' then 'Others'
       when lower(trim(a.va_closer_id))  = '14' then 'Paid'
       when lower(trim(a.va_closer_id))  = '15' then 'Others' 
       when lower(trim(a.va_closer_id))  = '16' then 'Paid'
       when lower(trim(a.va_closer_id))  = '17' then 'Organic'
       when lower(trim(a.va_closer_id))  = '18' then 'Organic'
       when lower(trim(a.va_closer_id))  = '19' then 'Paid' 
       when lower(trim(a.va_closer_id))  = '20' then 'Organic'
       else 'UNKNOWN'
  END) paid_vs_organic
	,lower(trim(a.va_closer_detail)) marketing_channel_detail
	,lower(trim(a.mobile_id)) mobile_id
	,lower(trim(a.user_agent)) user_agent
	,lower(trim(a.os)) os
	,lower(trim(a.hit_source)) hit_source
	,lower(trim(a.post_page_event)) post_page_event
,SUM(CASE WHEN post_page_event = '0' THEN 1 ELSE 0 END) AS pv_count
,SUM(CASE WHEN post_page_event = '100' THEN 1 ELSE 0 END) AS click_count
,SUM (case when
lower(trim(post_prop31)) in ('ldp:for_sale', 'ldp:for-sale', 'ldp:for_sale:null', 'ldp:for_sale:foreclosure', 'ldp:new_community:new_plan', 'ldp:new_home_plan' ,'ldp:new-communities','ldp:new_community', 'ldp:for-sale:new-homes','ldp:new-home-communities','ldp:new-home-communities:new-homes', 'ldp:new_community:null','ldp:new-home-communities','ldp:not_for_sale:off_market', 'ldp:not_for_sale:recently_sold','ldp:not-for-sale','ldp:not_for_sale','ldp:recently_sold','ldp:not-for-sale:recently-sold','ldp:not_for_sale:foreclosure', 'ldp:just_taken_off_market', 'ldp:for_rent','ldp:for_rent:null','ldp:rental','ldp:rentals'  ) 
then 1 
else 0
END) as LDPinclPDPallchannels_count
,SUM (case when
lower(trim(post_prop32)) in ('srp:for-sale:any', 'srp:for_sale', 'srp:for_sale:null', 'srp:for_sale:foreclosure','srp:for_sale:new_plan','srp:for-sale:new-homes', 'srp:new_community:null','srp:new_community','srp:for-sale:foreclosures','srp:new-home-communities','srp:new-home-communities:new-homes','srp:not_for_sale:recently_sold','srp:not-for-sale','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold','srp:not-for-sale:any','srp:off_market','srp:recently_sold','srp:not_for_sale:null', 'srp:rentals','srp:for_rent', 'srp:for_rent:null' )
then 1 
else 0
END) as SRPallchannels_count
,SUM (case when
lower(trim(post_prop31)) in ('ldp:for_sale', 'ldp:for-sale', 'ldp:for_sale:null', 'ldp:for_sale:foreclosure', 'ldp:new_community:new_plan', 'ldp:new_home_plan' ,'ldp:new-communities','ldp:new_community', 'ldp:for-sale:new-homes','ldp:new-home-communities','ldp:new-home-communities:new-homes', 'ldp:new_community:null','ldp:new-home-communities', 'ldp:for_rent','ldp:for_rent:null','ldp:rental','ldp:rentals'  )
Then 1
Else 0
END) as  LDPexclPDPallchannelsEXCLoffmktrcsdnfs_count
,SUM (case when
lower(trim(post_prop31)) in ('ldp:not_for_sale:off_market','ldp:not_for_sale:recently_sold', 'ldp:not_for_sale','ldp:recently_sold','ldp:just_taken_off_market', 'ldp:not-for-sale','ldp:not-for-sale:recently-sold') or
lower(trim(post_prop32)) in ( 'srp:not_for_sale:recently_sold','srp:not-for-sale','srp:recently_sold:recently_sold','srp:not-for-sale:recently-sold','srp:not-for-sale:any','srp:not_for_sale:null', 'srp:off_market','srp:not_for_sale','srp:recently_sold','srp:off_market:recently_sold','srp:off_market:foreclosure','srp:off_market:new_plan')
Then 1
Else 0
END) as PDPallchannelsINCLoffmktrcsdnfs_count    
FROM cnpd_omtr_pdt.hit_data_forqa a
	WHERE a.exclude_hit = '0' and a.hit_source in ('1','2')
AND year = '{year}'  
AND month =  '{month}'
AND day =  '{day}'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
