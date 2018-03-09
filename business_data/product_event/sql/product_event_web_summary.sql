SELECT cast(mst_datetime as date) mst_date
,visitor_id
,visit_id
,brand_experience
,brand_experience_type
,ldp_property_status_detail
,site_section
,sub_site_section
,mobile_web_status
,apps_type
,srp_property_status_detail
,page_type
,page_type_group
,web_app_version
,channel
,page_name_or_url
,os_version
,marketing_channel
,newscorp_view
,kpi_channel_view
,marketing_channel_group
,paid_vs_organic
,marketing_channel_detail
,user_agent
,device_type
,SUM(CASE WHEN post_page_event = '0' THEN 1 ELSE 0 END) AS pv_count
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss')   as etl_created_datetime , 'glue_etl' as etl_created_by
FROM biz_data.product_event_rdc_biz_data  a
WHERE
1 = 1
AND event_date = concat('{year}', '{month}', '{day}' )
Group by
cast(mst_datetime as date)
,visitor_id
,visit_id
,brand_experience
,brand_experience_type
,ldp_property_status_detail
,site_section
,sub_site_section
,mobile_web_status
,apps_type
,srp_property_status_detail
,page_type
,page_type_group
,web_app_version
,channel
,page_name_or_url
,os_version
,marketing_channel
,newscorp_view
,kpi_channel_view
,marketing_channel_group
,paid_vs_organic
,marketing_channel_detail
,user_agent
,device_type 
