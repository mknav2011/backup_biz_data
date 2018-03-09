select
visitor_id
,visit_id
,experience_type
, device_type 
, b.weekrangestartingsunday
, b.monthstartdate
, cast(mst_datetime as date) as visit_date
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss')   as etl_created_datetime , 'glue_etl' as etl_created_by
, min(mst_datetime) as visit_start_time
, max(mst_datetime) as visit_end_time
, count(*) as event_count
, sum(case when page_type_group = 'srp' and post_page_event ='0' then 1 else 0 end) srp_pageview_count
, sum(case when property_status = 'not for sale' and post_page_event ='0' then 1 else 0 end) pdp_pageview_count
, sum(case when page_type_group = 'ldp' and post_page_event ='0' then 1 else 0 end) ldp_pageview_count
, sum(case when property_status = 'for sale' and post_page_event ='0' then 1 else 0 end) for_sale_pageview_count
, sum(case when property_status = 'for rent' and post_page_event ='0' then 1 else 0 end) for_rent_pageview_count
, sum(case when site_section = 'local' and post_page_event ='0' then 1 else 0 end) local_pageview_count
, sum(case when site_section = 'news and insights' and post_page_event ='0' then 1 else 0 end) na_pageview_count
, sum(case when site_section = 'far' and post_page_event ='0' then 1 else 0 end) far_pageview_count
, sum(case when site_section in ('others', 'marketing','advertising') and post_page_event ='0' then 1 else 0 end) others_pageview_count
from product_event.rdc_biz_data
JOIN domain_review.date_dim b
ON(event_date = concat('{year}', '{month}', '{day}' )  and   cast(mst_datetime AS date) = b.fulldate )
group by
visitor_id
,visit_id
,experience_type
, device_type
, b.weekrangestartingsunday
, b.monthstartdate
, cast(mst_datetime as date)
