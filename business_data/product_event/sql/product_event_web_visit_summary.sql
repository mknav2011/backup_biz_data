select
visitor_id
,visit_id
, device_type as visit_source
, b.weekrangestartingsunday
, b.monthstartdate
, cast(mst_datetime as date) as visit_date
, to_char(current_timestamp, 'yyyy-mm-dd hh24:mi:ss')   as etl_created_datetime , 'glue_etl' as etl_created_by
, min(mst_datetime) as visit_start_time
, max(mst_datetime) as visit_end_time
, count(*) as event_count
, sum(case when page_type_group = 'srp' then 1 else 0 end) srp_view_count
, sum(case when site_section = 'not for sale' then 1 else 0 end) pdp_view_count
, sum(case when page_type_group = 'ldp' then 1 else 0 end) ldp_view_count
, sum(case when site_section = 'for sale' and post_page_event ='0' then 1 else 0 end) fs_pageview_count
, sum(case when site_section = 'for rent' and post_page_event ='0' then 1 else 0 end) fr_pageview_count
, sum(case when site_section = 'not for sale' and post_page_event ='0' then 1 else 0 end) nfs_pageview_count
, sum(case when channel = 'local' and post_page_event ='0' then 1 else 0 end) local_pageview_count
, sum(case when channel = 'news and insights' and post_page_event ='0' then 1 else 0 end) na_pageview_count
, sum(case when channel = 'far' and post_page_event ='0' then 1 else 0 end) far_pageview_count
, sum(case when channel in ('others', 'marketing','advertising') and post_page_event ='0' then 1 else 0 end) others_pageview_count
from biz_data.product_event_rdc_biz_data 
JOIN domain_review.date_dim b
ON(event_date = concat('{year}', '{month}', '{day}' )  and   cast(mst_datetime AS date) = b.fulldate )
group by
visitor_id
,visit_id
, device_type
, b.weekrangestartingsunday
, b.monthstartdate
, cast(mst_datetime as date)
, (case when page_type_group = 'srp' then 1 else 0 end)
, (case when site_section = 'not for sale' then 1 else 0 end)
, (case when page_type_group = 'ldp' then 1 else 0 end)
, (case when site_section = 'for sale' and post_page_event ='0' then 1 else 0 end)
, (case when site_section = 'for rent' and post_page_event ='0' then 1 else 0 end)
, (case when site_section = 'not for sale' and post_page_event ='0' then 1 else 0 end)
, (case when channel = 'local' and post_page_event ='0' then 1 else 0 end)
, (case when channel = 'news and insights' and post_page_event ='0' then 1 else 0 end)
, (case when channel = 'far' and post_page_event ='0' then 1 else 0 end)
, (case when channel in ('others', 'marketing','advertising') and post_page_event ='0' then 1 else 0 end)
