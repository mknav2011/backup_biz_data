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
, sum(case when page_type_group = 'srp' and lower(trim(event_type_id)) = '70' then 1 else 0 end) srp_pageview_count
, sum(case when property_status = 'not for sale' and lower(trim(event_type_id)) = '70' then 1 else 0 end) pdp_pageview_count
, sum(case when page_type_group = 'ldp' and lower(trim(event_type_id)) = '70' then 1 else 0 end) ldp_pageview_count
, sum(case when property_status = 'for sale' and lower(trim(event_type_id)) = '70' then 1 else 0 end) for_sale_pageview_count
, sum(case when property_status = 'for rent' and lower(trim(event_type_id)) = '70' then 1 else 0 end) for_rent_pageview_count
, sum(case when property_status = 'others' and lower(trim(event_type_id)) = '70' then 1 else 0 end) others_pageview_count
from biz_data_product_event.mapi_biz_data
JOIN domain_review.date_dim b
ON(event_date = concat('{year}', '{month}', '{day}' ) and cast(mst_datetime AS date) = b.fulldate )
group by
visitor_id
,visit_id
,experience_type
, device_type
, b.weekrangestartingsunday
, b.monthstartdate
, cast(mst_datetime as date)
