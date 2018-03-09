WITH events as( 
SELECT 
etl_source_filename, etl_source_partition_timestamp 
, etl_approximatearrivaltimestamp,  etl_recordid, etl_ztg_id  
, concat (year, month, day, hour) as mst_datetime 
, params__querystring__accid as  accid
,params__querystring__adtsid as  adtsid
,params__querystring__adtyp as  adtyp
,params__querystring__advantage_lid as  advantage_lid
,params__querystring__appls as  appls
,params__querystring__basicoo_lid as  basicoo_lid
,params__querystring__campid as  campid
, params__querystring__chnl as  chnl
,params__querystring__comid as comid
,params__querystring__env as  env
,params__querystring__lid as  lid
,params__querystring__lnkel as  lnkel
,params__querystring__page as  page
,params__querystring__pgno as pgno
,params__querystring__pgvar as  pgvar
,params__querystring__planid as  planid
, params__querystring__pmdl as pmdl
,params__querystring__pmnf as  pmnf
,params__querystring__pos as pos
,params__querystring__ppage as ppage
, params__querystring__ptnid as  ptnid
,params__querystring__rank as rank
,params__querystring__schid as schid
, params__querystring__session as  session
,params__querystring__src as  src
, params__querystring__sver as sver
, params__querystring__tm as tm
,params__querystring__turbo_active_lid as  turbo_active_lid
, params__querystring__turbos as turbos
, params__querystring__tz  as  tz
,context__user_agent 
,params__querystring__visitor as  visitor
, params__querystring__wigt as  wigt  
,split( replace(replace(params__querystring__basicoo_lid,'[', ''),']','' ), ',') as basicoo_lid_list 
,split( replace(replace(params__querystring__advantage_lid,'[', ''),']','' ), ',') as advantage_lid_list
,split( replace(replace(params__querystring__turbo_active_lid,'[', ''),']','' ), ',') as turbo_lid_list 
from cnpd_mapi_pdt.mapi_forqa
where params__querystring__ptnid   in ('186', '202')
 AND year = '{year}'  
AND month =  '{month}'
  AND day =  '{day}'
),
impressions as (
SELECT * 
FROM events
CROSS JOIN UNNEST(basicoo_lid_list) AS t (impression)
UNION ALL
SELECT * 
FROM events
CROSS JOIN UNNEST(advantage_lid_list) AS t (impression)
UNION ALL
SELECT * 
FROM events
CROSS JOIN UNNEST(turbo_lid_list) AS t (impression) 

),
impressions_split as(
select * 
, case when (ptnid = '186') 
then map( array['lid', 'rank','adtypes', 'roles'], split(impression,':') ) 
when (ptnid = '202' and cardinality(split(impression,':') ) = 5 and cardinality(turbo_lid_list) > 0 )
then map( array['lid', 'rank','adtypes', 'campid', 'roles'], split(impression,':') ) 
when (ptnid = '202' and cardinality(split(impression,':') ) = 4 and cardinality(turbo_lid_list) > 0 )
then map( array['lid', 'rank', 'campid', 'roles'], split(impression,':') )
when (ptnid = '186' and cardinality(split(impression,':') ) = 4 and cardinality(basicoo_lid_list) > 0 )
then map( array['lid', 'rank', 'campid', 'roles'], split(impression,':') )
end as tt_map
from impressions 
) 
select etl_source_filename, etl_source_partition_timestamp , etl_approximatearrivaltimestamp,  etl_recordid, etl_ztg_id, mst_datetime 
, accid, adtsid, adtyp, advantage_lid, appls, basicoo_lid, campid, chnl, comid, env, lid, lnkel, page, pgno, pgvar, planid, pmdl, pmnf, pos, ppage, ptnid, rank, schid, session, src, sver, tm, turbo_active_lid, turbos, tz, visitor, wigt
,context__user_agent 
,element_at(tt_map, 'lid') as impression_lid 
,element_at(tt_map, 'rank') as impression_rank 
,element_at(tt_map, 'adtypes') as impression_adtypes
,element_at(tt_map, 'campid') as impression_campid 
,element_at(tt_map, 'roles') as impression_roles
,regexp_extract(element_at(tt_map, 'roles'), '[a].(\d+)', 1) as impression_agent
,regexp_extract(element_at(tt_map, 'roles'), '[o].(\d+)', 1) as impression_office
,regexp_extract(element_at(tt_map, 'roles'), '[b].(\d+)', 1) as impression_broker 
from impressions_split

UNION ALL
SELECT etl_source_filename, etl_source_partition_timestamp 
, etl_approximatearrivaltimestamp,  etl_recordid, etl_ztg_id  
, concat (year, month, day, hour) as mst_datetime 
, params__querystring__accid as  accid
,params__querystring__adtsid as  adtsid
,params__querystring__adtyp as  adtyp
,params__querystring__advantage_lid as  advantage_lid
,params__querystring__appls as  appls
,params__querystring__basicoo_lid as  basicoo_lid
,params__querystring__campid as  campid
, params__querystring__chnl as  chnl
,params__querystring__comid as comid
,params__querystring__env as  env
,params__querystring__lid as  lid
,params__querystring__lnkel as  lnkel
,params__querystring__page as  page
,params__querystring__pgno as pgno
,params__querystring__pgvar as  pgvar
,params__querystring__planid as  planid
, params__querystring__pmdl as pmdl
,params__querystring__pmnf as  pmnf
,params__querystring__pos as pos
,params__querystring__ppage as ppage
, params__querystring__ptnid as  ptnid
,params__querystring__rank as rank
,params__querystring__schid as schid
, params__querystring__session as  session
,params__querystring__src as  src
, params__querystring__sver as sver
, params__querystring__tm as tm
,params__querystring__turbo_active_lid as  turbo_active_lid
, params__querystring__turbos as turbos
, params__querystring__tz  as  tz 
,params__querystring__visitor as  visitor
, params__querystring__wigt as  wigt
,context__user_agent
,null as impression_lid 
,null as impression_rank 
,null as impression_adtypes
,null as impression_campid 
,null as impression_roles
,null as impression_agent
,null as impression_office
,null as impression_broker 
from cnpd_mapi_pdt.mapi_forqa  
where params__querystring__ptnid not in ('186', '202')
 AND year = '{year}'  
AND month =  '{month}'
  AND day =  '{day}'
