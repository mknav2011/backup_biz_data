with profile_api_current as
(select * from (select *, row_number() over(partition by fulfillment_id  order by meta__last_update_date_mst desc, etl_create_date desc) as row_num
from profile_api
where  meta__last_update_date_mst < cast('PULL_DATE_PARAM' as date)
) a where row_num = 1)
select
P.fulfillment_id,
nrds_id,
role,
fullname as full_name,
firstname as first_name,
middlename as middle_name,
lastname as last_name,
P.name as profile_name,
photo__href as photo,
background_photo__href as background_photo,
case when bio = '' then 'true' else 'false' end as bio ,
max(case when PE.type = 'Showcase Lead Routing'
then PE.value else '' end) as email_showcase_lead_routing,
max(case when PE.type = 'My/Your Contact'
then PE.value else '' end) as email_my_your_contact,
max(case when PE.type = 'FAR Profile'
then PE.value else '' end) as email_far_profile,
max(case when PP.type = 'Mobile'
then PP.number else '' end) as phone_mobile,
max(case when PP.type = 'Office'
then PP.number else '' end) as phone_office,
max(case when PP.type = 'Fax'
then PP.number else '' end) as phone_fax,
max(case when PP.type = 'Toll Free'
then PP.number else '' end) as phone_toll_free,
max(case when PP.type = 'Home Office'
then PP.number else '' end) as phone_home_office,
max(case when PP.type = 'Other'
then PP.number else '' end) as phone_other,
max(case when PP.type = 'Home'
then PP.number else '' end) as phone_home,
replace(replace(replace(replace(zips,'['),']'),'"'),',','|') as zips,
concat(PM.type,'-',PM.abbreviation,'-',PM.member__id) as primary_mls_set_id,
concat(POM.type,'-',POM.abbreviation,'-',POM.member__id) as primary_office_mls_set_id,
broker__name as brokerage_firm_name,
first_year,
replace(replace(replace(replace(specialties,'['),']'),'"'),',','|') as specialties,
social_connections__blog_rss__link as linked_blog,
max(PF.link) as linked_facebook,
address__line as address_line,
address__city as city,
address__state as state,
address__state_code as state_code,
address__postal_code as postal_code,
address__country as country,
recommendations__count as recommendations_count,
ratings__responseCount as response_count,
ratings__overall_satisfaction as overall_satisfaction,
ratings__average_rating as average_rating,
ratings__overall_rating as overall_rating,
ratings__testimonial_return_rate as testimonial_return_rate,
ratings__performance_rating as performance_rating,
ratings__show_scores as show_scores,
ratings__recommendation_rating as recommendation_rating,
ratings__reviews_count as reviews_count,
office__fulfillment_id as office_fulfillment_id,
broker__fulfillment_id as broker_fulfillment_id,
office__address__line as office_address_line,
office__address__city as office_city,
office__address__state as office_state,
office__address__state_code as office_state_code,
office__address__postal_code as office_postal_code,
office__address__country as office_country,
P.id as source_id,
record_status,
far_override,
default,
meta__last_update_date_mst as mst_last_update_date,
website,
case when slogan = '' then 'true' else 'false' end as slogan,
'SNAPSHOT_DATE_PARAM' as data_snapshot_date
from profile_api_current P
left join profile_api___emails PE
on P.etl_ztg_id = PE.etl_parent_ztg_id
left join profile_api___phones PP
on P.etl_ztg_id = PP.etl_parent_ztg_id
left join profile_api___social_connections__facebook__pages PF
on P.etl_ztg_id = PF.etl_parent_ztg_id
left join profile_api___mls PM
on P.etl_ztg_id = PM.etl_parent_ztg_id and PM.primary = 'true'
left join profile_api___office__mls POM
on P.etl_ztg_id = POM.etl_parent_ztg_id and POM.primary = 'true'
group by
P.fulfillment_id,
nrds_id,
role,
fullname,
firstname,
middlename,
lastname,
P.name,
photo__href,
background_photo__href,
case when bio = '' then 'true' else 'false' end,
replace(replace(replace(replace(zips,'['),']'),'"'),',','|'),
concat(PM.type,'-',PM.abbreviation,'-',PM.member__id),
concat(POM.type,'-',POM.abbreviation,'-',POM.member__id),
broker__name,
first_year,
replace(replace(replace(replace(specialties,'['),']'),'"'),',','|'),
social_connections__blog_rss__link,
address__line,
address__city ,
address__state,
address__state_code,
address__postal_code,
address__country,
recommendations__count,
ratings__responseCount,
ratings__overall_satisfaction,
ratings__average_rating,
ratings__overall_rating,
ratings__testimonial_return_rate,
ratings__performance_rating,
ratings__show_scores,
ratings__recommendation_rating,
ratings__reviews_count,
office__fulfillment_id,
broker__fulfillment_id,
office__address__line,
office__address__city,
office__address__state,
office__address__state_code,
office__address__postal_code,
office__address__country,
P.id,
record_status,
far_override,
default,
meta__last_update_date_mst,
website,
case when slogan = '' then 'true' else 'false' end
limit 100