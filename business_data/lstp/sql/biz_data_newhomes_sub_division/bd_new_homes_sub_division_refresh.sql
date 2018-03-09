WITH edw_newhomes_subdivisions AS
(
select
row_number() OVER (PARTITION BY mastersubdivisionid ORDER BY cast(subdivisiondimkey as bigint) DESC) AS row_number_seq,
subdivisiondimkey,
mastersubdivisionid,
masterbuilderid,
isdeleted,
dateislive,
amsubdivisionrecordstatus,
bdxsubdivisionid,
status,
sharewithrealtors,
pricelow,
pricehigh,
sqftlow,
sqfthigh,
marketingchannel,
subdivisionnumber,
subdivisionname,
subparentname,
subdesc,
usedefaultleadsemail,
subleadsemail,
subleadsemailleadspermessage,
subadrstreet1,
subadrstreet2,
subadrcounty,
subadrcity,
subadrstate,
subadrzip,
subadrcountry,
subadrlat,
subadrlong,
subvideotour,
drivingdirections,
subwebsite,
buildonyourlot,
communitystyle,
photocount,
schdistname,
schdistnameleaid,
schelem,
schelemncesid,
schmiddle,
schmiddlencesid,
schhigh,
schhighncesid,
servicehoa,
servicehoamonthlyfee,
servicehoaname,
servicehoayearlyfee,
subamenpool,
subamenplayground,
subamengolfcourse,
subamentennis,
subamensoccer,
subamenvolleyball,
subamenbasketball,
subamenbaseball,
subamenviews,
subamenlake,
subamenpond,
subamenmarina,
subamenbeach,
subamenwaterfrontlots,
subamenpark,
subamentrails,
subamengreenbelt,
subamenclubhouse,
subamencomcenter,
greenprogramval,
greenprogramtitle,
greenprogramreftype,
primarycommunityimage,
accreditationseal,
promotion,
testimonial,
salesoffagt,
salesoffadroutofcommunity,
salesoffadrstreet1,
salesoffadrstreet2,
salesoffadrcounty,
salesoffadrcity,
salesoffadrstate,
salesoffadrzip,
salesoffadrcountry,
salesoffadrgeolat,
salesoffadrgeolong,
salesoffphoneareacode,
salesoffphoneprefix,
salesoffphonesuffix,
salesoffphoneextension,
salesoffemail,
salesofffaxareacode,
salesofffaxprefix,
salesofffaxsuffix,
salesoffhours,
totaltaxrate,
taxes,
effectivefrom,
effectiveto,
createddate,
createdby,
modifieddate,
modifiedby
from
lstp_edw_pdt__newhomes_sub_division_dim
)
SELECT
accreditationseal accreditation_seal,
amsubdivisionrecordstatus am_sub_division_recordstatus,
bdxsubdivisionid bdx_sub_division_id,
buildonyourlot build_on_your_lot,
communitystyle community_style,
createdby created_by,
cast(substring(createddate,1,23)as timestamp) created_date,
cast(substring(dateislive,1,23)as timestamp) date_is_live,
drivingdirections driving_directions,
cast(substring(effectivefrom,1,23)as timestamp) effective_from,
cast(substring(effectiveto,1,23)as timestamp) effective_to,
greenprogramreftype green_program_ref_type,
greenprogramtitle green_program_title,
greenprogramval green_program_val,
CASE WHEN isdeleted = '1' THEN 'y' else 'n' END is_deleted,
marketingchannel marketing_channel,
masterbuilderid master_builder_id,
mastersubdivisionid master_sub_division_id,
modifiedby modifie_dby,
cast(substring(modifieddate,1,23)as timestamp) modified_date,
photocount photo_count,
pricehigh price_high,
pricelow price_low,
primarycommunityimage primary_community_image,
promotion promotion,
lower(salesoffadrcity) sales_off_adr_city,
lower(salesoffadrcountry) sales_off_adr_country,
lower(salesoffadrcounty) sales_off_adr_county,
salesoffadrgeolat sales_off_adr_geolat,
salesoffadrgeolong sales_off_adr_geolong,
salesoffadroutofcommunity sales_off_adr_out_of_community,
lower(salesoffadrstate) sales_off_adr_state,
lower(salesoffadrstreet1) sales_off_adr_street1,
lower(salesoffadrstreet2) sales_off_adr_street2,
lower(salesoffadrzip) sales_off_adr_zip,
lower(salesoffagt) sales_off_agt,
salesoffemail sales_off_email,
salesofffaxareacode sales_off_fax_area_code,
salesofffaxprefix sales_off_fax_prefix,
salesofffaxsuffix sales_off_fax_suffix,
salesoffhours sales_off_hours,
salesoffphoneareacode sales_off_phone_area_code,
salesoffphoneextension sales_off_phone_extension,
salesoffphoneprefix sales_off_phone_prefix,
salesoffphonesuffix sales_off_phone_suffix,
lower(schdistname) sch_dist_name,
lower(schdistnameleaid) sch_dist_name_leaid,
schelem sch_elem,
schelemncesid sch_elemnces_id,
schhigh sch_high,
schhighncesid sch_highnces_id,
schmiddle sch_middle,
schmiddlencesid sch_middlences_id,
servicehoa service_hoa,
servicehoamonthlyfee service_hoa_monthly_fee,
servicehoaname service_hoa_name,
servicehoayearlyfee service_hoa_yearly_fee,
sharewithrealtors share_with_realtors,
sqfthigh sqft_high,
sqftlow sqft_low,
status status,
subadrcity sub_adr_city,
subadrcountry sub_adr_country,
subadrcounty sub_adr_county,
subadrlat sub_adr_lat,
subadrlong sub_adr_long,
subadrstate sub_adr_state,
subadrstreet1 sub_adr_street1,
subadrstreet2 sub_adr_street2,
subadrzip sub_adr_zip,
subamenbaseball sub_amen_base_ball,
subamenbasketball sub_amen_basket_ball,
subamenbeach sub_amen_beach,
subamenclubhouse sub_amen_club_house,
subamencomcenter sub_amen_com_center,
subamengolfcourse sub_amen_golf_course,
subamengreenbelt sub_amen_green_belt,
subamenlake sub_amen_lake,
subamenmarina sub_amen_marina,
subamenpark sub_amen_park,
subamenplayground sub_amen_play_ground,
subamenpond sub_amen_pond,
subamenpool sub_amen_pool,
subamensoccer sub_amen_soccer,
subamentennis sub_amen_tennis,
subamentrails sub_amen_trails,
subamenviews sub_amen_views,
subamenvolleyball sub_amen_volley_ball,
subamenwaterfrontlots sub_amen_water_front_lots,
subdesc sub_desc,
subdivisiondimkey sub_division_dim_key,
subdivisionname sub_division_name,
subdivisionnumber sub_division_number,
subleadsemail sub_leads_email,
lower(subleadsemailleadspermessage) sub_leads_email_leads_per_message,
subparentname sub_parent_name,
subvideotour sub_video_tour,
subwebsite sub_web_site,
taxes taxes,
testimonial testimonial,
totaltaxrate total_tax_rate,
usedefaultleadsemail use_default_leads_email
FROM edw_newhomes_subdivisions
WHERE row_number_seq = 1
