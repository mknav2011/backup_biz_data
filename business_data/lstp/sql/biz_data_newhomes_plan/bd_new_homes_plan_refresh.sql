WITH edw_newhomes_plan AS
(
select
row_number() OVER (PARTITION BY masterplanid ORDER BY cast(plandimkey as bigint) DESC) AS row_number_seq,
plandimkey,
masterplanid,
mastersubdivisionid,
isdeleted,
dateislive,
amplanrecordstatus,
bdxplanid,
plannumber,
planname,
plantype,
plandesc,
planmarketingheadline,
planhothome,
planhothometitle,
planhothomedesc,
plannotavailable,
planbaseprice,
planbasepriceexcludesland,
planbasesqft,
planstories,
planlivingroom,
planlivingarea,
planlivingareatype,
plandiningroom,
plandiningareas,
planbedrooms,
planmasterbedlocation,
planbaths,
planhalfbaths,
plangarage,
plangarageentry,
planbasement,
planbonusroom,
planguestroom,
planmediaroom,
plangameroom,
planoffroom,
planloft,
planstudy,
plansunroom,
planfamilyroom,
planadroutofcommunity,
planadrstreet1,
planadrstreet2,
planadrcounty,
planadrcity,
planadrstate,
planadrzip,
planadrcountry,
planadrgeolat,
planadrgeolong,
planimgvirtualtour,
planimgviewer,
planwebsite,
planenvisiondesigncenter,
photocount,
planamenvaultedceilings,
planamenfireplaces,
planamenwalkinclosets,
effectivefrom,
effectiveto,
createddate,
createdby,
modifieddate,
modifiedby
from
lstp_edw_pdt__newhomes_plan_dim
)
SELECT
amplanrecordstatus am_plan_record_status,
bdxplanid bdx_plan_id,
createdby created_by,
cast(substring(createddate,1,23)as timestamp) created_date,
cast(substring(dateislive,1,23)as timestamp) date_is_live,
cast(substring(effectivefrom,1,23)as timestamp) effective_from,
cast(substring(effectiveto,1,23)as timestamp) effective_to,
isdeleted is_deleted,
masterplanid master_plan_id,
mastersubdivisionid master_sub_division_id,
modifiedby modified_by,
cast(substring(modifieddate,1,23)as timestamp) modified_date,
photocount photo_count,
lower(planadrcity) plan_adr_city,
lower(planadrcountry) plan_adr_country,
lower(planadrcounty) plan_adr_county,
planadrgeolat plan_adr_geo_lat,
planadrgeolong plan_adr_geo_long,
planadroutofcommunity plan_adr_out_of_community,
lower(planadrstate) plan_adr_state,
lower(planadrstreet1) plan_adr_street1,
lower(planadrstreet2) plan_adr_street2,
planadrzip plan_adr_zip,
planamenfireplaces plan_amen_fire_places,
planamenvaultedceilings plan_amen_vaulted_ceilings,
planamenwalkinclosets plan_amen_walkin_closets,
planbasement plan_basement,
planbaseprice plan_base_price,
planbasepriceexcludesland plan_base_price_excludes_land,
planbasesqft plan_base_sqft,
planbaths plan_baths,
planbedrooms plan_bed_rooms,
planbonusroom plan_bonus_room,
plandesc plan_desc,
plandimkey plan_dim_key,
plandiningareas plan_dining_areas,
plandiningroom plan_dining_room,
planenvisiondesigncenter plan_envision_design_center,
planfamilyroom plan_family_room,
plangameroom plan_game_room,
plangarage plan_garage,
plangarageentry plan_garage_entry,
planguestroom plan_guest_room,
planhalfbaths plan_half_baths,
planhothome plan_hot_home,
lower(planhothomedesc) plan_hot_home_desc,
lower(planhothometitle)  plan_hot_home_title,
planimgviewer plan_img_viewer,
planimgvirtualtour plan_img_virtual_tour,
planlivingarea plan_living_area,
planlivingareatype plan_living_area_type,
planlivingroom plan_living_room,
planloft plan_loft,
planmarketingheadline plan_marketing_head_line,
planmasterbedlocation plan_master_bed_location,
planmediaroom plan_media_room,
planname plan_name,
plannotavailable plan_not_available,
plannumber plan_number,
planoffroom plan_off_room,
planstories plan_stories,
planstudy plan_study,
plansunroom plan_sun_room,
plantype plan_type,
planwebsite plan_web_site
FROM edw_newhomes_plan
WHERE row_number_seq = 1
