WITH
cte_datamanager_pdt_datamanager_property AS
(
select row_number() OVER (PARTITION BY propid ORDER BY createdate_datetime DESC, lastupdated_datetime desc ) AS row_number_seq,
propid,mgmtid,listingtypeid,propstatusid,propname, address1, address2, postalcode, latitude, longitude,
createdate_datetime createdate, hspscontractid, hspscustomerid, city, state, phone, fax, email, numunits, clientpropertyid,
clientpeoplesoftid,multipleleadtypeid, lastupdated_datetime lastupdated, createdby, modifiedby, feedsource, last_updated_by_datafeed_datetime last_updated_by_datafeed,
productid, '-1' AS ivr_contact_type_id
FROM lstp_datamanager_pdt__datamanager_property
),
cte_datamanager_property_ivr AS
(
select row_number() OVER (PARTITION BY propid ORDER BY lastupdated_datetime DESC ) AS row_number_seq,
propid,ivrdisplocal, ivrwhisper, tfnassignmentid, lastupdated_datetime
FROM lstp_datamanager_pdt__datamanager_property_ivr
),
cte_senior_propcontact AS
(
SELECT
row_number() OVER (PARTITION BY propid, contacttypeid ORDER BY createdate_datetime DESC, modifieddate_datetime desc ) AS row_number_seq,
propid,
CASE WHEN  contacttypeid = '1' THEN value else '' END phone,
CASE WHEN  contacttypeid = '2' THEN value else '' END fax,
CASE WHEN  contacttypeid = '3' THEN value else '' END email
FROM lstp_senior_pdt__senior_propcontact WHERE contacttypeid in ('1', '2', '3')
),
cte_senior_propcontact_pivot AS
(
SELECT
propid,
MAX(phone) phone,
MAX(fax) fax,
MAX(email) email
FROM cte_senior_propcontact WHERE row_number_seq= 1
group by propid
)
,cte_senior_pdt_senior_property AS
(
select row_number() OVER (PARTITION BY propid ORDER BY createdate_datetime DESC, modifieddate_datetime desc ) AS row_number_seq,
propid,mgmtid, typeid listingtypeid,statusid propstatusid,propname, address1, address2,zip postalcode, latitude, longitude,
createdate_datetime createdate, hspscontractid, hspscustomerid, cityid, '-1' numunits,'na' clientpropertyid,
'na' clientpeoplesoftid,'-1' multipleleadtypeid, modifieddate_datetime lastupdated, 'na' createdby,'na' modifiedby,'na' feedsource, null last_updated_by_datafeed,
'na' productid,
  ivrcontacttypeid, ivrdisplocal, ivrwhisper, aahsaflag, confirmallcalls, modifieddate_datetime modifieddate, att_tollfree,
  mlgflag, tfnassignmentid
FROM lstp_senior_pdt__senior_property
)
,cte_senior_msrcity  AS
(
select row_number() OVER (PARTITION BY cityid ORDER BY etl_last_update_timestamp DESC ) AS row_number_seq,
cityid,lower(city) city,lower(state) state
FROM lstp_senior_pdt__senior_msrcity
)
SELECT
cte_datamanager_pdt_datamanager_property.propid,
cte_datamanager_pdt_datamanager_property.mgmtid,
cte_datamanager_pdt_datamanager_property.listingtypeid,
cte_datamanager_pdt_datamanager_property.propstatusid,
cte_datamanager_pdt_datamanager_property.propname,
cte_datamanager_pdt_datamanager_property.address1,
cte_datamanager_pdt_datamanager_property.address2,
cte_datamanager_pdt_datamanager_property.postalcode,
cte_datamanager_pdt_datamanager_property.latitude,
cte_datamanager_pdt_datamanager_property.longitude,
cte_datamanager_pdt_datamanager_property.createdate,
cte_datamanager_pdt_datamanager_property.hspscontractid,
cte_datamanager_pdt_datamanager_property.hspscustomerid,
cte_datamanager_pdt_datamanager_property.city,
cte_datamanager_pdt_datamanager_property.state,
cte_datamanager_pdt_datamanager_property.phone,
cte_datamanager_pdt_datamanager_property.fax,
cte_datamanager_pdt_datamanager_property.email,
cte_datamanager_pdt_datamanager_property.numunits,
cte_datamanager_pdt_datamanager_property.clientpropertyid,
cte_datamanager_pdt_datamanager_property.clientpeoplesoftid,
cte_datamanager_pdt_datamanager_property.multipleleadtypeid,
cte_datamanager_pdt_datamanager_property.lastupdated,
cte_datamanager_pdt_datamanager_property.createdby,
cte_datamanager_pdt_datamanager_property.modifiedby,
cte_datamanager_pdt_datamanager_property.feedsource,
cte_datamanager_pdt_datamanager_property.productid,
'-1' ivr_contact_type_id,
cte_datamanager_property_ivr.ivrdisplocal,
cte_datamanager_property_ivr.ivrwhisper,
'' aahsaflag,
'' confirmallcalls,
cte_datamanager_pdt_datamanager_property.lastupdated modifieddate,
'na' att_tollfree,
'' mlgflag,
cte_datamanager_property_ivr.tfnassignmentid
,'datamanager' sourcesystem
,'rentals' sourcesystemtype
FROM cte_datamanager_pdt_datamanager_property
LEFT JOIN cte_datamanager_property_ivr ON cte_datamanager_pdt_datamanager_property.propid = cte_datamanager_property_ivr.propid AND cte_datamanager_property_ivr.row_number_seq = 1
WHERE cte_datamanager_pdt_datamanager_property.row_number_seq = 1

UNION ALL


SELECT
cte_senior_pdt_senior_property.propid,
cte_senior_pdt_senior_property.mgmtid,
cte_senior_pdt_senior_property.listingtypeid,
cte_senior_pdt_senior_property.propstatusid,
cte_senior_pdt_senior_property.propname,
cte_senior_pdt_senior_property.address1,
cte_senior_pdt_senior_property.address2,
cte_senior_pdt_senior_property.postalcode,
cte_senior_pdt_senior_property.latitude,
cte_senior_pdt_senior_property.longitude,
cte_senior_pdt_senior_property.createdate,
cte_senior_pdt_senior_property.hspscontractid,
cte_senior_pdt_senior_property.hspscustomerid,
cte_senior_msrcity.city,
cte_senior_msrcity.state,
cte_senior_propcontact_pivot.phone,
cte_senior_propcontact_pivot.fax,
cte_senior_propcontact_pivot.email,
cte_senior_pdt_senior_property.numunits,
cte_senior_pdt_senior_property.clientpropertyid,
cte_senior_pdt_senior_property.clientpeoplesoftid,
cte_senior_pdt_senior_property.multipleleadtypeid,
cte_senior_pdt_senior_property.lastupdated,
cte_senior_pdt_senior_property.createdby,
cte_senior_pdt_senior_property.modifiedby,
cte_senior_pdt_senior_property.feedsource,
cte_senior_pdt_senior_property.productid,
cte_senior_pdt_senior_property.ivrcontacttypeid,
cte_senior_pdt_senior_property.ivrdisplocal,
cte_senior_pdt_senior_property.ivrwhisper,
cte_senior_pdt_senior_property.aahsaflag,
cte_senior_pdt_senior_property.confirmallcalls,
cte_senior_pdt_senior_property.lastupdated modifieddate,
cte_senior_pdt_senior_property.att_tollfree,
cte_senior_pdt_senior_property.mlgflag,
cte_senior_pdt_senior_property.tfnassignmentid
,'senior' sourcesystem
,'seniorhousing' sourcesystemtype
FROM cte_senior_pdt_senior_property
LEFT JOIN cte_senior_propcontact_pivot ON cte_senior_propcontact_pivot.propid=cte_senior_pdt_senior_property.propid
LEFT JOIN cte_senior_msrcity ON cte_senior_msrcity.cityid=cte_senior_pdt_senior_property.cityid and cte_senior_msrcity.row_number_seq = 1
WHERE cte_senior_pdt_senior_property.row_number_seq = 1
