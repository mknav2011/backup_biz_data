with cte_datamanager_management as
(
select row_number() OVER (PARTITION BY mgmtid ORDER BY createdate_datetime DESC, lastupdated_datetime desc ) AS row_number_seq,
mgmtid, mgmtname, mgmtaddress1, mgmtaddress2, mgmtcity, mgmtstate, mgmtpostalcode, mgmtcontact,
billingcontact, mgmtphone, mgmtfax, mgmtemail, mgmtlicense, mgmthomepage, mgmtlogo, pushreports, createdate, lastupdated
,'datamanager'  sourcesystem
,'rentals' sourcesystemtype,lastupdated_datetime, createdate_datetime
FROM lstp_datamanager_pdt__datamanager_management
),
cte_senior_management as
(
select row_number() OVER (PARTITION BY mgmtid ORDER BY createdate_datetime DESC, modifieddate_datetime desc ) AS row_number_seq,
mgmtid, mgmtname, address1 mgmtaddress1, address2 mgmtaddress2,city mgmtcity,state mgmtstate, zip mgmtpostalcode,'na' mgmtcontact,
'na' billingcontact, phone mgmtphone, fax mgmtfax, email mgmtemail, 'na' mgmtlicense,mgmturl mgmthomepage,mgmtlogo mgmtlogo,
'' pushreports, createdate,modifieddate lastupdated
,'senior'  sourcesystem
,'seniorhousing' sourcesystemtype,modifieddate_datetime lastupdated_datetime,createdate_datetime createdate_datetime
FROM lstp_senior_pdt__senior_management
)

SELECT
mgmtid, mgmtname, mgmtaddress1, mgmtaddress2, mgmtcity, mgmtstate, mgmtpostalcode, mgmtcontact,
billingcontact, mgmtphone, mgmtfax, mgmtemail, mgmtlicense, mgmthomepage, mgmtlogo, pushreports, createdate, lastupdated
,sourcesystem
,sourcesystemtype,lastupdated_datetime, createdate_datetime
FROM cte_datamanager_management WHERE row_number_seq=1
UNION ALL
SELECT
mgmtid, mgmtname, mgmtaddress1, mgmtaddress2, mgmtcity, mgmtstate, mgmtpostalcode, mgmtcontact,
billingcontact, mgmtphone, mgmtfax, mgmtemail, mgmtlicense, mgmthomepage, mgmtlogo, pushreports, createdate, lastupdated
,sourcesystem
,sourcesystemtype,lastupdated_datetime, createdate_datetime
FROM cte_senior_management WHERE row_number_seq=1
