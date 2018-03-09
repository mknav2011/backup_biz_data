
WITH litings_latest AS
(
SELECT
MAX(cast(listing_dim_key as bigint)) as listing_dim_key,
--  listing_dim_key,
coalesce(listing_address,'') listing_address,
mls_property_id,
property_id,
listing_agent_mls_set_id,
listing_office_mls_set_id
FROM listings_history
WHERE
listing_status in ('for sale', 'for rent')
AND UPPER(listing_city) <> 'BINDER'
AND is_displayed = 1
and mst_effective_from_datetime < timestamp '2017-12-15'
AND mst_effective_to_datetime >=timestamp '2017-12-15'
and bucket in ('historical','latest') and year in ('2017','2018','9999')
--and listing_agent_mls_set_id='A-OKOK-MARTINK'
GROUP BY
listing_address,
mls_property_id,
property_id,
listing_agent_mls_set_id,
listing_office_mls_set_id
),
TempListCLC1 AS
(
SELECT
listing_dim_key,
listing_address,
mls_property_id,
property_id,
listing_agent_mls_set_id CustomerSourceSystemID
from litings_latest WHERE listing_agent_mls_set_id IS NOT NULL
UNION ALL
SELECT
listing_dim_key,
listing_address,
mls_property_id,
property_id,
listing_office_mls_set_id CustomerSourceSystemID
from litings_latest WHERE listing_office_mls_set_id IS NOT NULL
),
main_list as
(
SELECT MSTDateDimKey ,CustomerSourceSystemID , COUNT(property_id) CLC_A, 0 AS CLC_B
FROM
(
	SELECT  timestamp '2017-12-15'  AS MSTDateDimKey, CustomerSourceSystemID, property_id
	FROM    TempListCLC1 GROUP BY CustomerSourceSystemID, property_id
) H GROUP BY MSTDateDimKey , CustomerSourceSystemID
UNION ALL
SELECT MSTDateDimKey ,CustomerSourceSystemID ,0 CLC_A,COUNT(CLC_B) CLC_B
FROM
(
  	SELECT  timestamp '2017-12-15' AS MSTDateDimKey , CustomerSourceSystemID , concat(listing_address, mls_property_id)  AS CLC_B
	FROM    TempListCLC1 GROUP BY CustomerSourceSystemID , concat(listing_address, mls_property_id)
) HH GROUP BY MSTDateDimKey , CustomerSourceSystemID
)
,
tmpProperty AS
(
SELECT  timestamp '2017-12-15'  AS MSTDateDimKey ,
        CustomerSourceSystemID ,
        property_id,
        MAX(listing_dim_key) listing_dim_key
FROM    TempListCLC1
GROUP BY CustomerSourceSystemID , property_id
),
tmpAddress AS
(
SELECT   timestamp '2017-12-15' AS MSTDateDimKey,
		CustomerSourceSystemID ,
		concat(listing_address, mls_property_id)  AS HLC_B,
		MAX(listing_dim_key) listing_dim_key
FROM TempListCLC1
GROUP BY CustomerSourceSystemID ,concat(listing_address, mls_property_id)
),
main_list_2 AS
(
  SELECT
		 timestamp '2017-12-15' AS MSTDateDimKey ,
        CustomerSourceSystemID ,
        CASE WHEN MAX(CLC_A) <MAX(CLC_B) THEN MAX(CLC_A) ELSE MAX(CLC_B) END AS CLC,
		CASE WHEN MAX(CLC_A) <MAX(CLC_B) THEN 'PropertyID' ELSE 'Address' END AS CLCType
  from main_list GROUP BY CustomerSourceSystemID
  )
 , FinalDetailFact AS
 (
SELECT  a.MSTDateDimKey,a.CustomerSourceSystemID,b.listing_dim_key
FROM main_list_2 a
INNER JOIN tmpProperty b ON a.CustomerSourceSystemID = b.CustomerSourceSystemID
WHERE CLCType = 'PropertyID'
UNION
SELECT  a.MSTDateDimKey,a.CustomerSourceSystemID,b.listing_dim_key
FROM main_list_2 a
INNER JOIN tmpAddress b ON a.CustomerSourceSystemID = b.CustomerSourceSystemID
WHERE CLCType = 'Address'
   )
SELECT
		a.MSTDateDimKey ,
        a.CustomerSourceSystemID ,
        b.Listing_Type ,
        b.Listing_Postal_Code ,
        COUNT(1) CLC
FROM listings_history  b
join FinalDetailFact a
ON cast(a.listing_dim_key as varchar) = b.listing_dim_key
GROUP BY
		MSTDateDimKey ,
        CustomerSourceSystemID ,
        Listing_Type ,
        Listing_Postal_Code