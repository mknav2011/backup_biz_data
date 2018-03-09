WITH litings_latest AS
(
SELECT
MAX(cast(listing_dim_key as bigint)) as listing_dim_key,
listing_address,
mls_id,
property_id,
listing_agent_mls_set_id,
listing_office_mls_set_id
FROM listings_history
WHERE
listing_status in ('for sale', 'for rent')
AND UPPER(listing_city) <> 'BINDER'
AND is_displayed = 1
AND timestamp '2017-12-15' BETWEEN mst_effective_from_datetime AND mst_effective_to_datetime
GROUP BY
listing_address,
mls_id,
property_id,
listing_agent_mls_set_id,
listing_office_mls_set_id
),
TempListCLC1 AS
(
SELECT
listing_dim_key,
listing_address,
mls_id,
property_id,
listing_agent_mls_set_id CustomerSourceSystemID
from litings_latest WHERE listing_agent_mls_set_id IS NOT NULL
UNION ALL
SELECT
listing_dim_key,
listing_address,
mls_id,
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
  	SELECT  timestamp '2017-12-15' AS MSTDateDimKey , CustomerSourceSystemID , concat(listing_address, mls_id)  AS CLC_B
	FROM    TempListCLC1 GROUP BY CustomerSourceSystemID , concat(listing_address, mls_id)
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
		concat(listing_address, mls_id)  AS HLC_B,
		MAX(listing_dim_key) listing_dim_key
FROM TempListCLC1
GROUP BY CustomerSourceSystemID ,concat(listing_address, mls_id)
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
        substring(a.CustomerSourceSystemID,1,1) ,
        COUNT(1) CLC
FROM FinalDetailFact a
GROUP BY
        substring(a.CustomerSourceSystemID,1,1)

