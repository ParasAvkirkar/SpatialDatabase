WITH
  source_address(point) AS (
    VALUES(DB2GSE.ST_Point(-72.993983, 40.824369, 1))
  )
SELECT f.FACILITYID,
 		f.FACILITYNAME,
 		f.ADDRESS1,
 		f.ADDRESS2,
 		f.STATE,
 		f.ZIPCODE,
 		f.COUNTYCODE,
 		f.COUNTY,
 		f.GEOLOCATION,
  		CONCAT(DECIMAL(DB2GSE.ST_Distance(s.point, f.GEOLOCATION, 'STATUTE MILE'), 10, 4), ' miles') AS distance
FROM CSE532.FACILITY f INNER JOIN CSE532.FACILITYCERTIFICATION c
    ON f.FACILITYID = c.FACILITYID AND c.ATTRIBUTEVALUE = 'Emergency Department'
    INNER JOIN source_address s
    ON DB2GSE.ST_Intersects(f.GEOLOCATION, DB2GSE.ST_Buffer(s.point, 10, 'STATUTE MILE')) = 1 
ORDER BY distance
LIMIT 1;