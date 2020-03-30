WITH zip_codes_having_emergency (zip_code) AS (
            SELECT DISTINCT u.ZCTA5CE10 AS zip_code
            FROM CSE532.FACILITY f INNER JOIN CSE532.FACILITYCERTIFICATION c
            ON f.FACILITYID = c.FACILITYID AND c.ATTRIBUTEVALUE = 'Emergency Department'
            INNER JOIN CSE532.USZIP AS u
            ON SUBSTR(f.ZIPCODE, 1, 5) = u.ZCTA5CE10
),
zip_code_neighbor_relationship (source_zip, neighbor_zip) AS (
    SELECT us_source.ZCTA5CE10, us_neighbor.ZCTA5CE10
    FROM CSE532.USZIP AS us_source
    INNER JOIN CSE532.USZIP AS us_neighbor
    ON DB2GSE.ST_INTERSECTS(us_source.SHAPE, us_neighbor.SHAPE) = 1
),
 zip_self_neighbors_having_emergency(zip_code) AS (
    SELECT zn.source_zip
    FROM zip_code_neighbor_relationship AS zn INNER JOIN zip_codes_having_emergency AS ze
        ON zn.neighbor_zip = ze.zip_code
    GROUP BY zn.source_zip)
SELECT usz.ZCTA5CE10 AS result_zip_code
FROM CSE532.USZIP usz LEFT JOIN zip_self_neighbors_having_emergency zcwsne
ON usz.ZCTA5CE10 = zcwsne.zip_code
WHERE zcwsne.zip_code IS NULL;


WITH zip_codes_with_polygons (zip_code, location, polygon, facility_id) AS (
    SELECT f.ZIPCODE, f.GEOLOCATION, DB2GSE.ST_Buffer(f.GEOLOCATION, 10, 'STATUTE MILE'), f.FACILITYID
    FROM CSE532.FACILITY f
),
zip_code_self_neighbors_er (zip_code) AS (
    SELECT DISTINCT source.zip_code
    FROM zip_codes_with_polygons source INNER JOIN zip_codes_with_polygons neighbors
    ON DB2GSE.ST_INTERSECTS(neighbors.location, source.polygon)
    INNER JOIN CSE532.FACILITYCERTIFICATION c
    ON neighbors.facility_id = c.FACILITYID AND c.ATTRIBUTEVALUE = 'Emergency Department'
)
SELECT i.ZIPCODE
FROM CSE532.FACILITY i LEFT JOIN zip_code_self_neighbors_er j
ON i.ZIPCODE = j.zip_code
WHERE j.zip_code IS NULL;