WITH facility_shapes (all_digit_zip, facility_id, shape) AS (
    SELECT f.ZIPCODE, f.FACILITYID, usz.SHAPE
    FROM CSE532.FACILITY f INNER JOIN CSE532.USZIP usz
    ON SUBSTR(f.ZIPCODE, 1, 5) = usz.ZCTA5CE10
), zip_codes_not_having_er(zip_code) AS (
    SELECT DISTINCT f.ZIPCODE
    FROM CSE532.FACILITY f
    WHERE f.ZIPCODE NOT IN (
        SELECT DISTINCT p.ZIPCODE
        FROM CSE532.FACILITY p INNER JOIN CSE532.FACILITYCERTIFICATION q
        ON p.FACILITYID = q.FACILITYID AND q.ATTRIBUTEVALUE = 'Emergency Department'
    )
), zip_codes_self_neighbor_having_er (all_digit_zip) AS (
    SELECT DISTINCT src.all_digit_zip
    FROM facility_shapes src INNER JOIN facility_shapes neighbor
    ON DB2GSE.ST_INTERSECTS(src.shape, neighbor.shape) = 1
    INNER JOIN CSE532.FACILITYCERTIFICATION fac
    ON neighbor.facility_id = fac.FACILITYID AND fac.ATTRIBUTEVALUE = 'Emergency Department'
)
SELECT DISTINCT SUBSTR(i.zip_code, 1, 5) AS noerzips_result
FROM zip_codes_not_having_er i LEFT JOIN zip_codes_self_neighbor_having_er e ON i.zip_code = e.all_digit_zip
WHERE e.all_digit_zip IS NULL;