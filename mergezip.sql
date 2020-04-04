-- CREATE TABLE CSE532.zip_code_details (
-- 	zip_code VARCHAR(20),
-- 	population BIGINT,
-- 	shape DB2GSE.ST_MULTIPOLYGON
-- )@

-- CREATE TABLE CSE532.zip_code_neighbors (
-- 	src_zip_code VARCHAR(20),
-- 	neighbor_zip_code VARCHAR(20)
-- )@


-- INSERT INTO CSE532.zip_code_details(zip_code, population, shape)  WITH zip_pop(zip_code, population) AS (
--     SELECT z.ZIP, MAX(z.ZPOP)
--     FROM CSE532.ZIPPOP z
--     GROUP BY z.ZIP
-- )
-- SELECT usz.ZCTA5CE10, zp.population, usz.SHAPE
-- FROM CSE532.USZIP usz INNER JOIN zip_pop zp
-- ON CAST(usz.ZCTA5CE10 AS INT) = zp.zip_code AND zp.population > 0;
--     @

-- INSERT INTO CSE532.zip_code_neighbors(src_zip_code, src_population, neighbor_zip_code, neighbor_population)
-- WITH zip_pop(zip_code, population) AS (
--     SELECT z.ZIP, MAX(z.ZPOP)
--     FROM CSE532.ZIPPOP z
--     GROUP BY z.ZIP
-- ), zip_population_shape(zip_code, population, shape) AS (
--     SELECT usz.ZCTA5CE10, zp.population, usz.SHAPE
--     FROM CSE532.USZIP usz INNER JOIN zip_pop zp
--     ON CAST(usz.ZCTA5CE10 AS INT) = zp.zip_code AND zp.population > 0
-- )
-- SELECT src.zip_code, src.population, neighbor.zip_code, neighbor.population
-- FROM zip_population_shape src INNER JOIN zip_population_shape neighbor
-- ON DB2GSE.ST_INTERSECTS(src.shape, neighbor.shape) = 1;


CREATE OR REPLACE PROCEDURE CSE532.MERGE_ZIPCODE (OUT output NUMERIC(15,5))
	LANGUAGE SQL MODIFIES SQL DATA
	BEGIN
		
		DECLARE count INT DEFAULT 0;
		DECLARE rows_fetched INT DEFAULT 0;
		DECLARE at_end INT DEFAULT 0;

		DECLARE curr_avg_population DOUBLE DEFAULT 0;

		DECLARE v_zip_code VARCHAR(20);
		DECLARE v_population BIGINT;
		DECLARE v_neighbor_zip_code VARCHAR(20);

		DECLARE running_component_id INT DEFAULT 1;

		DECLARE CONTINUE HANDLER FOR NOT FOUND SET at_end = 1;


		FOR v_row AS SELECT zip_code, population FROM CSE532.zip_code_details DO
			-- SET count = count + 1;

			IF NOT EXISTS (SELECT * FROM CSE532.zip_member_table WHERE member_zip = v_row.zip_code) THEN
				SET count = count + 2;
			END IF;

			-- BEGIN
			-- 	DECLARE zip_code_cursor CURSOR FOR SELECT ZCTA5CE10 FROM CSE532.USZIP WHERE ZCTA5CE10 = '43451';
			-- 	SET at_end = 0;
			-- 	OPEN zip_code_cursor;
			-- 	FETCH zip_code_cursor INTO zip_code;
			-- 	WHILE (at_end = 0) DO
			-- 		SET count = count + 1000;
			-- 		FETCH zip_code_cursor INTO zip_code;
			-- 	END WHILE;

			-- 	CLOSE zip_code_cursor;
			-- END;

		END FOR;

		

    	SET output = count;

	END@


CALL CSE532.MERGE_ZIPCODE(?)@

-- DROP TABLE CSE532.zip_code_details@

-- DROP TABLE CSE532.zip_code_neighbors@