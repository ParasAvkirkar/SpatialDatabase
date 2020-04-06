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

INSERT INTO CSE532.merge_component_table(parent_zip, population)
SELECT zcd.zip_code, zcd.population
FROM CSE532.zip_code_details zcd
WHERE zcd.population >= (SELECT AVG(population) FROM CSE532.zip_code_details)@

INSERT INTO CSE532.zip_member_table(parent_zip, member_zip)
SELECT zcd.zip_code, zcd.zip_code
FROM CSE532.zip_code_details zcd
WHERE zcd.population >= (SELECT AVG(population) FROM CSE532.zip_code_details)@

CREATE OR REPLACE PROCEDURE CSE532.MERGE_ZIPCODE (OUT output NUMERIC(15,5))
	LANGUAGE SQL MODIFIES SQL DATA
	BEGIN
		DECLARE curr_avg_population DOUBLE DEFAULT 0;
		DECLARE count INT DEFAULT 0;

		SELECT AVG(population) INTO curr_avg_population FROM CSE532.zip_code_details;

		-- outer loop begin clause
		BEGIN
			DECLARE curr_zip_code VARCHAR(20);
			DECLARE curr_population BIGINT;
						

			FOR v1 AS c1 CURSOR WITH HOLD FOR 
				SELECT zip_code, population 
				FROM CSE532.zip_code_details
				DO

				SET curr_zip_code = zip_code;
				SET curr_population = curr_population;


				IF NOT EXISTS (SELECT * FROM CSE532.zip_member_table WHERE member_zip = curr_zip_code) THEN
					-- if this zip-code is not process yet (begin clause)
					
					BEGIN
						DECLARE is_neighbor_found INT DEFAULT 0;

						FOR v2 AS c2 CURSOR WITH HOLD FOR
						SELECT mct.parent_zip AS neighbor_parent, mct.population AS neighbor_population
							FROM CSE532.zip_code_neighbors zcn
							INNER JOIN CSE532.zip_member_table zmt ON zcn.src_zip_code = curr_zip_code
							AND zmt.member_zip = zcn.neighbor_zip_code
							INNER JOIN CSE532.merge_component_table mct ON zmt.parent_zip = mct.parent_zip
							ORDER BY mct.population LIMIT 1
						DO
							SET is_neighbor_found = 1;
							INSERT INTO CSE532.zip_member_table(parent_zip, member_zip) 
								VALUES(neighbor_parent, curr_zip_code);

							UPDATE CSE532.merge_component_table SET population = neighbor_population + curr_population
								WHERE parent_zip = neighbor_parent;
						END FOR;

						IF is_neighbor_found = 0 THEN
							INSERT INTO CSE532.merge_component_table(parent_zip, population) 
								VALUES(curr_zip_code, curr_population);

							INSERT INTO CSE532.zip_member_table(parent_zip, member_zip) 
								VALUES(curr_zip_code, curr_zip_code);

						END IF;
					END;

				-- ending if-else of zip-code part of zip_member_table
				END IF;
				SET count = count + 1;

				UPDATE CSE532.process_track_table SET zipcode_process = count;

				COMMIT WORK;
			END FOR;

		-- second begin-end clause
		END;

    	SET output = curr_avg_population;

	END@


CALL CSE532.MERGE_ZIPCODE(?)@

SELECT *
FROM CSE532.merge_component_table
WHERE population < 9475.0@

-- DROP TABLE CSE532.zip_code_details@

-- DROP TABLE CSE532.zip_code_neighbors@