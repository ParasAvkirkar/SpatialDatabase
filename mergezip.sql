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
		DECLARE rows_to_process INT DEFAULT 0;
		DECLARE at_end INT DEFAULT 0;

		DECLARE curr_avg_population DOUBLE DEFAULT 0;

		DECLARE running_component_id INT DEFAULT 1;

		DECLARE CONTINUE HANDLER FOR NOT FOUND SET at_end = 1;

		SELECT AVG(population) INTO curr_avg_population FROM CSE532.zip_code_details;

		-- outer loop begin clause
		BEGIN
			DECLARE curr_zip_code VARCHAR(20);
			DECLARE curr_population BIGINT;
			DECLARE is_exit INT;

			DECLARE zip_code_process_cursor CURSOR WITH HOLD FOR SELECT zip_code, population FROM CSE532.zip_code_details;

			OPEN zip_code_process_cursor;
			
			SELECT COUNT(*) INTO rows_to_process FROM CSE532.zip_code_details;

			WHILE count < rows_to_process DO
				FETCH zip_code_process_cursor INTO curr_zip_code, curr_population;
				IF NOT EXISTS (SELECT * FROM CSE532.zip_member_table WHERE member_zip = curr_zip_code) THEN
					-- if this zip-code is not process yet (begin clause)
					
					BEGIN
						DECLARE v_component_id INT;
						DECLARE v_zip_code VARCHAR(20);
						DECLARE v_population BIGINT;

						DECLARE neighbor_cursor CURSOR FOR SELECT mct.component_id, mct.parent_zip, mct.population
							FROM CSE532.zip_code_neighbors zcn
							INNER JOIN CSE532.zip_member_table zmt ON zcn.src_zip_code = curr_zip_code
							AND zmt.member_zip = zcn.neighbor_zip_code
							INNER JOIN CSE532.merge_component_table mct ON zmt.component_id = mct.component_id
							AND mct.population < curr_avg_population
							ORDER BY mct.population LIMIT 1;

						OPEN neighbor_cursor;					
						FETCH neighbor_cursor INTO v_component_id, v_zip_code, v_population;
						IF v_population IS NOT NULL THEN
							INSERT INTO CSE532.zip_member_table(parent_zip, member_zip, component_id) 
								VALUES(v_zip_code, curr_zip_code, v_component_id);

							UPDATE CSE532.merge_component_table SET population = v_population + curr_population
								WHERE component_id = v_component_id;
						ELSE
							INSERT INTO CSE532.merge_component_table(component_id, parent_zip, population) 
								VALUES(running_component_id, curr_zip_code, curr_population);

							INSERT INTO CSE532.zip_member_table(parent_zip, member_zip, component_id) 
								VALUES(curr_zip_code, curr_zip_code, running_component_id);

							SET running_component_id = running_component_id + 1;
						END IF;

						CLOSE neighbor_cursor;
					END;

				-- ending if-else of zip-code part of zip_member_table
				END IF;
				

				SET count = count + 1;
				UPDATE CSE532.process_track_table SET zipcode_process = count;

				COMMIT WORK;
			END WHILE;
			CLOSE zip_code_process_cursor;

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