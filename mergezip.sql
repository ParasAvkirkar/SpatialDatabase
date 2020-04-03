CREATE TABLE CSE532.zip_code_details (
	zip_code VARCHAR(20),
	population BIGINT,
	shape DB2GSE.ST_MULTIPOLYGON
)@

CREATE TABLE CSE532.zip_code_neighbors (
	src_zip_code VARCHAR(20),
	neighbor_zip_code VARCHAR(20)
)@


INSERT INTO CSE532.zip_code_details(zip_code, population, shape) SELECT usz.ZCTA5CE10, z.ZPOP, usz.shape
FROM CSE532.USZIP usz INNER JOIN
    CSE532.ZIPPOP z
    ON CAST(usz.ZCTA5CE10 AS INT) = z.ZIP
    LIMIT 10@

INSERT INTO CSE532.zip_code_neighbors SELECT src.ZCTA5CE10, neighbor.ZCTA5CE10
FROM CSE532.USZIP src INNER JOIN CSE532.USZIP neighbor
ON DB2GSE.ST_INTERSECTS(src.SHAPE, neighbor.SHAPE) = 1 LIMIT 10@


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

		DECLARE CONTINUE HANDLER FOR NOT FOUND SET at_end = 1;


		FOR v_row AS SELECT zip_code, population FROM CSE532.zip_code_details DO
			-- SET count = count + 1;

			BEGIN
				DECLARE zip_code_cursor CURSOR FOR SELECT ZCTA5CE10 FROM CSE532.USZIP WHERE ZCTA5CE10 = '43451';
				SET at_end = 0;
				OPEN zip_code_cursor;
				FETCH zip_code_cursor INTO zip_code;
				WHILE (at_end = 0) DO
					SET count = count + 1000;
					FETCH zip_code_cursor INTO zip_code;
				END WHILE;

				CLOSE zip_code_cursor;
			END;

		END FOR;

		

    	SET output = count;

	END@


CALL CSE532.MERGE_ZIPCODE(?)@

DROP TABLE CSE532.zip_code_details@

DROP TABLE CSE532.zip_code_neighbors@