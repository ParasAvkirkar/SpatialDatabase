CREATE OR REPLACE PROCEDURE CSE532.MERGE_ZIPCODE (OUT output NUMERIC(15,5))
	LANGUAGE SQL MODIFIES SQL DATA
P1: BEGIN
	-- Declare variables/cursor
		DECLARE curr_avg_population DOUBLE DEFAULT 0;
		DECLARE count INT DEFAULT 0; 
		DECLARE v_shape DB2GSE.ST_MULTIPOLYGON;
		
		DECLARE CUR CURSOR FOR SELECT shape FROM CSE532.USZIP LIMIT 10;

		CREATE GLOBAL TEMPORARY TABLE zip_code_details (
	 		zip_code INT,
	 		population BIGINT,
	 		shape DB2GSE.ST_MULTIPOLYGON
 		);
 		
 		INSERT INTO zip_code_details(zip_code, population, shape) SELECT z.ZIP, z.ZPOP, usz.shape
		FROM CSE532.USZIP usz INNER JOIN
		    CSE532.ZIPPOP z
		    ON CAST(usz.ZCTA5CE10 AS INT) = z.ZIP
		    LIMIT 10;
	
		DROP TABLE zip_code_details;
		
		OPEN CUR;
		FETCH_LOOP: LOOP 
		 	FETCH CUR INTO v_shape;
		 	SET count = count + 1;
		 	
			END LOOP FETCH_LOOP;
		CLOSE CUR;
		
		SET output = count;
		
	
END P1