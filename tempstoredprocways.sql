-- CREATE USER TEMPORARY TABLESPACE USERTEMPDATA MANAGED BY AUTOMATIC STORAGE;
CREATE OR REPLACE PROCEDURE CSE532.MERGE_ZIPCODE (OUT STDDEV NUMERIC(15,5))
	LANGUAGE SQL MODIFIES SQL DATA
	P1: BEGIN
    	

		DECLARE curr_avg_population DOUBLE DEFAULT 0;
		DECLARE count INT DEFAULT 0;


		-- DECLARE GLOBAL TEMPORARY TABLE  SESSION.dec (
	 --    	zip_code INT,
	 --    	population BIGINT,
	 --    	shape DB2GSE.ST_MULTIPOLYGON
	 --    );


	 	CREATE GLOBAL TEMPORARY TABLE zip_code_details (
	 		zip_code INT,
	 		population BIGINT,
	 		shape DB2GSE.ST_MULTIPOLYGON
 		);


		INSERT INTO zip_code_details(zip_code, population, shape) SELECT z.ZIP, z.ZPOP, usz.shape
		FROM CSE532.USZIP usz INNER JOIN
		    CSE532.ZIPPOP z
		    ON CAST(usz.ZCTA5CE10 AS INT) = z.ZIP;
		
		-- call DB2GSE.ST_REGISTER_SPATIAL_COLUMN('CSE532','USZIP','shape', 'NAD83_SRS_1');


		SELECT AVG(population) INTO curr_avg_population
		FROM zip_code_details;

		


    	SET STDDEV = curr_avg_population;

    	DROP TABLE zip_code_details;

	END P1@

-- CREATE TABLE CSE532.MERGE_ZIPCODE_SHAPES (
--     member_id INT,
--     zip_code INT,
--     zip_code_root INT,
--     shape DB2GSE.ST_MULTIPOLYGON
-- )@

CALL CSE532.MERGE_ZIPCODE(?)@