!erase zip.msg;

DROP TABLE cse532.uszip;

!erase zip.msg;
 
!db2se import_shape sample
-fileName         C:\Users\Paras\Documents\TheoryOfDatabase\homework3\tl_2019_us_zcta510\tl_2019_us_zcta510.shp
-srsName          nad83_srs_1
-tableSchema      cse532
-tableName        uszip
-spatialColumn    shape
-client           1
-messagesFile     zip.msg
;
 
!db2se register_spatial_column sample
-tableSchema      cse532
-tableName        uszip
-columnName       shape
-srsName          nad83_srs_1
;
 
 -- describe table cse532.uszip;