drop index cse532.facilityidx;
drop index cse532.zipidx;

create index cse532.facilityidx on cse532.facility(geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipidx on cse532.uszip(shape) extend using db2gse.spatial_index(0.85, 2, 5);

CREATE INDEX facilitycertification_id_idx
ON CSE532.FACILITYCERTIFICATION(FACILITYID);

CREATE INDEX facilitycertification_attribute_idx
ON CSE532.FACILITYCERTIFICATION(ATTRIBUTEVALUE);

CREATE INDEX facilitycertification_composite_idx
ON CSE532.FACILITYCERTIFICATION(FACILITYID, ATTRIBUTEVALUE);

CREATE INDEX uszip_zip_idx
ON CSE532.USZIP(ZCTA5CE10);

runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;