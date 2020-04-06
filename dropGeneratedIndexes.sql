drop index facilitycertification_id_idx;
drop index facilitycertification_attribute_idx;
drop index facilitycertification_attribute_idx;
drop index facilitycertification_composite_idx;
drop index uszip_zip_idx;
drop index facilityidx;
drop index zipidx;


runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;