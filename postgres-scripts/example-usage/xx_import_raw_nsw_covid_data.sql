
-- NSW COVID CASE DATA

-- create table
drop table if exists testing.nsw_raw_covid_cases_20220503;
create table testing.nsw_raw_covid_cases_20220503
(
    notification_date     date    not null,
    postcode              text    not null,
    lhd_2010_code         text    not null,
    lhd_2010_name         text    not null,
    lga_code19            text    not null,
    lga_name19            text    not null,
    confirmed_by_pcr      text    not null,
    confirmed_cases_count integer not null
);
alter table testing.nsw_raw_covid_cases_20220503 owner to postgres;

-- import CSV file -- 142,390 rows affected in 336 ms
COPY testing.nsw_raw_covid_cases_20220503
    FROM '/Users/minus34/git/iag_geo/concord/data/confirmed_cases_table1_location_agg.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

analyse testing.nsw_raw_covid_cases_20220503;

-- add primary key (faster if done after import) -- completed in 8 s 496 ms
alter table testing.nsw_raw_covid_cases_20220503 add constraint nsw_raw_covid_cases_20220503_pkey
    primary key (notification_date, postcode, lga_code19, confirmed_by_pcr);


-- create aggregated cases by postcode table for testing concordance file -- 617 rows affected in 138 ms
drop table if exists testing.nsw_covid_cases_20220503_postcode;
create table testing.nsw_covid_cases_20220503_postcode as
select postcode,
       sum(confirmed_cases_count) as cases
from testing.nsw_raw_covid_cases_20220503
group by postcode
;
analyse testing.nsw_covid_cases_20220503_postcode;

alter table testing.nsw_covid_cases_20220503_postcode add constraint nsw_covid_cases_20220503_postcode_pkey
    primary key (postcode);

-- create aggregated cases by LGA table for testing concordance file -- 131 rows affected in 104 ms
drop table if exists testing.nsw_covid_cases_20220503_lga;
create table testing.nsw_covid_cases_20220503_lga as
select lga_code19,
       sum(confirmed_cases_count) as cases
from testing.nsw_raw_covid_cases_20220503
group by lga_code19
;
analyse testing.nsw_covid_cases_20220503_lga;

alter table testing.nsw_covid_cases_20220503_lga add constraint nsw_covid_cases_20220503_lga_pkey
    primary key (lga_code19);


-- export cases tables
COPY testing.nsw_covid_cases_20220503_postcode
    to '/Users/minus34/git/iag_geo/concord/data/nsw_covid_cases_20220503_postcode.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY testing.nsw_covid_cases_20220503_lga
    to '/Users/minus34/git/iag_geo/concord/data/nsw_covid_cases_20220503_lga.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);


-- NSW COVID TEST DATA

-- create table
drop table if exists testing.nsw_raw_covid_tests_20220503;
create table testing.nsw_raw_covid_tests_20220503
(
    test_date     date    not null,
    postcode      text    not null,
    lhd_2010_code text    not null,
    lhd_2010_name text    not null,
    lga_code19    text    not null,
    lga_name19    text    not null,
    test_count    integer not null
);
alter table testing.nsw_raw_covid_tests_20220503 owner to postgres;

-- import CSV file -- 142,390 rows affected in 336 ms
COPY testing.nsw_raw_covid_tests_20220503
    FROM '/Users/minus34/git/iag_geo/concord/data/pcr_testing_table1_location_agg.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

analyse testing.nsw_raw_covid_tests_20220503;

-- add primary key (faster if done after import) -- completed in 8 s 496 ms
alter table testing.nsw_raw_covid_tests_20220503 add constraint nsw_raw_covid_tests_20220503_pkey
    primary key (test_date, postcode, lga_code19);


-- create aggregated tests by postcode table for testing concordance file -- 613 rows affected in 99 ms
drop table if exists testing.nsw_covid_tests_20220503_postcode;
create table testing.nsw_covid_tests_20220503_postcode as
select postcode,
       sum(test_count) as tests
from testing.nsw_raw_covid_tests_20220503
group by postcode
;
analyse testing.nsw_covid_tests_20220503_postcode;

alter table testing.nsw_covid_tests_20220503_postcode add constraint nsw_covid_tests_20220503_postcode_pkey
    primary key (postcode);

-- create aggregated tests by LGA table for testing concordance file -- 129 rows affected in 594 ms
drop table if exists testing.nsw_covid_tests_20220503_lga;
create table testing.nsw_covid_tests_20220503_lga as
select lga_code19,
       sum(test_count) as tests
from testing.nsw_raw_covid_tests_20220503
group by lga_code19
;
analyse testing.nsw_covid_tests_20220503_lga;

alter table testing.nsw_covid_tests_20220503_lga add constraint nsw_covid_tests_20220503_lga_pkey
    primary key (lga_code19);


-- export tests tables
COPY testing.nsw_covid_tests_20220503_postcode
    to '/Users/minus34/git/iag_geo/concord/data/nsw_covid_tests_20220503_postcode.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY testing.nsw_covid_tests_20220503_lga
    to '/Users/minus34/git/iag_geo/concord/data/nsw_covid_tests_20220503_lga.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);
