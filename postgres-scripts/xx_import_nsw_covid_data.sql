

-- NSW COVID CASE DATA

-- create postcode table
drop table if exists testing.nsw_covid_cases_20220503_postcode;
create table testing.nsw_covid_cases_20220503_postcode
(
    postcode text    not null,
    cases    integer not null
);
alter table testing.nsw_covid_cases_20220503_postcode owner to postgres;

-- import postcode CSV file
COPY testing.nsw_covid_cases_20220503_postcode
    FROM '/Users/s57405/git/iag_geo/concord/data/nsw_covid_cases_20220503_postcode.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);
analyse testing.nsw_covid_cases_20220503_postcode;

-- add postcode primary key
alter table testing.nsw_covid_cases_20220503_postcode add constraint nsw_covid_cases_20220503_postcode_pkey
    primary key (postcode);


-- create LGA table
drop table if exists testing.nsw_covid_cases_20220503_lga;
create table testing.nsw_covid_cases_20220503_lga
(
    lga_code19 text    not null,
    cases    integer not null
);
alter table testing.nsw_covid_cases_20220503_lga owner to postgres;

-- import LGA CSV file
COPY testing.nsw_covid_cases_20220503_lga
    to '/Users/s57405/git/iag_geo/concord/data/nsw_covid_cases_20220503_lga.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);
analyse testing.nsw_covid_cases_20220503_lga;

-- add LGA primary key
alter table testing.nsw_covid_cases_20220503_lga add constraint nsw_covid_cases_20220503_lga_pkey
    primary key (lga_code19);



-- NSW COVID TEST DATA

-- create postcode table
drop table if exists testing.nsw_covid_tests_20220503_postcode;
create table testing.nsw_covid_tests_20220503_postcode
(
    postcode text    not null,
    tests    integer not null
);
alter table testing.nsw_covid_tests_20220503_postcode owner to postgres;

-- import postcode CSV file
COPY testing.nsw_covid_tests_20220503_postcode
    FROM '/Users/s57405/git/iag_geo/concord/data/nsw_covid_tests_20220503_postcode.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);
analyse testing.nsw_covid_tests_20220503_postcode;

-- add postcode primary key
alter table testing.nsw_covid_tests_20220503_postcode add constraint nsw_covid_tests_20220503_postcode_pkey
    primary key (postcode);


-- create LGA table
drop table if exists testing.nsw_covid_tests_20220503_lga;
create table testing.nsw_covid_tests_20220503_lga
(
    lga_code19 text    not null,
    tests    integer not null
);
alter table testing.nsw_covid_tests_20220503_lga owner to postgres;

-- import LGA CSV file
COPY testing.nsw_covid_tests_20220503_lga
    to '/Users/s57405/git/iag_geo/concord/data/nsw_covid_tests_20220503_lga.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);
analyse testing.nsw_covid_tests_20220503_lga;

-- add LGA primary key
alter table testing.nsw_covid_tests_20220503_lga add constraint nsw_covid_tests_20220503_lga_pkey
    primary key (lga_code19);
