
-- create table
drop table if exists testing.nsw_covid_cases_20220503;
create table testing.nsw_covid_cases_20220503
(
    from_source     text not null,
    from_bdy        text not null,
    from_id         text not null,
    from_name       text not null,
    to_source       text not null,
    to_bdy          text not null,
    to_id           text not null,
    to_name         text not null,
    address_count   integer,
    address_percent double precision
);
alter table testing.nsw_covid_cases_20220503 owner to postgres;

-- import CSV file -- 586,977 rows affected in 1 s 365 ms
COPY testing.nsw_covid_cases_20220503
    FROM '/Users/s57405/git/iag_geo/concord/data/nsw_covid_cases_20220503.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

analyse testing.nsw_covid_cases_20220503;

-- add primary key (faster if done after import) -- completed in 8 s 496 ms
alter table testing.nsw_covid_cases_20220503 add constraint nsw_covid_cases_20220503_pkey
    primary key (from_source, from_bdy, from_id, to_source, to_bdy, to_id);
