
-- create table
drop table if exists gnaf_202202.boundary_concordance;
create table gnaf_202202.boundary_concordance
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
alter table gnaf_202202.boundary_concordance owner to postgres;

-- import CSV file -- 586,977 rows affected in 1 s 365 ms
COPY gnaf_202202.boundary_concordance
    FROM '/Users/minus34/Downloads/boundary_concordance.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

analyse gnaf_202202.boundary_concordance;

-- add primary key (faster if done after import) -- completed in 8 s 496 ms
alter table gnaf_202202.boundary_concordance add constraint boundary_concordance_pkey
    primary key (from_source, from_bdy, from_id, to_source, to_bdy, to_id);
