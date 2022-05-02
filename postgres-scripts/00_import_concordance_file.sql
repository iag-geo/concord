
-- create table
drop table if exists gnaf_202202.boundary_concordance;
create table gnaf_202202.boundary_concordance
(
    from_source     text not null,
    from_type       text not null,
    from_id         text not null,
    from_name       text not null,
    to_source       text not null,
    to_type         text not null,
    to_id           text not null,
    to_name         text not null,
    address_count   integer,
    address_percent double precision,
    primary key (from_id, to_id)
);
alter table gnaf_202202.boundary_concordance owner to postgres;

-- import from CSV file
COPY gnaf_202202.boundary_concordance
    FROM '/Users/minus34/git/iag_geo/concord/data/boundary_concordance.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

analyse gnaf_202202.boundary_concordance;
