
-- create table
drop table if exists gnaf_202508.boundary_concordance;
create table gnaf_202508.boundary_concordance
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
    address_percent numeric(4, 1)
);
alter table gnaf_202508.boundary_concordance owner to postgres;

-- import CSV file -- 586,977 rows affected in 1 s 365 ms
COPY gnaf_202508.boundary_concordance
    FROM '/Users/minus34/Downloads/boundary_concordance.csv'
    WITH (HEADER, DELIMITER ',', FORMAT CSV);

analyse gnaf_202508.boundary_concordance;

-- add primary key (faster if done after import) -- completed in 8 s 496 ms
alter table gnaf_202508.boundary_concordance add constraint boundary_concordance_pkey
    primary key (from_source, from_bdy, from_id, to_source, to_bdy, to_id);

-- add index on required fields for converting data
create index boundary_concordance_combo_idx on gnaf_202508.boundary_concordance
    using btree (from_source, from_bdy, to_source, to_bdy);

alter table gnaf_202508.boundary_concordance cluster on boundary_concordance_combo_idx;