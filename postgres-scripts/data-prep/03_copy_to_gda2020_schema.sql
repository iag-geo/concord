
CREATE SCHEMA IF NOT EXISTS gnaf_202508_gda2020 AUTHORIZATION postgres;

-- create tables
drop table if exists gnaf_202508_gda2020.address_principal_census_2016_boundaries;
create table gnaf_202508_gda2020.address_principal_census_2016_boundaries as
select * from gnaf_202508.address_principal_census_2016_boundaries;
analyse gnaf_202508_gda2020.address_principal_census_2016_boundaries;

drop table if exists gnaf_202508_gda2020.address_principal_census_2021_boundaries;
create table gnaf_202508_gda2020.address_principal_census_2021_boundaries as
select * from gnaf_202508.address_principal_census_2021_boundaries;
analyse gnaf_202508_gda2020.address_principal_census_2021_boundaries;

drop table if exists gnaf_202508_gda2020.boundary_concordance_score;
create table gnaf_202508_gda2020.boundary_concordance_score as
select * from gnaf_202508.boundary_concordance_score;
analyse gnaf_202508_gda2020.boundary_concordance_score;

drop table if exists gnaf_202508_gda2020.boundary_concordance;
create table gnaf_202508_gda2020.boundary_concordance as
select * from gnaf_202508.boundary_concordance;
analyse gnaf_202508_gda2020.boundary_concordance;

-- add primary key (faster if done after import) -- completed in 8 s 496 ms
alter table gnaf_202508_gda2020.boundary_concordance add constraint boundary_concordance_gda2020_pkey
    primary key (from_source, from_bdy, from_id, to_source, to_bdy, to_id);

-- add index on required fields for converting data
create index boundary_concordance_gda2020_combo_idx on gnaf_202508_gda2020.boundary_concordance
    using btree (from_source, from_bdy, to_source, to_bdy);

alter table gnaf_202508_gda2020.boundary_concordance cluster on boundary_concordance_gda2020_combo_idx;

