
-- get all ABS Census boundaries for each GNAF address (GNAF only links directly to meshblocks, SAs & GCCs)

-- step 1 - create a bunch of temp tables merging meshblocks with non-ABS structures (LGA, RAs etc...)
-- use meshblock bdy centroids to get the bdy ID
-- this approach is simpler than downloading & importing ABS correspondence files, which are subject to change
-- also RA, SED and CED bdys are groups of SA1s; the rest are groups of meshblocks. Meshblocks are used for all bdys to keep the code simple (performance hit is minimal)

-- create temp table of meshblock centroids (ensure centroid is within polygon by using ST_PointOnSurface)
drop table if exists temp_mb;
create temporary table temp_mb as
select mb_code16,
       ST_Transform(ST_PointOnSurface(geom), 4283) as geom
from census_2016_bdys.mb_2016_aust
;
analyse temp_mb;
create index temp_mb_geom_idx on temp_mb using gist (geom);
alter table temp_mb cluster on temp_mb_geom_idx;

-- get temp tables of meshblock IDs per boundary

drop table if exists temp_ced_mb;
create temporary table temp_ced_mb as
select distinct temp_mb.mb_code16, bdy.ced_code16, bdy.ced_name16 from temp_mb
inner join census_2016_bdys.ced_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ced_mb;

drop table if exists temp_lga_mb;
create temporary table temp_lga_mb as
select distinct temp_mb.mb_code16, bdy.lga_code16, bdy.lga_name16 from temp_mb
inner join census_2016_bdys.lga_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_lga_mb;

drop table if exists temp_poa_mb;
create temporary table temp_poa_mb as
select distinct temp_mb.mb_code16, bdy.poa_code16, bdy.poa_name16 from temp_mb
inner join census_2016_bdys.poa_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_poa_mb;

drop table if exists temp_ra_mb;
create temporary table temp_ra_mb as
select distinct temp_mb.mb_code16, bdy.ra_code16, bdy.ra_name16 from temp_mb
inner join census_2016_bdys.ra_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ra_mb;

drop table if exists temp_sed_mb;
create temporary table temp_sed_mb as
select distinct temp_mb.mb_code16, bdy.sed_code16, bdy.sed_name16 from temp_mb
inner join census_2016_bdys.sed_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_sed_mb;

drop table if exists temp_ucl_mb;
create temporary table temp_ucl_mb as
select distinct temp_mb.mb_code16, bdy.ucl_code16, bdy.ucl_name16 from temp_mb
inner join census_2016_bdys.ucl_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ucl_mb;

drop table temp_mb;


-- step 2 -- get ABS bdy IDs for all addresses -- 14,451,352 rows in 5 mins
drop table if exists gnaf.address_principal_census_2016_boundaries;
create table gnaf.address_principal_census_2016_boundaries as
with abs as (
    select mb.mb_code16,
           mb_cat16,
           sa1_main16 as sa1_code16,
           sa1_7dig16 as sa1_name16,
           sa2_main16 as sa2_code16,
           sa2_name16,
           sa3_code16,
           sa3_name16,
           sa4_code16,
           sa4_name16,
           gcc_code16,
           gcc_name16,
           ced_code16,
           ced_name16,
           lga_code16,
           lga_name16,
           poa_code16,
           poa_name16,
           ra_code16,
           ra_name16,
           sed_code16,
           sed_name16,
           ucl_code16,
           ucl_name16,
           mb.ste_code16,
           mb.ste_name16
    from census_2016_bdys.mb_2016_aust as mb
    inner join temp_ced_mb as ced on ced.mb_code16 = mb.mb_code16
    inner join temp_lga_mb as lga on lga.mb_code16 = mb.mb_code16
    inner join temp_poa_mb as poa on poa.mb_code16 = mb.mb_code16
    inner join temp_ra_mb as ra on ra.mb_code16 = mb.mb_code16
    inner join temp_ucl_mb as ucl on ucl.mb_code16 = mb.mb_code16
    left outer join temp_sed_mb as sed on sed.mb_code16 = mb.mb_code16
)
select gid,
       adr.gnaf_pid,
       abs.*
from gnaf.address_principals as adr
     inner join abs on abs.mb_code16 = adr.mb_2016_code::text
;
analyse gnaf.address_principal_census_2016_boundaries;

alter table gnaf.address_principal_census_2016_boundaries add constraint address_principal_census_2016_boundaries_pkey primary key (gnaf_pid);
alter table gnaf.address_principal_census_2016_boundaries cluster on address_principal_census_2016_boundaries_pkey;

drop table if exists temp_ced_mb;
drop table if exists temp_lga_mb;
drop table if exists temp_poa_mb;
drop table if exists temp_ra_mb;
drop table if exists temp_ucl_mb;
drop table if exists temp_sed_mb;


-- select count(*) from gnaf.address_principals; -- 14,488,752
-- select count(*) from gnaf.address_principal_census_2016_boundaries; -- 14,488,752
