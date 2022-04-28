
-- get all ABS Census boundaries for each GNAF address (GNAF only links directly to meshblocks, SAs & GCCs)

-- step 1 - create a bunch of temp tables merging meshblocks with non-ABS structures (LGA, RAs etc...)
-- use meshblock bdy centroids to get the bdy ID
-- this approach is simpler than downloading & importing ABS correspondence files, which are subject to change
-- also RA, SED and CED bdys are groups of SA1s; the rest are groups of meshblocks. Meshblocks are used for all bdys to keep the code simple (performance hit is minimal)

-- create temp table of meshblock centroids (ensure centroid is within polygon by using ST_PointOnSurface)
drop table if exists temp_mb;
create temporary table temp_mb as
select mb_16code,
       ST_PointOnSurface(geom) as geom
from admin_bdys_202202.abs_2016_mb
;
analyse temp_mb;
create index temp_mb_geom_idx on temp_mb using gist (geom);
alter table temp_mb cluster on temp_mb_geom_idx;

-- get temp tables of meshblock IDs per boundary

drop table if exists temp_ced_mb;
create temporary table temp_ced_mb as
select distinct temp_mb.mb_16code, bdy.ced_code16 as ced_16code, bdy.ced_name16 as ced_16name from temp_mb
inner join census_2016_bdys.ced_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ced_mb;

drop table if exists temp_lga_mb;
create temporary table temp_lga_mb as
select distinct temp_mb.mb_16code, bdy.lga_code16 as lga_16code, bdy.lga_name16 as lga_16name from temp_mb
inner join census_2016_bdys.lga_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_lga_mb;

drop table if exists temp_poa_mb;
create temporary table temp_poa_mb as
select distinct temp_mb.mb_16code, bdy.poa_code16 as poa_16code, bdy.poa_name16 as poa_16name from temp_mb
inner join census_2016_bdys.poa_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_poa_mb;

drop table if exists temp_ra_mb;
create temporary table temp_ra_mb as
select distinct temp_mb.mb_16code, bdy.ra_code16 as ra_16code, bdy.ra_name16 as ra_16name from temp_mb
inner join census_2016_bdys.ra_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ra_mb;

drop table if exists temp_sed_mb;
create temporary table temp_sed_mb as
select distinct temp_mb.mb_16code, bdy.sed_code16 as sed_16code, bdy.sed_name16 as sed_16name from temp_mb
inner join census_2016_bdys.sed_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_sed_mb;

drop table if exists temp_ucl_mb;
create temporary table temp_ucl_mb as
select distinct temp_mb.mb_16code, bdy.ucl_code16 as ucl_16code, bdy.ucl_name16 as ucl_16name from temp_mb
inner join census_2016_bdys.ucl_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ucl_mb;

drop table temp_mb;


-- step 2 -- get ABS bdy IDs for all addresses -- 14,451,352 rows in 5 mins
drop table if exists gnaf_202202.address_principal_census_2016_boundaries;
create table gnaf_202202.address_principal_census_2016_boundaries as
with abs as (
    select mb.mb_16code,
           mb_category,

           sa1_16main,
           sa1_16_7cd,
           sa2_16main,
           sa2_16_5cd,
           sa2_16name,
           sa3_16code,
           sa3_16name,
           sa4_16code,
           sa4_16name,
           gcc_16code,
           gcc_16name,
           ced_16code,
           ced_16name,
           lga_16code,
           lga_16name,
           poa_16code,
           poa_16name,
           ra_16code,
           ra_16name,
           sed_16code,
           sed_16name,
           ucl_16code,
           ucl_16name,
           mb.state
    from admin_bdys_202202.abs_2016_mb as mb
    inner join temp_ced_mb as ced on ced.mb_16code = mb.mb_16code
    inner join temp_lga_mb as lga on lga.mb_16code = mb.mb_16code
    inner join temp_poa_mb as poa on poa.mb_16code = mb.mb_16code
    inner join temp_ra_mb as ra on ra.mb_16code = mb.mb_16code
    inner join temp_ucl_mb as ucl on ucl.mb_16code = mb.mb_16code
    left outer join temp_sed_mb as sed on sed.mb_16code = mb.mb_16code
)
select gid,
       gnaf.gnaf_pid,
       blg.is_residential,
       blg.
       -- reliability,
       abs.*
from gnaf_202202.address_principals as gnaf
     inner join abs on abs.mb_16code = gnaf.mb_2016_code
     inner join geoscape_202203.address_principals_buildings as blg on blg.gnaf_pid = gnaf.gnaf_pid

;
analyse gnaf_202202.address_principal_census_2016_boundaries;

alter table gnaf_202202.address_principal_census_2016_boundaries add constraint address_principal_census_2016_boundaries_pkey primary key (gnaf_pid);
alter table gnaf_202202.address_principal_census_2016_boundaries cluster on address_principal_census_2016_boundaries_pkey;

drop table if exists temp_ced_mb;
drop table if exists temp_lga_mb;
drop table if exists temp_poa_mb;
drop table if exists temp_ra_mb;
drop table if exists temp_ucl_mb;
drop table if exists temp_sed_mb;


-- update where non-residential planning zone but MB is residential -- 1,686,417 rows
update gnaf_202202.address_principal_census_2016_boundaries
set is_residential = 'residential'
where is_residential is null
  and mb_category = 'RESIDENTIAL'
;
analyse gnaf_202202.address_principal_census_2016_boundaries;


-- select count(*) from gnaf_202202.address_principals; -- 14,451,352
select count(*) from gnaf_202202.address_principal_census_2016_boundaries; -- 14,451,352

-- select * from gnaf_202202.address_principal_census_2016_boundaries where sed_16code is null;

