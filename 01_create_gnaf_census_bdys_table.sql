
-- get all ABS Census boundaries for each GNAF address (GNAF only links directly to meshblocks, SAs & GCCs)

-- step 1 - create a bunch of temp tables merging meshblocks with non-ABS structures (LGA, RAs etc...)
-- use meshblock bdy centroids to get the bdy ID

-- create temp table of meshblock centroids (ensuring centroid is within polygon)
drop table if exists temp_mb;
create temporary table temp_mb as
select mb_16code,
       ST_PointOnSurface(geom) as geom
from admin_bdys_202202.abs_2016_mb
;
analyse temp_mb;
create index temp_mb_geom_idx on temp_mb using gist (geom);
alter table temp_mb cluster on temp_mb_geom_idx;

-- get temp tables of meshblock IDs by boundary
drop table if exists temp_lga_mb;
create temporary table temp_lga_mb as
select temp_mb.mb_16code, bdy.lga_code16 as lga_16code, bdy.lga_name16 as lga_16name from temp_mb
inner join census_2016_bdys.lga_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_lga_mb;
ALTER TABLE temp_lga_mb ADD CONSTRAINT temp_lga_mb_pkey PRIMARY KEY (lga_16code);

drop table if exists temp_poa_mb;
create temporary table temp_poa_mb as
select temp_mb.mb_16code, bdy.poa_code16 as poa_16code, bdy.poa_name16 as poa_16name from temp_mb
inner join census_2016_bdys.poa_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_poa_mb;
ALTER TABLE temp_poa_mb ADD CONSTRAINT temp_poa_mb_pkey PRIMARY KEY (poa_16code);

drop table if exists temp_ra_mb;
create temporary table temp_ra_mb as
select temp_mb.mb_16code, bdy.ra_code16 as ra_16code, bdy.ra_name16 as ra_16name from temp_mb
inner join census_2016_bdys.ra_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ra_mb;
ALTER TABLE temp_ra_mb ADD CONSTRAINT temp_ra_mb_pkey PRIMARY KEY (ra_16code);

drop table if exists temp_ucl_mb;
create temporary table temp_ucl_mb as
select temp_mb.mb_16code, bdy.ucl_code16 as ucl_16code, bdy.ucl_name16 as ucl_16name from temp_mb
inner join census_2016_bdys.ucl_2016_aust as bdy on st_intersects(temp_mb.geom, bdy.geom);
analyse temp_ucl_mb;
ALTER TABLE temp_ucl_mb ADD CONSTRAINT temp_ucl_mb_pkey PRIMARY KEY (ucl_16code);

drop table temp_mb;



drop table if exists gnaf_202202.address_principal_census_2016_boundaries;
create table gnaf_202202.address_principal_census_2016_boundaries as
with merge as (
    select gnaf_pid,
           reliability,
           mb.mb_16code,
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
           lga_16code,
           lga_16name,
           poa_16code,
           poa_16name,
           ra_16code,
           ra_16name,
           ucl_16code,
           ucl_16name,
           mb.state
    from gnaf_202202.address_principals as gnaf
        inner join admin_bdys_202202.abs_2016_mb as mb on mb.mb_16code = gnaf.mb_2016_code
        inner join temp_lga_mb as lga on lga.mb_16code = mb.mb_16code
        inner join temp_poa_mb as poa on poa.mb_16code = mb.mb_16code
        inner join temp_ra_mb as ra on ra.mb_16code = mb.mb_16code
        inner join temp_ucl_mb as ucl on ucl.mb_16code = mb.mb_16code
)
select *
from merge
;
analyse gnaf_202202.address_principal_census_2016_boundaries;


select * from gnaf_202202.address_principal_census_2016_boundaries
;


-- select count(*) from gnaf_202202.address_principal_census_2016_boundaries; -- 14451352
--
-- select count(*) from admin_bdys_202202.abs_2016_mb; -- 358011
-- select count(*) from temp_lga_mb; -- 358011
-- select count(*) from temp_poa_mb; -- 358011
-- select count(*) from temp_ra_mb; -- 358011
-- select count(*) from temp_ucl_mb; -- 358011


drop table if exists temp_lga_mb;
drop table if exists temp_poa_mb;
drop table if exists temp_ra_mb;
drop table if exists temp_ucl_mb;

