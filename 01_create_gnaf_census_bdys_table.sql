
-- get all ABS Census boundaries for each GNAF address (GNAF only links directly to meshblocks, SAs & GCCs)

-- step 1 - create a bunch of temp tables merging meshblocks with non-ABS structures (LGA, RAs etc...)
-- use meshblock bdy centroids to get the bdy ID

drop table if exists temp_lga_mb;
create temporary table temp_lga_mb as
with mb as (select mb_16code, ST_PointOnSurface(geom) as geom from admin_bdys_202202.abs_2016_mb),
bdy as (select lga_code16, lga_name16, st_subdivide(geom, 512) as geom from census_2016_bdys.lga_2016_aust)
select distinct mb. mb_16code, bdy.lga_code16, bdy.lga_name16 from mb
inner join bdy on st_intersects(mb.geom, bdy.geom);
analyse temp_lga_mb;

drop table if exists temp_poa_mb;
create temporary table temp_poa_mb as
with mb as (select mb_16code, ST_PointOnSurface(geom) as geom from admin_bdys_202202.abs_2016_mb)
select mb. mb_16code, bdy.poa_code16, bdy.poa_name16 from mb
inner join census_2016_bdys.poa_2016_aust as bdy on st_intersects(mb.geom, bdy.geom);
analyse temp_poa_mb;

drop table if exists temp_ra_mb;
create temporary table temp_ra_mb as
with mb as (select mb_16code, ST_PointOnSurface(geom) as geom from admin_bdys_202202.abs_2016_mb)
select mb. mb_16code, bdy.ra_code16, bdy.ra_name16 from mb
inner join census_2016_bdys.ra_2016_aust as bdy on st_intersects(mb.geom, bdy.geom);
analyse temp_ra_mb;

drop table if exists temp_ucl_mb;
create temporary table temp_ucl_mb as
with mb as (select mb_16code, ST_PointOnSurface(geom) as geom from admin_bdys_202202.abs_2016_mb)
select mb. mb_16code, bdy.ucl_code16, bdy.ucl_name16 from mb
inner join census_2016_bdys.ucl_2016_aust as bdy on st_intersects(mb.geom, bdy.geom);
analyse temp_ucl_mb;


drop table if exists gnaf_202202.address_principal_census_2016_boundaries;
create table gnaf_202202.address_principal_census_2016_boundaries as
with merge as (
    select gnaf_pid,
           reliability,
           mb_16code,
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
           mb.state
    from gnaf_202202.address_principals as gnaf
             inner join admin_bdys_202202.abs_2016_mb as mb on mb.mb_16code = gnaf.mb_2016_code
)
select *
from merge
;
analyse gnaf_202202.address_principal_census_2016_boundaries;


select * from gnaf_202202.address_principal_census_2016_boundaries
;


drop table if exists temp_lga_mb;

