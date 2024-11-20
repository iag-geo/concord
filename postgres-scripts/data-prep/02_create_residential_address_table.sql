
-- this script require Geoscape Buildings - a licensed data product that needs to be purchased
-- https://geoscape.com.au/data/buildings/



-- create table of gnaf addresses with building counts and planning zone data to identify residential addresses
drop table if exists geoscape_202203.address_principals_buildings;
create table geoscape_202203.address_principals_buildings as
with blg as (
    select adr.address_detail_pid as gnaf_pid,
           string_agg(distinct coalesce(lower(blgs.planning_zone), 'unknown'), ' - ')::text as planning_zone,
           count(*)::smallint as building_count
    from geoscape_202203.building_address as adr
    inner join geoscape_202203.buildings as blgs on blgs.building_pid = adr.building_pid
    where adr.address_detail_pid is not null
    group by adr.address_detail_pid
)
select gnaf.gnaf_pid,
       gnaf.reliability,
       gnaf.state,
       planning_zone,
       null::boolean as is_residential,
       coalesce(building_count, 0) as building_count,
       gnaf.mb_2016_code,
       lower(mb16.mb_category) as mb_category_2016,
       gnaf.mb_2021_code,
       lower(mb21.mb_cat) as mb_category_2021,
       gnaf.geom
from gnaf_202411.address_principals as gnaf
     inner join admin_bdys_202411.abs_2016_mb as mb16 on mb16.mb_16code = gnaf.mb_2016_code
     inner join admin_bdys_202411.abs_2021_mb as mb21 on mb21.mb21_code = gnaf.mb_2021_code
    left outer join blg on blg.gnaf_pid = gnaf.gnaf_pid
;
analyse geoscape_202203.address_principals_buildings;

-- -- reset -- testing only
-- update geoscape_202203.address_principals_buildings
-- set is_residential = null
-- ;
-- analyse geoscape_202203.address_principals_buildings;

-- flag residential addresses based on planning zone -- 9,373,554 rows affected in 47 s 74 ms
update geoscape_202203.address_principals_buildings
set is_residential = true
where planning_zone like '%residential%'
--    or planning_zone like '%mixed use%'
;
analyse geoscape_202203.address_principals_buildings;

-- flag non-residential addresses that have a building -- 2,208,070 rows affected in 48 s 602 m
update geoscape_202203.address_principals_buildings
    set is_residential = false
where is_residential is null
    and building_count > 0
    and planning_zone <> 'unknown'
;
analyse geoscape_202203.address_principals_buildings;

-- update addresses with no planning zone in residential MBs -- 1,956,429 rows affected in 11 s 347 ms
update geoscape_202203.address_principals_buildings as gnaf
    set is_residential = true
where is_residential is null
  and mb_category_2021 = 'residential'
;
analyse geoscape_202203.address_principals_buildings;

-- update addresses with no planning zone in non-residential MBs -- 913,299 rows affected in 8 s 970 ms
update geoscape_202203.address_principals_buildings as gnaf
set is_residential = false
where is_residential is null
  and mb_category_2021 <> 'residential'
;
analyse geoscape_202203.address_principals_buildings;

-- add indexes
alter table geoscape_202203.address_principals_buildings add constraint address_principals_buildings_pkey primary key (gnaf_pid);
create index address_principals_buildings_geom_idx on geoscape_202203.address_principals_buildings using gist (geom);
alter table geoscape_202203.address_principals_buildings cluster on address_principals_buildings_geom_idx;
