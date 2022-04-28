
-- get counts of GNAFPIDS with and without buildings
with blg as (
    select address_detail_pid as gnaf_pid,
           coalesce(is_residential, 'unknown') as is_residential,
           count(*)           as building_count
    from geoscape_202203.building_address
    group by address_detail_pid,
             is_residential
), merge as (
    select gnaf.gnaf_pid,
           blg.gnaf_pid as blg_gnaf_pid,
           is_residential,
           building_count
    from gnaf_202202.address_principals as gnaf
    full outer join blg on blg.gnaf_pid = gnaf.gnaf_pid
)
select is_residential,
       sum(building_count) as building_count,
       sum(case when gnaf_pid is not null and blg_gnaf_pid is not null then 1 else 0 end) as match_count,
       sum(case when gnaf_pid is not null and blg_gnaf_pid is null then 1 else 0 end) as gnaf_only_count,
       sum(case when gnaf_pid is null and blg_gnaf_pid is not null then 1 else 0 end) as blg_gnaf_only_match_count
from merge
    group by is_residential
;


-- 202111 Geoscape Buildings
-- +--------------+--------------+-----------+---------------+-------------------------+
-- |is_residential|building_count|match_count|gnaf_only_count|blg_gnaf_only_match_count|
-- +--------------+--------------+-----------+---------------+-------------------------+
-- |NULL          |NULL          |0          |2860145        |0                        |
-- |Yes           |19493030      |9329794    |0              |4517                     |
-- |unknown       |6191638       |2261422    |0              |18021                    |
-- +--------------+--------------+-----------+---------------+-------------------------+

-- 202222 Geoscape Buildings
-- +--------------+--------------+-----------+---------------+-------------------------+
-- |is_residential|building_count|match_count|gnaf_only_count|blg_gnaf_only_match_count|
-- +--------------+--------------+-----------+---------------+-------------------------+
-- |NULL          |NULL          |0          |2778451        |0                        |
-- |Yes           |20585054      |9558707    |0              |3182                     |
-- |unknown       |6299707       |2114194    |0              |12517                    |
-- +--------------+--------------+-----------+---------------+-------------------------+


-- create table of gnaf points with building counts and planning zone data
drop table if exists geoscape_202203.address_principals_buildings;
create table geoscape_202203.address_principals_buildings as
with blg as (
    select adr.address_detail_pid as gnaf_pid,
           string_agg(distinct coalesce(lower(blgs.planning_zone), 'unknown'), ' - ') as planning_zone,
           count(*) as building_count
    from geoscape_202203.building_address as adr
    inner join geoscape_202203.buildings as blgs on blgs.building_pid = adr.building_pid
    group by adr.address_detail_pid
)
select gnaf.gnaf_pid,
       gnaf.reliability,
       gnaf.state,
       planning_zone,
       case when planning_zone LIKE '%residential%'
           or planning_zone LIKE '%mixed use%'
                then 'residential' end as is_residential,
       coalesce(building_count, 0) as building_count,
       gnaf.mb_2016_code,
       lower(mb16.mb_category) as mb_category_2016,
       gnaf.mb_2021_code,
       lower(mb21.mb_cat) as mb_category_2021,
       gnaf.geom
from gnaf_202202.address_principals as gnaf
     inner join admin_bdys_202202.abs_2016_mb as mb16 on mb16.mb_16code = gnaf.mb_2016_code
     inner join admin_bdys_202202.abs_2021_mb as mb21 on mb21.mb21_code = gnaf.mb_2021_code
    left outer join blg on blg.gnaf_pid = gnaf.gnaf_pid
;
analyse geoscape_202203.address_principals_buildings;


-- flag non-residential addresses that have a building -- 2,208,070 rows affected in 14 s 103 ms
update geoscape_202203.address_principals_buildings
    set is_residential = 'non-residential'
where is_residential is null
    and building_count > 0
    and planning_zone <> 'unknown'
;
analyse geoscape_202203.address_principals_buildings;


-- update addresses with no planning zone in residential MBs -- 1,956,429 rows affected in 8 s 741 ms
update geoscape_202203.address_principals_buildings as gnaf
    set is_residential = 'residential'
where is_residential is null
  and mb_category_2021 = 'residential'
;
analyse geoscape_202203.address_principals_buildings;


-- update addresses with no planning zone in non-residential MBs -- 1,956,429 rows affected in 8 s 741 ms
update geoscape_202203.address_principals_buildings as gnaf
set is_residential = 'non-residential'
where is_residential is null
  and mb_category_2021 <> 'residential'
;
analyse geoscape_202203.address_principals_buildings;


alter table geoscape_202203.address_principals_buildings add constraint address_principals_buildings_pkey primary key (gnaf_pid);
create index address_principals_buildings_geom_idx on geoscape_202203.address_principals_buildings using gist (geom);
alter table geoscape_202203.address_principals_buildings cluster on address_principals_buildings_geom_idx;


-- -- compare planning_zone with meshblock category -- 14,451,346 rows affected in 46 s 513 ms
-- drop table if exists testing.temp_address_principals_buildings;
-- create table testing.temp_address_principals_buildings as
-- select gnaf1.gnaf_pid,
--        gnaf1.is_residential,
--        gnaf1.planning_zone as planning_zone,
--        lower(gnaf2.mb_category) as mb_category_2016,
--        gnaf1.mb_2016_code,
--        lower(gnaf3.mb_category_2021) as mb_category_2021,
--        gnaf1.mb_2021_code
-- from geoscape_202203.address_principals_buildings as gnaf1
-- inner join gnaf_202202.address_principal_census_2016_boundaries as gnaf2 on gnaf2.gnaf_pid = gnaf1.gnaf_pid
-- inner join gnaf_202202.address_principal_census_2021_boundaries as gnaf3 on gnaf3.gnaf_pid = gnaf1.gnaf_pid
-- -- where gnaf1.is_residential = lower(gnaf2.mb_category)
-- ;
-- analyse testing.temp_address_principals_buildings;


-- select *
-- from testing.temp_address_principals_buildings;


select count(*) as address_count,
       count(distinct mb_2021_code) as mb_count
--        count(distinct mb_code_2021) as mb_count
from geoscape_202203.address_principals_buildings
where is_residential <> mb_category_2021
-- where is_residential <> mb_category_2021
and is_residential = 'residential'
;




-- ABS 2016 Meshblocks
-- +-------------+--------+
-- |address_count|mb_count|
-- +-------------+--------+
-- |2635619      |112689  |
-- +-------------+--------+


-- ABS 2021 Meshblocks
-- +-------------+--------+
-- |address_count|mb_count|
-- +-------------+--------+
-- |191324       |18496   |
-- +-------------+--------+



select reliability,
       count(*) as address_count
from geoscape_202203.address_principals_buildings
where building_count = 0
group by reliability
order by reliability
;

select is_residential,
       count(*) as address_count
from geoscape_202203.address_principals_buildings
group by is_residential
order by is_residential
;

select is_residential,
       mb_category_2021,
       count(*) as address_count
from geoscape_202203.address_principals_buildings
group by is_residential,
         mb_category_2021
order by is_residential,
         mb_category_2021
;


select *
from geoscape_202203.address_principals_buildings
where planning_zone = 'unknown'
;


