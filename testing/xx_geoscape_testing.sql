
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
    from gnaf_202502.address_principals as gnaf
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


