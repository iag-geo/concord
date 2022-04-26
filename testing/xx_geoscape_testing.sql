




select count(*) from gnaf_202202.address_principals; -- 14,451,352




with blg as (
    select address_detail_pid as gnaf_pid,
           coalesce(is_residential, 'unknown') as is_residential,
           count(*)           as building_count
    from geoscape_202111.building_address
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

-- +--------------+--------------+-----------+---------------+-------------------------+
-- |is_residential|building_count|match_count|gnaf_only_count|blg_gnaf_only_match_count|
-- +--------------+--------------+-----------+---------------+-------------------------+
-- |NULL          |NULL          |0          |2860145        |0                        |
-- |Yes           |19493030      |9329794    |0              |4517                     |
-- |unknown       |6191638       |2261422    |0              |18021                    |
-- +--------------+--------------+-----------+---------------+-------------------------+



-- select is_residential,
--        count(*) as address_count,
--        sum(building_count) as building_count
--
-- from blg
-- group by is_residential;


select *
from geoscape_202111.building_cad;