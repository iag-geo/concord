



with gnaf as (
    select address_detail_pid as gnaf_pid,
           is_residential,
           count(*)           as building_count
    from geoscape_202111.building_address
    group by address_detail_pid,
             is_residential
)
select is_residential,
       count(*) as addres_count,
       sum(building_count) as building_count

from gnaf
group by is_residential;
