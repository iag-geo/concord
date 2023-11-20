
-- create concordance table using addresses as a residential population proxy (yes, it's flawed but close)

-- step 1 of 2 -- get both boundary IDs for each GNAFPID -- 14,451,352 rows affected in 4 m 29 s 11 ms
-- If ABS LGAs or Postcodes - use meshblock centroids to assign addresses to ABS LGAs or Postcodes
drop table if exists temp_bdy_concordance;
create temporary table temp_bdy_concordance as
with mb as (
    select mb_2016_code,
           ST_PointOnSurface(geom) as geom
    from testing.mb_2016_counts_2022
), source as (
    select gnaf_pid,
           lga_code16                      as source_id,
           split_part(lga_name16, ' (', 1) as source_name,
           state                           as source_state
    from mb
    inner join census_2016_bdys.lga_2016_aust as abs_lga on st_intersects(mb.geom, abs_lga.geom)
    inner join gnaf_202311.address_principals as gnaf on gnaf.mb_2016_code = mb.mb_2016_code
)
select source.gnaf_pid,
       source_id,
       source_name,
       source_state,
       lga_pid as target_id,
       lga_name as target_name,
       state as target_state
from gnaf_202311.address_principal_admin_boundaries as psma
         inner join source on source.gnaf_pid = psma.gnaf_pid
;
analyse temp_bdy_concordance;



-- step 2 of 2 -- aggregate addresses and determine % overlap between all bdys.
-- This is the % that will be applied to all datasets being converted between bdys
drop table if exists testing.concordance;
create table testing.concordance as
with source_counts as (
    select source_id,
           count(*) as address_count
    from temp_bdy_concordance
    group by source_id
-- ), target_counts as (
--     select target_id,
--            count(*) as address_count
--     from temp_bdy_concordance
--     group by target_id
), agg as (
    select source_id,
           source_name,
           source_state,
           target_id,
           target_name,
           target_state,
           count(*) as address_count
    from temp_bdy_concordance
    group by source_id,
             source_name,
             source_state,
             target_id,
             target_name,
             target_state
), final as (
    select 'abs lga'::text as source_type,
           agg.source_id,
           agg.source_name,
           agg.source_state,
           'geoscape lga'::text as target_type,
           agg.target_id,
           agg.target_name,
           agg.target_state,
           agg.address_count,
           source_counts.address_count                                                        as total_source_addresses,
           (agg.address_count::float / source_counts.address_count::float * 100.0)::smallint  as percent_source_addresses
--            target_counts.address_count                                                       as total_target_addresses,
--            (agg.address_count::float / target_counts.address_count::float * 100.0)::smallint as percent_target_addresses
    from agg
             inner join source_counts on source_counts.source_id = agg.source_id
--              inner join target_counts on target_counts.target_id = agg.target_id
), target as (
    select lga_pid,
           st_collect(geom) as geom
    from admin_bdys_202311.local_government_areas
    group by lga_pid
)
select final.*,
       st_intersection(source.geom, target.geom) as geom
from final
         inner join census_2016_bdys.lga_2016_aust as source on final.source_id = source.lga_code16
         inner join target on final.target_id = target.lga_pid
where percent_source_addresses > 0
--    or percent_target_addresses > 0
;
analyse testing.concordance;

ALTER TABLE testing.concordance ADD CONSTRAINT concordance_pkey PRIMARY KEY (source_id, target_id);
create index concordance_geom_idx on testing.concordance using gist (geom);
alter table testing.concordance cluster on concordance_geom_idx;

drop table if exists temp_bdy_concordance;


select count(*)
from testing.concordance;

select *
from testing.concordance
where target_id = 'lga7298de6f3112';
