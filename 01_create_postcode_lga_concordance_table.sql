
-- create concordance table using addresses as a residential population proxy (yes, it's flawed but close)


-- step 1 of 2 -- get both boundary IDs for each GNAFPID -- 14,451,352 rows affected in 38 s 631 ms
-- If ABS LGAs or Postcodes - use meshblock centroids to assign addresses to ABS LGAs or Postcodes
drop table if exists temp_bdy_concordance;
create temporary table temp_bdy_concordance as
with source as (
    select gnaf_pid,
           postcode as source_id,
           concat(state, ' ', postcode) as source_name,
           state    as source_state
    from gnaf_202202.address_principals as gnaf
)
select source.gnaf_pid,
       source_id,
       source_name,
       source_state,
       lga_pid as target_id,
       lga_name as target_name,
       state as target_state
from gnaf_202202.address_principal_admin_boundaries as psma
         inner join source on source.gnaf_pid = psma.gnaf_pid
;
analyse temp_bdy_concordance;



select count(*),
       locality_name,
       postcode,
       state
from gnaf_202202.address_principal_admin_boundaries
where lga_pid is null
group by locality_name,
       postcode,
       state
order by locality_name,
   postcode,
   state
;



-- step 2 of 2 -- aggregate addresses and determine % overlap between all bdys.
-- This is the % that will be applied to all datasets being converted between bdys
-- drop table if exists testing.concordance;
-- create table testing.concordance as
insert into testing.concordance
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
    select 'geoscape postcode'::text as source_type,
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
--              inner join target_counts on target_counts.target_id = lga.target_id
-- ), source_bdys as (
--     select postcode,
--            st_collect(geom) as geom
--     from admin_bdys_202202.postcode_bdys
--     where postcode is not null
--     group by postcode
-- ), target_bdys as (
--     select lga_pid,
--            st_collect(geom) as geom
--     from admin_bdys_202202.local_government_areas_analysis
--     group by lga_pid
)
select final.*,
       null::geometry(polygon, 4283) as geom
--        st_intersection(source_bdys.geom, target_bdys.geom) as geom
from final
--          inner join source_bdys on final.source_id = source_bdys.postcode
--          inner join target_bdys on final.target_id = target_bdys.lga_pid
where percent_source_addresses > 0
and target_id is null
--    or percent_target_addresses > 0
;
analyse testing.concordance;

-- ALTER TABLE testing.concordance ADD CONSTRAINT concordance_pkey PRIMARY KEY (source_id, target_id);
-- create index concordance_geom_idx on testing.concordance using gist (geom);
-- alter table testing.concordance cluster on concordance_geom_idx;

-- drop table if exists temp_bdy_concordance;


select count(*)
from testing.concordance;

select *
from testing.concordance
where target_id = 'lga7298de6f3112';
