
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

-- manual fixes

-- all of ACT -- 232,665 rows
update temp_bdy_concordance as tmp
set target_id = 'lgaact9999991',
    target_name = 'Unincorporated ACT'
where target_state = 'ACT'
;

-- Specific localities
update temp_bdy_concordance as tmp
set target_id = 'lgaot9999991',
    target_name = 'Unincorporated OT (Norfolk Island)'
from gnaf_202202.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'locc15e0d2d6f2a'
  and target_id is null;

update temp_bdy_concordance as tmp
set target_id = 'lgaot9999992',
    target_name = 'Unincorporated OT (Jervis Bay)'
from gnaf_202202.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'loced195c315de9'
  and target_id is null;

update temp_bdy_concordance as tmp
set target_id = 'lgasa9999991',
    target_name = 'Unincorporated SA (Thistle Island)'
from gnaf_202202.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = '250190776'
  and target_id is null;

-- 35 boatsheds in Hobart
update temp_bdy_concordance as tmp
set target_id = 'lgacbffb11990f2',
    target_name = 'Hobart City'
from gnaf_202202.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'loc0f7a581b85b7'
  and target_id is null;

-- slightly offshore points in SA
update temp_bdy_concordance as tmp
set target_id = 'lgaa8d127fa14e7',
    target_name = 'Ceduna'
from gnaf_202202.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'loccf8be9dcdacd'
  and target_id is null;


-- NSW/QLD border silliness
update temp_bdy_concordance as tmp
set target_id = 'lga7872e04f6637',
    target_name = 'Tenterfield'
from gnaf_202202.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'loc552bd3aef1b8'
  and target_id is null;


-- delete the ~150 records without an LGA - these are all offshore points, a number being oyster leases and boat moorings
delete from temp_bdy_concordance
where target_id is null;

analyse temp_bdy_concordance;

-- -- who's left
-- select count(*) as address_count,
--        locality_name,
--        postcode,
--        state
-- from temp_bdy_concordance as tmp, gnaf_202202.address_principal_admin_boundaries as psma
-- where psma.gnaf_pid = tmp.gnaf_pid
--     and target_id is null
-- group by locality_name,
--          postcode,
--          state
-- order by address_count desc,
--          locality_name,
--          postcode,
--          state
-- ;



-- step 2 of 2 -- aggregate addresses and determine % overlap between all bdys.
-- This is the % that will be applied to all datasets being converted between bdys
drop table if exists testing.concordance;
create table testing.concordance as
-- insert into testing.concordance
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
), source_bdys as (
    select postcode,
           st_collect(geom) as geom
    from admin_bdys_202202.postcode_bdys
    where postcode is not null
    group by postcode
), target_bdys as (
    select lga_pid,
           st_collect(geom) as geom
    from admin_bdys_202202.local_government_areas_analysis
    group by lga_pid
)
select final.*,
--        null::geometry(polygon, 4283) as geom,
       st_intersection(source_bdys.geom, target_bdys.geom) as geom
from final
         inner join source_bdys on final.source_id = source_bdys.postcode
         inner join target_bdys on final.target_id = target_bdys.lga_pid
where percent_source_addresses > 0
--    or percent_target_addresses > 0
;
analyse testing.concordance;

ALTER TABLE testing.concordance ADD CONSTRAINT concordance_pkey PRIMARY KEY (source_id, target_id);
create index concordance_geom_idx on testing.concordance using gist (geom);
alter table testing.concordance cluster on concordance_geom_idx;

-- drop table if exists temp_bdy_concordance;


select count(*)
from testing.concordance;



select *
from testing.concordance
where target_id = 'lga7298de6f3112';
