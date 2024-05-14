
-- create concordance table using addresses as a residential population proxy (it's flawed but close)


-- step 1 of 2 -- get both boundary IDs for each GNAFPID -- 14,451,352 rows affected in 38 s 631 ms
-- If ABS LGAs or Postcodes - use meshblock centroids to assign addresses to ABS LGAs or Postcodes
drop table if exists temp_bdy_concordance;
create temporary table temp_bdy_concordance as
select source.gnaf_pid,
       source.postcode as source_id,
       concat(source.state, ' ', source.postcode) as source_name,
       source.state as source_state,
       target.lga_pid as target_id,
       target.lga_name as target_name,
       target.state as target_state
from gnaf_202405.address_principal_admin_boundaries as target
inner join gnaf_202405.address_principals as source on source.gnaf_pid = target.gnaf_pid
;
analyse temp_bdy_concordance;

-- manual fixes for addresses with no LGA (these are mostly valid nulls. e.g. ACT has no councils)

-- all of ACT -- 232,665 rows
update temp_bdy_concordance as tmp
set target_id = 'lgaact9999991',
    target_name = 'Unincorporated - ACT'
where target_state = 'ACT'
;

-- Specific localities
update temp_bdy_concordance as tmp
set target_id = 'lgaot9999991',
    target_name = 'Unincorporated - Norfolk Island'
from gnaf_202405.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'locc15e0d2d6f2a'
  and target_id is null;

update temp_bdy_concordance as tmp
set target_id = 'lgaot9999992',
    target_name = 'Unincorporated - Jervis Bay'
from gnaf_202405.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'loced195c315de9'
  and target_id is null;

update temp_bdy_concordance as tmp
set target_id = 'lgasa9999991',
    target_name = 'Unincorporated - Thistle Island'
from gnaf_202405.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = '250190776'
  and target_id is null;

-- 35 boatsheds in Hobart
update temp_bdy_concordance as tmp
set target_id = 'lgacbffb11990f2',
    target_name = 'Hobart City'
from gnaf_202405.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'loc0f7a581b85b7'
  and target_id is null;

-- slightly offshore points in SA
update temp_bdy_concordance as tmp
set target_id = 'lgaa8d127fa14e7',
    target_name = 'Ceduna'
from gnaf_202405.address_principal_admin_boundaries as psma
where psma.gnaf_pid = tmp.gnaf_pid
  and locality_pid = 'loccf8be9dcdacd'
  and target_id is null;

-- NSW/QLD border silliness
update temp_bdy_concordance as tmp
set target_id = 'lga7872e04f6637',
    target_name = 'Tenterfield'
from gnaf_202405.address_principal_admin_boundaries as psma
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
-- from temp_bdy_concordance as tmp, gnaf_202405.address_principal_admin_boundaries as psma
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


-- step 2 of 2 -- aggregate by both boundaries and determine % of addresses in each.
-- This is the % that will be applied to all datasets being converted between bdys
drop table if exists testing.concordance;
create table testing.concordance as
with agg as (
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
           (sum(agg.address_count) over (partition by agg.source_id))::integer as total_source_addresses,
           (agg.address_count::float / (sum(agg.address_count) over (partition by agg.source_id))::float * 100.0)::smallint as percent_source_addresses
    from agg
)
select *
from final
where percent_source_addresses > 0
;
analyse testing.concordance;

ALTER TABLE testing.concordance ADD CONSTRAINT concordance_pkey PRIMARY KEY (source_id, target_id);

-- drop table if exists temp_bdy_concordance;


select count(*)
from testing.concordance;


select *
from testing.concordance

;



select *
from testing.concordance
where source_id = '0822';
