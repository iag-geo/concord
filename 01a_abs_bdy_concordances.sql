
-- create concordances between 2 ABS boundary types, using addresses as a proxy for whatever your data is (it's flawed but usually close)

-- aggregate by both boundaries and determine % of addresses in each.
-- This is the % that will be applied to all datasets being converted between bdys
drop table if exists testing.concordance;
create table testing.concordance as
with agg as (
    select poa_16code as source_id,
           poa_16code as source_name,
           state as source_state,
           lga_16code as target_id,
           lga_16name as target_name,
           state as target_state,
           count(*) as address_count
    from gnaf_202202.address_principal_census_2016_boundaries
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
