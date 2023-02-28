
-- drop table if exists testing.census_2016_bdy_concordance;
-- create table testing.census_2016_bdy_concordance
-- (
--     from_bdy       text not null,
--     from_id         text not null,
--     from_name       text not null,
--     to_bdy         text not null,
--     to_id           text not null,
--     to_name         text not null,
--     address_count   integer,
--     address_percent double precision
-- );
-- alter table testing.census_2016_bdy_concordance owner to postgres;
-- alter table testing.census_2016_bdy_concordance add constraint census_2016_bdy_concordance_pkey primary key (from_id, to_id);


-- create census_2016_bdy_concordances between 2 ABS boundary types, using addresses as a proxy for whatever your data is (it's flawed but usually close)

-- aggregate by both boundaries and determine % of addresses in each.
-- This is the % that will be applied to all datasets being converted between bdys
insert into testing.census_2016_bdy_concordance
with agg as (
    select poa_16code as from_id,
           poa_16code as from_name,
--            state as from_state,
           lga_16code as to_id,
           lga_16name as to_name,
--            state as to_state,
           count(*) as address_count
    from gnaf_202302.address_principal_census_2016_boundaries
    group by from_id,
             from_name,
--              from_state,
             to_id,
             to_name
--              to_state
), final as (
    select 'abs 2016 postcode'::text as from_bdy,
           agg.from_id,
           agg.from_name,
--            agg.from_state,
           'abs 2016 lga'::text as to_bdy,
           agg.to_id,
           agg.to_name,
--            agg.to_state,
           agg.address_count,
--            (sum(agg.address_count) over (partition by agg.from_id))::integer as total_from_addresses,
           (agg.address_count::float / (sum(agg.address_count) over (partition by agg.from_id))::float * 100.0) as percent_from_addresses
    from agg
)
select *
from final
where percent_from_addresses > 0
;
analyse testing.census_2016_bdy_concordance;


select count(*) as num_lgas,
       sum(percent_from_addresses) as percent_total,
       from_id,
       from_name
--        from_state
from testing.census_2016_bdy_concordance
group by from_id,
         from_name
--          from_state
order by percent_total desc
;


select *
from testing.census_2016_bdy_concordance
;

