
-- Usage example:
--    Purpose:
--      - You need to determine the rate of infection as a % of testing
--
--    Input data:
--      - Covid 19 cases by postcode
--      - Covid 19 test numbers by LGA
--
--    Output data:
--      - A dataset containing the rate of infection at the LGA level
--
--    NOTE:
--      - Don't use the output data! It's just a sample use case
--      - The rate of infection is incorrect as the data doesn't contain RAT test numbers, only PCR testing

with pc as (
    select con.to_id,
           con.to_name,
           con.to_source,
           sum(from_bdy.g3::float * con.address_percent / 100.0)::integer as population1
    from census_2016_data.lga_g01 as from_bdy
             inner join gnaf_202202.boundary_concordance as con on from_bdy.region_id = con.from_id
    where from_source = 'abs 2016'
        and from_bdy = 'lga'
        and to_source = 'abs 2016'
        and to_bdy = 'sa3'
    group by con.to_id,
             con.to_name,
             con.to_source
), merge as (
select to_id,
       to_name,
       to_source,
       population1,
       g3 as population2,
       g3 - population1 as pop_difference,
       (abs((g3 - population1) / g3) * 100.0)::smallint as pop_diff_percent
from census_2016_data.sa3_g01 as to_bdy
inner join pc on pc.to_id = to_bdy.region_id
)
select sum(population1) as population1,
       sum(population2) as population2,
       sum(abs(pop_difference)) as pop_difference,
       (sum(abs(pop_difference)) / sum(population2) * 100.0)::numeric(5, 1) as error_percent,
       sqrt(avg(power(population2 - population1, 2)))::smallint as rmse
from merge
;


-- ABS 2016 SA2 to LGA -- 97% concordance
-- +-----------+-----------+--------------+----------------------+----+
-- |population1|population2|pop_difference|pop_difference_percent|rmse|
-- +-----------+-----------+--------------+----------------------+----+
-- |23355559   |23355534   |122453        |0.5                   |510 |
-- +-----------+-----------+--------------+----------------------+----+

-- ABS 2016 SA2 to SA3 -- 100% concordance -- should be perfect -- TODO: confirm this is a rounding error
-- +-----------+-----------+--------------+----------------------+----+
-- |population1|population2|pop_difference|pop_difference_percent|rmse|
-- +-----------+-----------+--------------+----------------------+----+
-- |23355552   |23355479   |1545          |0.0                   |6   |
-- +-----------+-----------+--------------+----------------------+----+

-- ABS 2016 LGA to SA3 -- 73% concordance
-- +-----------+-----------+--------------+----------------------+----+
-- |population1|population2|pop_difference|pop_difference_percent|rmse|
-- +-----------+-----------+--------------+----------------------+----+
-- |23355538   |23355479   |1086411       |4.7                   |6901|
-- +-----------+-----------+--------------+----------------------+----+




-- ABS 2016 POA to LGA -- 94% concordance

-- abs 2016 used for residential addresses
-- +-----------+-----------+--------------+
-- |population1|population2|pop_difference|
-- +-----------+-----------+--------------+
-- |23308166   |23352358   |347972        |
-- +-----------+-----------+--------------+

-- abs 2021 used for residential addresses
-- +-----------+-----------+--------------+----------------------+----+
-- |population1|population2|pop_difference|pop_difference_percent|rmse|
-- +-----------+-----------+--------------+----------------------+----+
-- |23352385   |23355534   |322947        |1.4                   |1251|
-- +-----------+-----------+--------------+----------------------+----+

-- Geoscape planning zones with 2021 MBs
-- +-----------+-----------+--------------+
-- |population1|population2|pop_difference|
-- +-----------+-----------+--------------+
-- |23322667   |23352522   |378385        |
-- +-----------+-----------+--------------+




select *
from census_2016_data.metadata_stats
where sequential_id = 'G3'
    and long_id like '%pop%';



with agg as (
    select sa2_16main::text as from_id,
           sa2_16name as from_name,
           sa2_code_2021::text as to_id,
           sa2_name_2021 as to_name,
           count(*) as address_count
    from gnaf_202202.address_principal_census_2016_boundaries as f
        inner join gnaf_202202.address_principal_census_2021_boundaries as t on t.gnaf_pid = f.gnaf_pid
    where sa2_16main = '101021011'
      and mb_category = 'RESIDENTIAL'
      and mb_category_2021 = 'Residential'
    group by from_id,
             from_name,
             to_id,
             to_name
), final as (
    select 'abs 2016',
           'sa2',
           agg.from_id,
           agg.from_name,
           'abs 2021',
           'sa2',
           agg.to_id,
           agg.to_name,
           agg.address_count,
           (agg.address_count::float /
           (sum(agg.address_count) over (partition by agg.from_id))::float * 100.0) as percent
    from agg
)
select final.*
--        (st_area(st_intersection(st_transform(old.geom, 3577), st_transform(new.geom, 3577))) / 1000000.0) / old.areasqkm16 * 100.0 as percent_area
from final
-- inner join census_2016_bdys.sa2_2016_aust as old on old.sa2_main16 = final.from_id
-- inner join census_2021_bdys.sa2_2021_aust_gda94 as new on new.sa2_code_2021 = final.to_id
-- where percent > 0.0
;



select *
from gnaf_202202.boundary_concordance
where from_id ='POA3127';









with agg as (
    select f.sa2_16main::text as from_id,
           f.sa2_16name as from_name,
           t.sa2_code_2021::text as to_id,
           t.sa2_name_2021 as to_name,
           count(*) as address_count
    from gnaf_202202.address_principal_census_2016_boundaries as f
             inner join gnaf_202202.address_principal_census_2021_boundaries as t on t.gnaf_pid = f.gnaf_pid
    where f.sa2_16main is not null
      and t.sa2_code_2021 is not null
      and f.mb_category = 'RESIDENTIAL'
    group by from_id,
             from_name,
             to_id,
             to_name
), final as (
    select 'abs 2016',
           'sa2',
           agg.from_id,
           agg.from_name,
           'abs 2021',
           'sa2',
           agg.to_id,
           agg.to_name,
           agg.address_count,
           (agg.address_count::float /
           (sum(agg.address_count) over (partition by agg.from_id))::float * 100.0) as percent,
    from agg
)
select * from final where percent > 0.0;



-- test getting centroids for bdy overlaps
select *,
       st_centroid(st_intersection(from_bdy.geom, to_bdy.geom)) as geom
from census_2016_bdys.sa2_2016_aust as from_bdy
inner join census_2016_bdys.lga_2016_aust as to_bdy on st_intersects(from_bdy.geom, to_bdy.geom)
;

