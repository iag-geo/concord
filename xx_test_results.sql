
-- compare ABS correspondences with address count correspondences
-- 161 bdy pairs different by more than 5%
select *
from census_2021_bdys.correspondences_sa2 as cor
inner join testing.boundary_concordance as bdy on bdy.from_id = cor.sa2_maincode_2016
    and bdy.to_id = cor.sa2_code_2021
where abs(cor.ratio_from_to * 100.0 - bdy.address_percent) > 5.0
;

-- comparison stats
select sqrt(avg(power(cor.ratio_from_to * 100.0 - bdy.address_percent, 2)))::smallint  as rmse,
       avg(cor.ratio_from_to * 100.0 - bdy.address_percent)::smallint as mean_delta,
       min(cor.ratio_from_to * 100.0 - bdy.address_percent)::smallint as min_delta,
       max(cor.ratio_from_to * 100.0 - bdy.address_percent)::smallint as max_delta
from census_2021_bdys.correspondences_sa2 as cor
         inner join testing.boundary_concordance as bdy on bdy.from_id = cor.sa2_maincode_2016
    and bdy.to_id = cor.sa2_code_2021
where abs(cor.ratio_from_to * 100.0 - bdy.address_percent) > 5.0
;

-- +------------------+-------------------+---------+------------------+
-- |rmse              |mean_delta         |min_delta|max_delta         |
-- +------------------+-------------------+---------+------------------+
-- |18.890779379336106|-0.6046173298343015|-99.99983|55.995065242929655|
-- +------------------+-------------------+---------+------------------+

-- +------------------+-------------------+-------------------+-----------------+
-- |rmse              |mean_delta         |min_delta          |max_delta        |
-- +------------------+-------------------+-------------------+-----------------+
-- |11.780807976984471|-0.3882958870770031|-40.161570000000005|26.46975494669509|
-- +------------------+-------------------+-------------------+-----------------+




select *
from gnaf_202202.address_principal_census_2016_boundaries;


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





