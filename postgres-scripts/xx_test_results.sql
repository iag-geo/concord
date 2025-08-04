

-- 592552 rows
select count(*)
from gnaf_202508.boundary_concordance;



-- compare ABS correspondences with address count correspondences
select count(*) as bdy_pair_count,
       sqrt(avg(power(cor.ratio_from_to * 100.0 - bdy.address_percent, 2)))::smallint  as rmse,
       avg(cor.ratio_from_to * 100.0 - bdy.address_percent)::smallint as mean_delta,
       min(cor.ratio_from_to * 100.0 - bdy.address_percent)::smallint as min_delta,
       max(cor.ratio_from_to * 100.0 - bdy.address_percent)::smallint as max_delta,
       (sum(abs(cor.ratio_from_to * 100.0 - bdy.address_percent) * address_count) / 100.0)::integer as address_count
from census_2021_bdys_gda94.correspondences_sa2 as cor
         inner join gnaf_202508.boundary_concordance as bdy on bdy.from_id = cor.sa2_maincode_2016
    and bdy.to_id = cor.sa2_code_2021
where abs(cor.ratio_from_to * 100.0 - bdy.address_percent) > 5.0
;

-- residential 2016 MB comparison
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |2348          |3   |0         |-40      |26       |82437        |
-- +--------------+----+----------+---------+---------+-------------+

-- residential 2021 MB comparison
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |2364          |4   |0         |-40      |41       |136828       |
-- +--------------+----+----------+---------+---------+-------------+

-- address level residential planning zone
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |2332          |5   |0         |-72      |66       |79503        |
-- +--------------+----+----------+---------+---------+-------------+

-- address level residential planning zone + residential 2016 MB where planning zone is null comparison
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |2374          |4   |0         |-50      |39       |93712        |
-- +--------------+----+----------+---------+---------+-------------+

-- address level residential planning zone + residential 2021 MB where planning zone is null comparison
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |2384          |4   |0         |-42      |42       |113946       |
-- +--------------+----+----------+---------+---------+-------------+



-- residential 2016 MB comparison (difference > 5%)
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |107           |12  |0         |-40      |26       |57988        |
-- +--------------+----+----------+---------+---------+-------------+

-- residential 2021 MB comparison (difference > 5%)
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |132           |17  |-1        |-40      |41       |111996       |
-- +--------------+----+----------+---------+---------+-------------+

-- address level residential planning zone (difference > 5%)
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |118           |21  |-2        |-72      |66       |62772        |
-- +--------------+----+----------+---------+---------+-------------+

-- address level residential planning zone + residential 2016 MB where planning zone is null comparison (difference > 5%)
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |132           |16  |0         |-50      |39       |75234        |
-- +--------------+----+----------+---------+---------+-------------+

-- address level residential planning zone + residential 2021 MB where planning zone is null comparison (difference > 5%)
-- +--------------+----+----------+---------+---------+-------------+
-- |bdy_pair_count|rmse|mean_delta|min_delta|max_delta|address_count|
-- +--------------+----+----------+---------+---------+-------------+
-- |151           |16  |0         |-42      |42       |95469        |
-- +--------------+----+----------+---------+---------+-------------+



with agg as (
    select sa2_16main::text as from_id,
           sa2_16name as from_name,
           sa2_code_2021::text as to_id,
           sa2_name_2021 as to_name,
           count(*) as address_count
    from gnaf_202508.address_principal_census_2016_boundaries as f
        inner join gnaf_202508.address_principal_census_2021_boundaries as t on t.gnaf_pid = f.gnaf_pid
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
-- inner join census_2021_bdys_gda94.sa2_2021_aust_gda94 as new on new.sa2_code_2021 = final.to_id
-- where percent > 0.0
;

