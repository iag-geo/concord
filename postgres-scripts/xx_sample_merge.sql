


with pc as (select region_id as bdy_id, g3 as pop
            from census_2016_data.poa_g01),
    lga as (select region_id as bdy_id, g3 as pop
             from census_2016_data.lga_g01)
select * from gnaf_202202.boundary_concordance as bdy
inner join pc on pc.bdy_id = bdy.from_id
inner join lga on lga.bdy_id = bdy.to_id
;









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

