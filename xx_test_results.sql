
-- compare ABS correspondaences with address count correspondences
-- 161 bdy pairs different by more than 5%
select *
from census_2021_bdys.correspondences_sa2 as cor
inner join testing.boundary_concordance as bdy on bdy.from_id = cor.sa2_maincode_2016
    and bdy.to_id = cor.sa2_code_2021
where abs(cor.ratio_from_to * 100.0 - bdy.address_percent) > 5.0
;





with agg as (
    select sa2_16main as from_id,
           sa2_16name as from_name,
           sa2_code_2021 as to_id,
           sa2_name_2021 as to_name,
           count(*) as address_count
    from gnaf_202202.address_principal_census_2016_boundaries as f inner join gnaf_202202.address_principal_census_2021_boundaries as t on t.gnaf_pid = f.gnaf_pid
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
select * from final where percent > 0.0;





st_intersection(source.geom, target.geom) as geom