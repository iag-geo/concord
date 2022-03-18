
-- -- fix LGA and POA ID prefix errors
-- update census_2016_bdys.lga_2016_aust
--     set lga_code16 = substring(lga_code16, 4, length(lga_code16))
-- ;
-- vacuum analyse census_2016_bdys.lga_2016_aust;
-- 
-- update census_2016_bdys.poa_2016_aust
-- set poa_code16 = substring(poa_code16, 4, length(poa_code16))
-- ;
-- vacuum analyse census_2016_bdys.poa_2016_aust;

with psma_lga as (
    select lga_pid,
           name as psma_name,
           st_collect(geom) as geom
    from admin_bdys_202202.local_government_areas
    group by lga_pid,
             name
), psma_lga_pnt as (
    select lga_pid,
           psma_name,
           st_centroid(geom) as geom
    from psma_lga
), abs_lga_pnt as (
    select lga_code16,
           lga_name16 as abs_name,
           st_centroid(geom) as geom
    from census_2016_bdys.lga_2016_aust
), merge1 as (
    select 'ABS pnt in PSMA bdy'::text as match_type,
           lga_pid,
           psma_name,
           lga_code16,
           abs_name,
           abs_lga_pnt.geom
    from psma_lga
             inner join abs_lga_pnt on st_intersects(abs_lga_pnt.geom, psma_lga.geom)
-- ), merge2 as (
--     select 'PSMA pnt in ABS bdy'::text as match_type,
--            lga_pid,
--            psma_name,
--            lga_code16,
--            abs_name,
--            abs_lga_pnt.geom
--     from psma_lga
--              inner join abs_lga_pnt on st_intersection(abs_lga_pnt.geom, psma_lga.geom)
)
select *
from merge1
;


;




select *
from census_2016_bdys.lga_2016_aust;

