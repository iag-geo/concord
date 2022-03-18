
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




-- get all combinations of PSMA and ABS 2016 LGAs -- 562 rows affected in 3 m 0 s 850 ms
drop table if exists customer_growth.lga_ids;
create table customer_growth.lga_ids as
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
           ST_PointOnSurface(geom) as geom  -- use point on surface to ensure point is within polygon
    from psma_lga
), abs_lga_pnt as (
    select lga_code16,
           split_part(lga_name16, ' (', 1) as lga_name16,
           ST_PointOnSurface(geom) as geom
    from census_2016_bdys.lga_2016_aust
), merge1 as (
--     select 'ABS pnt in PSMA bdy'::text as match_type,
    select lga_pid,
           psma_name,
           lga_code16,
           lga_name16
--            abs_lga_pnt.geom
    from psma_lga
    inner join abs_lga_pnt on st_intersects(abs_lga_pnt.geom, psma_lga.geom)
), merge2 as (
--     select 'PSMA pnt in ABS bdy'::text as match_type,
    select lga_pid,
           psma_name,
           lga_code16,
           split_part(lga_name16, ' (', 1) as lga_name16
--            abs_lga_pnt.geom
    from psma_lga_pnt
    inner join census_2016_bdys.lga_2016_aust as abs_lga on st_intersects(psma_lga_pnt.geom, abs_lga.geom)
), uni as (
    select * from merge1
    union all
    select * from merge2
)
select lga_pid,
       psma_name,
       lga_code16,
       lga_name16,
       count(*) as match_count
from uni
group by lga_pid,
         psma_name,
         lga_code16,
         lga_name16
;
analyse customer_growth.lga_ids;




-- different names -- 54 rows
select *
from customer_growth.lga_ids
where psma_name <> lga_ids.lga_name16
;

-- only a one-way match -- 33 rows
select *
from customer_growth.lga_ids
where match_count < 2
;