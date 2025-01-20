
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





-- -- project current population from changes to address counts since 201708 and test against 2021 LGA population data
--
-- FAR TOO INACCURATE
--
-- -- step 1 - add 201708 and 220202 address counts to 2016 meshblocks
-- drop table if exists testing.mb_2016_counts_2022;
-- create table testing.mb_2016_counts_2022 as
-- with gnaf_2017 as (
--     select mb_2016_code,
--            count(*) as address_count
--     from gnaf_201708.address_principals
--     group by mb_2016_code
-- ), gnaf_2022 as (
--     select mb_2016_code,
--            count(*) as address_count
--     from gnaf_202502.address_principals
--     group by mb_2016_code
-- )
-- select mb.mb_2016_code,
--        mb_category_name_2016,
--        area_albers_sqkm,
--        dwelling,
--        person,
--        mb.address_count,
--        coalesce(gnaf_2017.address_count, 0) as  address_count_201708,
--        coalesce(gnaf_2022.address_count, 0) as  address_count_202205,
--        0::integer as person_202205,
--        state,
--        geom
-- from testing.mb_2016_counts as mb
-- left outer join gnaf_2017 on gnaf_2017.mb_2016_code = mb.mb_2016_code
-- left outer join gnaf_2022 on gnaf_2022.mb_2016_code = mb.mb_2016_code
-- ;
-- analyse testing.mb_2016_counts_2022;
--
-- -- addd projected population
-- update testing.mb_2016_counts_2022
--     set person_202205 = ((person::float / address_count_201708::float) * address_count_202205)::integer
-- where address_count_201708 > 0
-- ;
-- analyse testing.mb_2016_counts_2022;
--
-- ALTER TABLE testing.mb_2016_counts_2022 ADD CONSTRAINT mb_2016_counts_2022_pkey PRIMARY KEY (mb_2016_code);
-- create index mb_2016_counts_2022_geom_idx on testing.mb_2016_counts_2022 using gist (geom);
-- alter table testing.mb_2016_counts_2022 cluster on mb_2016_counts_2022_geom_idx;


-- aggregate meshbocks by ABS LGA (using centroid)
drop table if exists testing.lga_pop;
create table testing.lga_pop as
with mb as (
    select mb_2016_code,
           person,
           person_202205,
           ST_PointOnSurface(geom)         as geom
    from testing.mb_2016_counts_2022
)
select lga_code16,
       lga_name16,
       ste_code16,
       sum(person) as person,
       sum(person_202205) as person_202205
from mb
inner join census_2016_bdys.lga_2016_aust as abs_lga on st_intersects(mb.geom, abs_lga.geom)
group by lga_code16,
         lga_name16,
         ste_code16

;
analyse testing.lga_pop;


select *
from testing.lga_pop
order by ste_code16,
         lga_name16
;





-- get all combinations of PSMA and ABS 2016 LGAs using centroids -- 562 rows affected in 3 m 0 s 850 ms
drop table if exists testing.lga_ids;
create table testing.lga_ids as
with psma_lga as (
    select lga_pid,
           name as psma_name,
           st_collect(geom) as geom
    from admin_bdys_202502.local_government_areas
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
analyse testing.lga_ids;




-- different names -- 54 rows
select *
from testing.lga_ids
where psma_name <> lga_ids.lga_name16
;

-- only a one-way match -- 33 rows
select *
from testing.lga_ids
where match_count < 2
;






-- works in progress.....

-- get all combinations of PSMA and ABS 2016 LGAs using centroids -- 562 rows affected in 3 m 0 s 850 ms
drop table if exists testing.lga_ids_by_area;
create table testing.lga_ids_by_area as
with psma_lga as (
    select lga_pid,
           name as psma_name,
           sum(st_area(st_transform(geom, 3577))) as area_m2
    from admin_bdys_202502.local_government_areas
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
analyse testing.lga_ids_by_area;


