
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




-- create concordance file using addresses as a residential population proxy (yes, it's flawed but close)

-- step 1 of 2 -- get ABS and PSMA LGA IDs for ech GNAFPID -- 14,451,352 rows affected in 4 m 29 s 11 ms
drop table if exists testing.temp_lga_concordance;
create table testing.temp_lga_concordance as
with mb as (
    select mb_2016_code,
           ST_PointOnSurface(geom)         as geom
    from testing.mb_2016_counts_2022
), abs as (
    select gnaf_pid,
           lga_code16,
           state as abs_state,
           split_part(lga_name16, ' (', 1) as lga_name16
    from mb
    inner join census_2016_bdys.lga_2016_aust as abs_lga on st_intersects(mb.geom, abs_lga.geom)
    inner join gnaf_202202.address_principals as gnaf on gnaf.mb_2016_code = mb.mb_2016_code
)
select abs.gnaf_pid,
       lga_code16,
       lga_name16,
       abs_state,
       lga_pid,
       lga_name,
       state
from gnaf_202202.address_principal_admin_boundaries as psma
inner join abs on abs.gnaf_pid = psma.gnaf_pid
-- limit 100000
;
analyse testing.temp_lga_concordance;

-- select *
-- from testing.temp_lga_concordance
-- where lga_pid is not null;

-- step 2 of 2 -- aggregate addresses and determine % overlap between all bdys.
--   This is the % that will be applied to all datasets being converted between bdys
drop table if exists testing.concordance;
create table testing.concordance as
with abs_counts as (
    select lga_code16,
           count(*) as address_count
    from testing.temp_lga_concordance
    group by lga_code16
), psma_counts as (
    select lga_pid,
           count(*) as address_count
    from testing.temp_lga_concordance
    group by lga_pid
), lga as (
    select lga_code16,
           lga_name16,
           abs_state,
           lga_pid,
           lga_name,
           state,
           count(*) as address_count
    from testing.temp_lga_concordance
--     where lga_name16 = 'Ballina'
    group by lga_code16,
             lga_name16,
             abs_state,
             lga_pid,
             lga_name,
             state
), final as (
    select 'abs lga'::text as bdy1_type,
           lga.lga_code16 as bdy1_id,
           lga_name16 as bdy1_name,
           abs_state as bdy1_state,
           'geoscape lga'::text as bdy2_type,
           lga.lga_pid as bdy2_id,
           lga_name as bdy2_name,
           state as bdy2_state,
           lga.address_count,
           abs_counts.address_count                                                        as total_bdy1_addresses,
           (lga.address_count::float / abs_counts.address_count::float * 100.0)::smallint  as percent_bdy1_addresses,
           psma_counts.address_count                                                       as total_bdy2_addresses,
           (lga.address_count::float / psma_counts.address_count::float * 100.0)::smallint as percent_bdy2_addresses
    from lga
             inner join abs_counts on abs_counts.lga_code16 = lga.lga_code16
             inner join psma_counts on psma_counts.lga_pid = lga.lga_pid
)
select final.*,
       st_intersection(abs_lga.geom, psma_lga.geom) as geom
from final
inner join census_2016_bdys.lga_2016_aust as abs_lga on final.bdy1_id = abs_lga.lga_code16
inner join admin_bdys_202202.local_government_areas as psma_lga on final.bdy2_id = psma_lga.lga_pid
where percent_bdy1_addresses > 0
   or percent_bdy2_addresses > 0
-- where (percent_abs_addresses > 0 and percent_abs_addresses < 100)
--       or (percent_psma_addresses > 0 and percent_psma_addresses < 100)
;
analyse testing.concordance;

ALTER TABLE testing.concordance ADD CONSTRAINT concordance_pkey PRIMARY KEY (bdy1_id, bdy2_id);
create index concordance_geom_idx on testing.concordance using gist (geom);
alter table testing.concordance cluster on concordance_geom_idx;

select count(*)
from testing.concordance;


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
--     from gnaf_202202.address_principals
--     group by mb_2016_code
-- )
-- select mb.mb_2016_code,
--        mb_category_name_2016,
--        area_albers_sqkm,
--        dwelling,
--        person,
--        mb.address_count,
--        coalesce(gnaf_2017.address_count, 0) as  address_count_201708,
--        coalesce(gnaf_2022.address_count, 0) as  address_count_202202,
--        0::integer as person_202202,
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
--     set person_202202 = ((person::float / address_count_201708::float) * address_count_202202)::integer
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
           person_202202,
           ST_PointOnSurface(geom)         as geom
    from testing.mb_2016_counts_2022
)
select lga_code16,
       lga_name16,
       ste_code16,
       sum(person) as person,
       sum(person_202202) as person_202202
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
analyse testing.lga_ids_by_area;


