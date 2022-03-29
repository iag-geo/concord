
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
), psma_lga as (
    select lga_pid,
           st_collect(geom) as geom
    from admin_bdys_202202.local_government_areas
    group by lga_pid
)
select final.*,
       st_intersection(abs_lga.geom, psma_lga.geom) as geom
from final
         inner join census_2016_bdys.lga_2016_aust as abs_lga on final.bdy1_id = abs_lga.lga_code16
         inner join psma_lga on final.bdy2_id = psma_lga.lga_pid
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
