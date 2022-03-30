
-- create concordance table using addresses as a residential population proxy (yes, it's flawed but close)

-- step 1 of 2 -- get ABS and PSMA LGA IDs for ech GNAFPID -- 14,451,352 rows affected in 4 m 29 s 11 ms
-- use meshblock centroids to assign addresses to ABS LGAs
drop table if exists temp_lga_concordance;
create temporary table temp_lga_concordance as
with mb as (
    select mb_2016_code,
           ST_PointOnSurface(geom) as geom
    from testing.mb_2016_counts_2022
), bdy1 as (
    select gnaf_pid,
           lga_code16                      as bdy1_id,
           split_part(lga_name16, ' (', 1) as bdy1_name,
           state                           as bdy1_state
    from mb
    inner join census_2016_bdys.lga_2016_aust as abs_lga on st_intersects(mb.geom, abs_lga.geom)
    inner join gnaf_202202.address_principals as gnaf on gnaf.mb_2016_code = mb.mb_2016_code
)
select bdy1.gnaf_pid,
       bdy1_id,
       bdy1_name,
       bdy1_state,
       lga_pid as bdy2_id,
       lga_name as bdy2_name,
       state as bdy2_state
from gnaf_202202.address_principal_admin_boundaries as psma
         inner join bdy1 on bdy1.gnaf_pid = psma.gnaf_pid
;
analyse temp_lga_concordance;


-- step 2 of 2 -- aggregate addresses and determine % overlap between all bdys.
-- This is the % that will be applied to all datasets being converted between bdys
drop table if exists testing.concordance;
create table testing.concordance as
with bdy1_counts as (
    select bdy1_id,
           count(*) as address_count
    from temp_lga_concordance
    group by bdy1_id
), bdy2_counts as (
    select bdy2_id,
           count(*) as address_count
    from temp_lga_concordance
    group by bdy2_id
), lga as (
    select bdy1_id,
           bdy1_name,
           bdy1_state,
           bdy2_id,
           bdy2_name,
           bdy2_state,
           count(*) as address_count
    from temp_lga_concordance
    group by bdy1_id,
             bdy1_name,
             bdy1_state,
             bdy2_id,
             bdy2_name,
             bdy2_state
), final as (
    select 'abs lga'::text as bdy1_type,
           lga.bdy1_id,
           lga.bdy1_name,
           lga.bdy1_state,
           'geoscape lga'::text as bdy2_type,
           lga.bdy2_id,
           lga.bdy2_name,
           lga.bdy2_state,
           lga.address_count,
           bdy1_counts.address_count                                                        as total_bdy1_addresses,
           (lga.address_count::float / bdy1_counts.address_count::float * 100.0)::smallint  as percent_bdy1_addresses,
           bdy2_counts.address_count                                                       as total_bdy2_addresses,
           (lga.address_count::float / bdy2_counts.address_count::float * 100.0)::smallint as percent_bdy2_addresses
    from lga
             inner join bdy1_counts on bdy1_counts.bdy1_id = lga.bdy1_id
             inner join bdy2_counts on bdy2_counts.bdy2_id = lga.bdy2_id
), bdy2_lga as (
    select lga_pid,
           st_collect(geom) as geom
    from admin_bdys_202202.local_government_areas
    group by lga_pid
)
select final.*,
       st_intersection(bdy1_lga.geom, bdy2_lga.geom) as geom
from final
         inner join census_2016_bdys.lga_2016_aust as bdy1_lga on final.bdy1_id = bdy1_lga.lga_code16
         inner join bdy2_lga on final.bdy2_id = bdy2_lga.lga_pid
where percent_bdy1_addresses > 0
   or percent_bdy2_addresses > 0
-- where (percent_bdy1_addresses > 0 and percent_bdy1_addresses < 100)
--       or (percent_bdy2_addresses > 0 and percent_bdy2_addresses < 100)
;
analyse testing.concordance;

ALTER TABLE testing.concordance ADD CONSTRAINT concordance_pkey PRIMARY KEY (bdy1_id, bdy2_id);
create index concordance_geom_idx on testing.concordance using gist (geom);
alter table testing.concordance cluster on concordance_geom_idx;

drop table if exists temp_lga_concordance;

select count(*)
from testing.concordance;
