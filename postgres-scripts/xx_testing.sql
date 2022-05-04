

select mb_16code,
       mb_category,
       sa1_16main as sa1_16code,
       sa1_16_7cd as sa1_16name,
       sa2_16main as sa2_16code,
       sa2_16name,
       sa3_16code,
       sa3_16name,
       sa4_16code,
       sa4_16name,
       gcc_16code,
       gcc_16name,
       state,
       area_sqm,
       mb16_pop,
       mb16_dwell
from admin_bdys_202202.abs_2016_mb
;



select 'abs 2016' as from_source,
       'mb' as from_bdy,
       mb_16code as from_id,
       mb_category as from_name,
       'abs 2016' as to_source,
       'sa1' as to_bdy,
       sa1_16main as to_id,
       sa1_16_7cd as to_name,
       count(*) as address_count,
       100.0 as address_percent
from admin_bdys_202202.abs_2016_mb as mb
inner join gnaf_202202.address_principals as gnaf on gnaf.mb_2016_code = mb.mb_16code
group by from_id,
         from_name,
         to_id,
         to_name
;



select 'abs 2021' as from_source,
       'mb' as from_bdy,
       mb21_code as from_id,
       mb_cat as from_name,
       'abs 2021' as to_source,
       'sa1' as to_bdy,
       sa1_21code as to_id,
       sa1_21pid as to_name,
       count(*) as address_count,
       100.0 as address_percent
from admin_bdys_202202.abs_2021_mb as mb
         inner join gnaf_202202.address_principals as gnaf on gnaf.mb_2021_code = mb.mb21_code
group by from_id,
         from_name,
         to_id,
         to_name
;





select from_source,
       from_bdy,
       from_id,
       from_name,
       to_source,
       to_bdy,
       to_id,
       to_name,
       address_count,
       address_percent
from gnaf_202202.boundary_concordance
;