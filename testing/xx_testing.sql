

select from_id,
       sum(address_count::float * address_percent) as weighted_address_count,
       sum(address_count) as address_count,
       sum(address_count::float * address_percent)  / sum(address_count)::float as weighted_percent
from testing.census_2016_bdy_concordance
where from_type = 'poa'
  and to_type = 'lga'
group by from_id
;

with cnt as (
    select from_type,
           from_id,
           to_type,
           sum(address_count::float * address_percent) as weighted_address_count,
           sum(address_count) as address_count
    from testing.census_2016_bdy_concordance
    group by from_type,
             from_id,
             to_type
)
select from_type,
       to_type,
       (sum(weighted_address_count) / sum(address_count)::float)::smallint as concordance_percent
from cnt
group by from_type,
         to_type
;


select * from testing.boundary_concordance as con
where from_type = 'sa2'
    and to_type = 'sa2'
    and address_percent < 100
order by from_id,
         address_percent
;

-- from_id,weighted_address_count,address_count
-- POA5172,149595.24688509462,2167

select * from testing.census_2016_bdy_concordance
where from_id = 'POA5172'
;






select bdy.state,
       bdy.lga_name,
       bdy.postcode,
       bdy.locality_name,
       count(bdy.gnaf_pid) as addr_count
from gnaf_202202.address_principal_admin_boundaries as bdy
         inner join gnaf_202202.address_principals as gnaf on gnaf.gnaf_pid = bdy.gnaf_pid
group by bdy.state,
         bdy.lga_name,
         bdy.postcode,
         bdy.locality_name
order by bdy.state,
         bdy.lga_name,
         bdy.postcode,
         bdy.locality_name,
         addr_count desc
;










select bdy.state,
       bdy.lga_name,
       bdy.postcode,
       bdy.locality_name,
       count(bdy.gnaf_pid) as addr_count
from gnaf_202202.address_principal_admin_boundaries as bdy
inner join gnaf_202202.address_principals as gnaf on gnaf.gnaf_pid = bdy.gnaf_pid
inner join admin_bdys_202202.abs_2016_mb as mb on mb.mb_16code = gnaf.mb_2016_code
where mb.mb_category = 'RESIDENTIAL'
group by bdy.state,
         bdy.lga_name,
         bdy.postcode,
         bdy.locality_name
order by bdy.state,
         bdy.lga_name,
         bdy.postcode,
         bdy.locality_name,
         addr_count desc
LIMIT 50
;



select *
from gnaf_202202.address_principals
;

select *
from gnaf_202202.address_alias_admin_boundaries
;

select *
from admin_bdys_202202.abs_2016_mb
;

select * from admin_bdys_202202.local_government_areas
order by name, state;



