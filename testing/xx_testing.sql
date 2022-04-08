







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



