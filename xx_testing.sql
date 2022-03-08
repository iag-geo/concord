

select bdy.state,
       bdy.lga_name,
       bdy.postcode,
       bdy.locality_name,
       count(gnaf_pid) as addr_count
from gnaf_202202.address_alias_admin_boundaries as bdy
-- where  = 'RESIDENTIAL'
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



select *
from gnaf_202202.address_alias_admin_boundaries
;

