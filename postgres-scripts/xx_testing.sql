
select count(*)
from admin_bdys_202205.abs_2021_sa2;


select *
from gnaf_202205.boundary_concordance
where from_bdy = 'postcode'
and to_bdy = 'poa';


select *
from admin_bdys_202205.abs_2016_sa1;


select *
from gnaf_202205.address_principal_census_2016_boundaries;


select *
from census_2021_bdys.mb_2021_aust_gda94;


-- test getting centroids for bdy overlaps
select *,
       st_centroid(st_intersection(from_bdy.geom, to_bdy.geom)) as geom
from census_2016_bdys.sa2_2016_aust as from_bdy
         inner join census_2016_bdys.lga_2016_aust as to_bdy on st_intersects(from_bdy.geom, to_bdy.geom)
;


select *
from gnaf_202205;


select *
from admin_bdys_202205.abs_2016_mb as adm
full outer join census_2016_bdys.mb_2016_aust as abs on adm.mb_16code::text = abs.mb_code16
where not st_equals(adm.geom, abs.geom)
;


select *
from census_2016_bdys.poa_2016_aust
where poa_code16 in ('POA2050', 'POA2042')
;


select from_id as postcode,
       to_id as lga_id,
       to_name as lga_name,
       address_count,
       address_percent
from gnaf_202205.boundary_concordance
where from_bdy = 'postcode'
  and to_source = 'abs 2016'
  and to_bdy = 'lga'
  and from_id in ('2050', '2042')
;


-- test table for concordance preso
drop table if exists testing.temp_mb;
create table testing.temp_mb as
select mb.*
from admin_bdys_202205.abs_2021_mb as mb
inner join admin_bdys_202205.postcode_bdys as pc on st_intersects(mb.geom, pc.geom)
-- inner join admin_bdys_202205.postcode_bdys as pc on st_intersects(st_centroid(mb.geom), pc.geom)
    and postcode in ('2050', '2042');

