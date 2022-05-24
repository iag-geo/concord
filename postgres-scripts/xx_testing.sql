
select count(*)
from admin_bdys_202205.abs_2021_sa2;


select *
from gnaf_202205.boundary_concordance
where from_bdy = 'postcode'
and to_bdy = 'poa';


select *
from admin_bdys_202205.abs_2016_sa1;




-- test getting centroids for bdy overlaps
select *,
       st_centroid(st_intersection(from_bdy.geom, to_bdy.geom)) as geom
from census_2016_bdys.sa2_2016_aust as from_bdy
         inner join census_2016_bdys.lga_2016_aust as to_bdy on st_intersects(from_bdy.geom, to_bdy.geom)
;

