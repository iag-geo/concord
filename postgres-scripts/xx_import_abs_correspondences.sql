
-- import ABS SA2 correspondence table

DROP TABLE IF EXISTS census_2021_bdys.correspondences_sa2;
CREATE TABLE census_2021_bdys.correspondences_sa2
(
    sa2_maincode_2016 text,
    sa2_name_2016 text,
    sa2_code_2021 text,
    sa2_name_2021 text,
    ratio_from_to double precision,
    indiv_to_region_qlty_indicator text,
    overall_quality_indicator text,
    bmos_null_flag smallint
) WITH (OIDS = FALSE);
ALTER TABLE census_2021_bdys.correspondences_sa2 OWNER to postgres;

-- one sample file
COPY census_2021_bdys.correspondences_sa2
    FROM '/Users/s57405/tmp/CG_SA2_2016_SA2_2021.csv' WITH (HEADER, DELIMITER ',', FORMAT CSV);

ANALYSE census_2021_bdys.correspondences_sa2;

CREATE INDEX correspondences_sa2_sa2_main_code_idx ON census_2021_bdys.correspondences_sa2 USING btree (sa2_maincode_2016);
