
-- Usage example:
--    Purpose:
--      - Determine the rate of Covid 19 infection as a % of testing
--
--    Input data:
--      - Covid 19 cases by postcode
--      - Covid 19 test numbers by LGA
--
--    Output data:
--      - A dataset containing the rate of infection at the LGA level
--
--    IMPORTANT: the output data is for testing only!
--      - The rates of infection are incorrect as the data doesn't contain RAT test numbers, only PCR testing.
--      - Also, 6 LGAs are not in the results due to the age of the ABS Census 2016 LGAs used versus the more recent Covid data.
--
-- -- input tables
-- select * from testing.nsw_covid_cases_20220503_postcode;
-- select * from testing.nsw_covid_tests_20220503_lga;
--
-- -- concordance table
-- select * from gnaf_202402.boundary_concordance;

WITH pc_data AS (
    SELECT con.to_id AS lga_id,
           con.to_name AS lga_name,
           sum(pc.cases::float * con.address_percent / 100.0)::integer AS cases,
           sum(pc.cases) as qa_count,
           count(*) as postcode_count
    FROM testing.nsw_covid_cases_20220503_postcode AS pc
    INNER JOIN gnaf_202402.boundary_concordance AS con ON pc.postcode = con.from_id
    WHERE con.from_source = 'geoscape 202402'
      AND con.from_bdy = 'postcode'
      AND con.to_source = 'abs 2016'
      AND con.to_bdy = 'lga'
    GROUP BY lga_id,
             lga_name
)
SELECT pc_data.lga_id,
       pc_data.lga_name,
       lga.tests,
       pc_data.cases,
       (pc_data.cases::float / lga.tests::float * 100.0)::numeric(4,1) AS infection_rate_percent,
       (pc_data.cases::float / pc_data.qa_count::float * 100.0)::smallint as concordance_percent,
       pc_data.postcode_count
FROM testing.nsw_covid_tests_20220503_lga AS lga
INNER JOIN pc_data on pc_data.lga_id = lga.lga_code19;


-- example of poor concordance due to number of partial postcodes that intersect with one LGA
select *
from gnaf_202402.boundary_concordance
WHERE from_source = 'geoscape 202402'
  AND from_bdy = 'postcode'
  AND to_source = 'abs 2016'
  AND to_bdy = 'lga'
  and to_name = 'Dungog (A)';

