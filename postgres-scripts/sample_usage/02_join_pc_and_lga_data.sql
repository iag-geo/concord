
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
--      - Also, 6 LGAs are not in the results due to the age of the ABS Census 2016 LGAs used.

WITH from_bdy AS (
    SELECT con.to_id,
           con.to_name,
           sum(pc.cases::float * con.address_percent / 100.0)::integer AS cases
    FROM testing.nsw_covid_cases_20220503_postcode AS pc
    INNER JOIN gnaf_202202.boundary_concordance AS con ON pc.postcode = con.from_id
    WHERE con.from_source = 'geoscape 202202'
      AND con.from_bdy = 'postcode'
      AND con.to_source = 'abs 2016'
      AND con.to_bdy = 'lga'
    GROUP BY con.to_id,
             con.to_name
)
SELECT from_bdy.to_id AS lga_id,
       from_bdy.to_name AS lga_name,
       lga.tests,
       from_bdy.cases,
       (from_bdy.cases::float / lga.tests::float * 100.0)::numeric(4,1) AS infection_rate_percent
FROM testing.nsw_covid_tests_20220503_lga AS lga
INNER JOIN from_bdy on from_bdy.to_id = concat('LGA', lga.lga_code19);  -- note: NSW Covid data is missing LGA prefix in IDs
