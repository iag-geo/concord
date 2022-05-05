
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

with from_bdy as (
    select con.to_id,
           con.to_name,
           sum(pc.cases::float * con.address_percent / 100.0)::integer as cases
    from testing.nsw_covid_cases_20220503_postcode as pc
             inner join gnaf_202202.boundary_concordance as con on pc.postcode = con.from_id
    where con.from_source = 'geoscape 202202'
        and con.from_bdy = 'postcode'
        and con.to_source = 'abs 2016'
        and con.to_bdy = 'lga'
    group by con.to_id,
             con.to_name
)
select from_bdy.to_id,
       from_bdy.to_name,
       lga.tests,
       from_bdy.cases,
       (from_bdy.cases::float / lga.tests::float * 100.0)::numeric(5,1) as infection_rate_percent
from testing.nsw_covid_tests_20220503_lga as lga
inner join from_bdy on from_bdy.to_id = concat('LGA', lga.lga_code19)  -- need to prefix IDs to match ABS IDs
;
