




# Concord

A [CSV file](https://minus34.com/opendata/geoscape-202202/boundary_concordance.csv) and supporting scripts for converting data between Australian boundaries.

It solves the problem of trying to merge 2 or more datasets based on different census or administrative boundaries such as statistical areas or postcodes.

Download & import the [concordance file](https://minus34.com/opendata/geoscape-202202/boundary_concordance.csv) into your database or reporting tool to get started. A [script](/postgres-scripts/00_import_concordance_file.sql) for importing into Postgres is also provided.

### Example Use Cases

- You have sales data by ABS Census 2016 SA2 boundaries & competitor data by local government area (LGA) and need to determine market share. Using the boundary concordance file, you convert the SA2 data to LGA and merge both datasets by LGA ID.
- You have Covid 19 cases by postcode & testing numbers by LGA and need to determine the rate of infection as a % of testing. You use the file to convert the postcode data to LGAs and merge both datasets by LGA ID.

### Limitations

Read this entire README file to understand the methodology used & it's accuracy.

Using this file comes with the following caveats:
- It only works with data related to residents, citizens & consumers. In other words - industrial, commercial & special use data isn't suited to conversion using the concordance file provided;
- The data represents % overlaps between boundaries that are a _best estimate_ of how data should be apportioned between 2 boundary sets based on residential address counts. Your data may have biases in it that mean this approach doesn't return the best result. e.g. looking at the image below - if your postcode 3127 customers were mostly on the Boroondara Council side - the boundary concordance file would incorrectly put 54% of customers in Whitehorse Council.

![pc_vs_lga.png](pc_vs_lga.png "ABS 2016 Postcodes (red) vs LGAs (blue)")

<sup>Postcode 3127 split 46%-54% by the Boroondara & Whitehorse council boundary in blue (Credits: boundary data by Australian Bureau of Statistics, under CC BY 4.0; Map tiles by Carto, under CC BY 3.0. Data by OpenStreetMap, under ODbL)</sup>

****

## Accuracy

Below are the average concordances between each boundary pair.

Concordance describes what % of residential addresses in the "from" boundary fit within a "to" boundary.

_e.g. As per the image above: 100% of postcode 3126 fits within the Boroondara LGA. However, only ~46% of postcode 3127 fits within that LGA._

A high average concordance indicates your data can be reliability converted to the new boundary. The lower the concordance the more inaccurate the data conversion will be.

Also, concordances are only reliable when going from a smaller boundary to a similar sized or larger one. Hence such conversions are not included in the supplied file. e.g. Going from LGAs to postcodes has a concordance of only 25% with an average error of ~14%.

The list of boundary pairs in the file with their concordances & average error rates is in [the boundary concordance score CSV file](/data/boundary_concordance_score.csv). Note: average error rates are only available for ABS 2016 from/to boundaries as the error check requires census population data.

## Methodology

The concordance file is generated by the following process:

1. Tag all GNAF addresses with 2016 & 2021 ABS Census boundaries and Geoscape 202202 Administrative boundaries
2. Remove all addresses in ABS Census 2021 meshblocks that are non-residential
3. Aggregate all residential addresses by a set of _**from**_ boundary and _**to**_ boundary pairs (e.g. postcode to LGA)
4. Determine the % overlap of residential addresses between both boundary types for all boundary pairs

****

## Get Started

### STEP 1 - Read the [Limitations](#Limitations)

### STEP 2 - Get the Concordance File

There are 2 options to get the data:
1. Download and import the file 
2. Run `01_create_concordance_file.py` to create the Postgres table & CSV file yourself

#### 1. Download and Import

1. Download the [concordance file](https://minus34.com/opendata/geoscape-202202/boundary_concordance.csv)
2. Import it into your database/reporting tool of choice. If using Postgres:
    1. Edit the file path, schema name & table owner in `00_import_concordance_file.sql` in the [postgres-scripts](/postgres-scripts) folder
    2. Run the SQL script to import the file  

#### 2. Run the Python Script

This requires a knowledge of Python, Postgres & pg_restore. The Python script doesn't currently take any arguments; input parameters are hardcoded and require edits to change.

If the boundary combination you want isn't in the default concordance file and running the script is too hard - raise an [issue](https://github.com/iag-geo/concord/issues) and we should be able to generate it fo you; noting you shouldn't convert data to a smaller boundary due to the increase in data errors.

Running the script yourself only needs to be done for 3 reasons:
1. The boundary from/to combination you need isn't in the standard [concordances file](https://minus34.com/opendata/geoscape-202202/boundary_concordance.csv)
2. We've been too lazy to update the concordances file with the latest boundary data from the ABS and/or Geoscape
3. You have a license of [Geoscape Buildings](https://geoscape.com.auhttps://minus34.com/opendata/geoscape-202202/boundary_concordance.csv/buildings/) or [Geoscape Land Parcels](https://geoscape.com.auhttps://minus34.com/opendata/geoscape-202202/boundary_concordance.csv/land-parcels/) and want to use the _planning zone_ data in those products to:
    1. Use a more accurate list of residential addresses to determine the data apportionment percentages (see note below); or
    2. Use a different set of addresses to apportion your data; e.g. industrial or commercial addresses

Running the script requires the following open data, available as Postgres dump files, as well as the optional licensed Geoscape data mentioned above:
1. ABS Census 2016 boundaries ([download](https://minus34.com/opendata/census-2016/census_2016_bdys.dmp))
2. ABS Census 2021 boundaries ([download](https://minus34.com/opendata/census-2021/census_2021_bdys.dmp))
3. GNAF from gnaf-loader ([download](https://minus34.com/opendata/geoscape-202202/gnaf-202202.dmp))
4. Geoscape Administrative Boundaries from gnaf-loader ([download](https://minus34.com/opendata/geoscape-202202/admin-bdys-202202.dmp))
5. ABS Census 2016 data - used to generate error rates only ([download](https://minus34.com/opendata/census-2016/census_2016_data.dmp))

##### 3. Process

1. Download the above dump files and import them using `pg_restore`
2. Edit `01a_create_gnaf_2016_census_bdy_table.sql` & `01b_create_gnaf_2021_census_bdy_table.sql` in the [postgres-scripts](/postgres-scripts) folder for your schema names & table owner
3. Run them to prep the census boundary tagged address tables
4. OPTIONAL: If you have access to Geoscape Buildings or Land Parcels data:
    1. import it into Postgres
    2. Edit the `02_create_residential_address_table.sql` in the [postgres-scripts](/postgres-scripts) folder to suit your dataset and schema name
    3. Run the above SQL script
5. Review & edit `01_create_concordance_file.py` as required - make any required changes in the sections marked for editing
6. Add `psycopg2` to your Python 3.x environment
7. Run the script

#### Note
The benefit of using Geoscape planning zone data over the default residential address filter (ABS Census 2021 meshblock categories) is limited due to ~2.3m addresses not having a planning zone, The code as-is fills this missing data with ABS Census 2021 meshblock categories.
 
### STEP 3 - Use the Concordance File

After loading the file into your database/reporting tool of choice - you use it by creating a 3 (or more) table join between the datasets you want to merge and the concordance file/table.

#### Example Script

A Postgres SQL script (below) for the abovementioned Covid 19 postcode to LGA example is included.

To run the example:
1. Edit the file path, schema name & table owner in `01_import_nsw_covid_data.sql` in the [postgres-scripts/example-usage](/postgres-scripts/example-usage) folder as required
2. Run the script to import the NSW Covid 19 data
3. Edit the schema name & table owner in `02_join_pc_and_lga_data.sql` as required
4. Run the script

```sql
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
INNER JOIN from_bdy on from_bdy.to_id = concat('LGA', lga.lga_code19); -- note: NSW Covid data is missing LGA prefix in IDs
```

## Data Licenses

Incorporates or developed using G-NAF © [Geoscape Australia](https://geoscape.com.au/legalhttps://minus34.com/opendata/geoscape-202202/boundary_concordance.csv-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under the [Open Geo-coded National Address File (G-NAF) End User Licence Agreement](https:/https://minus34.com/opendata/geoscape-202202/boundary_concordance.csv.gov.auhttps://minus34.com/opendata/geoscape-202202/boundary_concordance.csvset/ds-dga-19432f89-dc3a-4ef3-b943-5326ef1dbecc/distribution/dist-dga-09f74802-08b1-4214-a6ea-3591b2753d30/details?q=).

Incorporates or developed using Administrative Boundaries © [Geoscape Australia](https://geoscape.com.au/legalhttps://minus34.com/opendata/geoscape-202202/boundary_concordance.csv-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

Based on [Australian Bureau of Statistics](https://www.abs.gov.au/websitedbs/d3310114.nsf/Home/Attributing+ABS+Material) data, licensed by the Commonwealth of Australia under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

Test Covid 19 data © NSW Ministry of Health, licensed by the NSW Government under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
