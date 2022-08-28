# Concord

A [CSV file](https://minus34.com/opendata/geoscape-202208/boundary_concordance.csv) and supporting scripts for converting data between Australian boundaries.

It solves the problem of trying to merge 2 or more datasets based on different census or administrative boundaries such as statistical areas or postcodes.

It does this by providing a list of **_concordances_** between pairs of boundaries.  _e.g. In the image below: 100% of postcode 3126 fits within the Boroondara LGA. However, only ~46% of postcode 3127 fits within that LGA._

In this context, **_concordance_** describes what % of residential addresses in a "from" boundary fit within a "to" boundary.

Download & import the ~50Mb [concordance file](https://minus34.com/opendata/geoscape-202208/boundary_concordance.csv) into your database or reporting tool to [get started](#get-started). A [script](/postgres-scripts/00_import_concordance_file.sql) for importing into Postgres is also provided.

### Example Use Cases

- You have sales data by ABS Census 2016 SA2 boundaries & competitor data by local government area (LGA) and need to determine market share. Using the boundary concordance file, you convert the SA2 data to LGA and merge both datasets by LGA ID.
- You have Covid 19 cases by postcode & testing numbers by LGA and need to determine the rate of infection as a % of testing. You use the file to convert the postcode data to LGAs and merge both datasets by LGA ID.

### Limitations

Using this file comes with the following caveats:
- It only works with data related to residents, citizens & consumers. In other words - industrial, commercial & special use data isn't suited to conversion using the concordance file provided
- The data represents % overlaps between boundaries that are a _best estimate_ of how data should be apportioned between 2 boundary sets based on residential address counts. Your data may have biases in it that mean this approach doesn't return the best result. _e.g. looking at the image below - if your postcode 3127 customers were mostly on the Boroondara Council side - the boundary concordance file would incorrectly put 54% of them in Whitehorse Council_
- The code has been tested against the official ABS Census 2016 to 2021 boundary correspondences and the difference is: ~1% of addresses are in another boundary
    - **Note**: ABS Census 2016 to 2021 boundary conversions aren't in the initial version of the concordance file. Plan is to include these when the ABS Census 2021 data is released in June 2022
- ABS Census meshblock boundary concordances haven't been included as they blow the file out to over 200Mb (there are over 300,000 meshblock bdys). Plan is to provide these as a separate concordance file in the future.

![pc_vs_lga.png](pc_vs_lga.png "ABS 2016 Postcodes (red) vs LGAs (blue)")

<sup>Postcode 3127 split 46%-54% by the Boroondara & Whitehorse council boundary in blue (Credits: boundary data by Australian Bureau of Statistics, under CC BY 4.0; Map tiles by Carto, under CC BY 3.0. Data by OpenStreetMap, under ODbL)</sup>

****

## Accuracy

The list of boundary pairs in the file with their overall concordance & average error is in [the boundary concordance score CSV file](/data/boundary_concordance_score.csv).

A high overall concordance indicates your data can be reliability converted to the new boundary. The lower the concordance the more inaccurate the data conversion will be. Also, concordances are only reliable when going from a smaller boundary to a similar sized or larger one. Conversions from larger to smaller boundaries aren't supplied because of this.

In the [score](/data/boundary_concordance_score.csv) file, the **_error_** measures what proportion of data is placed in the wrong boundary when converting a statistic common to both boundaries, like population, from postcode to LGA (for example). _Note: error rates are only available for ABS 2016 from/to boundaries as the error check requires census data._

## Methodology

The concordance file is generated by the following process:

1. Tag all GNAF addresses with 2016 & 2021 ABS Census boundaries and geoscape 202208 Administrative boundaries
2. Remove all addresses in non-residential ABS Census 2021 meshblocks
3. Aggregate all residential addresses by a set of _**from**_ boundary and _**to**_ boundary pairs (e.g. postcode to LGA)
4. Determine the % overlap of residential addresses between both boundary types for all boundary pairs

****

## Get Started

### STEP 1 - Read the [Limitations](#Limitations)

..if you skipped them.

### STEP 2 - Get the Concordance File

There are 2 options to get the data:
1. Download and import the file 
2. Run `create_concordance_file.py` to create the Postgres table & CSV file yourself

#### 1. Download and Import

1. Download the [concordance file](https://minus34.com/opendata/geoscape-202208/boundary_concordance.csv)
2. Import it into your database/reporting tool of choice. If using Postgres:
    1. Edit the file path, schema name & table owner in `00_import_concordance_file.sql` in the [postgres-scripts](/postgres-scripts) folder
    2. Run the SQL script to import the file  

#### 2. Run the Python Script

This requires a knowledge of Python, Postgres & pg_restore.

BTW - if the boundary combination you want isn't in the default concordance file - you need to edit the `settings.py` file before running `create_concordance_file.py`. If this is too hard - raise an [issue](https://github.com/iag-geo/concord/issues) and we may be able to generate it for you; noting you shouldn't convert data to a smaller boundary due to the increase in data errors.

**Running the script only needs to be done for 3 reasons:**
1. The boundary from/to combination you need isn't in the standard [concordances file](https://minus34.com/opendata/geoscape-202208/boundary_concordance.csv)
2. It's now the future and we've been too lazy to update the concordances file with the latest boundary data from the ABS and/or Geoscape
3. You have a license of [Geoscape Buildings](https://geoscape.com.auhttps://minus34.com/opendata/geoscape-202208/boundary_concordance.csv/buildings/) or [Geoscape Land Parcels](https://geoscape.com.auhttps://minus34.com/opendata/geoscape-202208/boundary_concordance.csv/land-parcels/) and want to use the _planning zone_ data in those products to:
    1. Use a more accurate list of residential addresses to determine the data apportionment percentages (see **note** below); or
    2. Use a different set of addresses to apportion your data; e.g. industrial or commercial addresses

**Note:** The benefit of using Geoscape planning zone data over the default residential address filter (ABS Census 2021 meshblock categories) is limited due to ~2.3m addresses not having a planning zone, The code as-is fills this missing data with ABS Census 2021 meshblock categories.

#### Data Requirements

Running the script requires the following open data, available as Postgres dump files, as well as the optional licensed Geoscape data mentioned above:
1. ABS Census 2016 boundaries ([download](https://minus34.com/opendata/census-2016/census_2016_bdys.dmp))
2. ABS Census 2021 boundaries ([download](https://minus34.com/opendata/census-2021/census_2021_bdys_gda94.dmp))
3. GNAF from gnaf-loader ([download](https://minus34.com/opendata/geoscape-202208/gnaf-202208.dmp))
4. Geoscape Administrative Boundaries from gnaf-loader ([download](https://minus34.com/opendata/geoscape-202208/admin-bdys-202208.dmp))
5. ABS Census 2016 data - used to generate error rates only ([download](https://minus34.com/opendata/census-2016/census_2016_data.dmp))

#### Process

1. Download the above dump files and import them using `pg_restore`
2. OPTIONAL: If you have access to Geoscape Buildings or Land Parcels data:
    1. import it into Postgres
    2. Edit the `02_create_residential_address_table.sql` in the [postgres-scripts](/postgres-scripts/data-prep) folder to suit your dataset and schema name
    3. Run the above SQL script
3. Review & edit the boundaries to process in `settings.py` as required - make any required changes in the sections near the bottom marked for editing. If optionally using Geoscape Buildings data for residential address - change the `residential_address_source` to use it.
4. Add `psycopg2` to your Python 3.x environment
5. Run the script (takes ~30-45 mins on a 2017 MacBook Pro with 16Gb of RAM and 4 cores)

#### Command Line Options
The behaviour of the Python script can be controlled by specifying various command line options to the script. Supported arguments are:

##### Required Arguments
* `--output-path` local path where the boundary concordance files will be output.

##### Postgres Parameters
* `--pghost` the host name for the Postgres server. This defaults to the `PGHOST` environment variable if set, otherwise defaults to `localhost`.
* `--pgport` the port number for the Postgres server. This defaults to the `PGPORT` environment variable if set, otherwise `5432`.
* `--pgdb` the database name for Postgres server. This defaults to the `PGDATABASE` environment variable if set, otherwise `geoscape`.
* `--pguser` the username for accessing the Postgres server. This defaults to the `PGUSER` environment variable if set, otherwise `postgres`.
* `--pgpassword` password for accessing the Postgres server. This defaults to the `PGPASSWORD` environment variable if set, otherwise `password`.

##### Optional Arguments
* `--geoscape-version` Geoscape version number in YYYYMM format. Defaults to current year and last release month. e.g. `202208`.
* `--gnaf-schema` input schema name to store final GNAF tables in. Also the **output schema** for the concordance table. Defaults to `gnaf_<geoscape_version>`.
* `--admin-schema` input schema name to store final admin boundary tables in. Defaults to `admin_bdys_<geoscape_version>`.
* `--output-table` name of both output concordance table and file. Defaults to `boundary_concordance`.
* `--output-score_table` name of both output concordance QA table and file. Defaults to `<output_table>_score`.

#### Example Command Line Arguments
* Use the default GNAF & Admin Bdy data: `python create_concordance_file.py --output-path=~/tmp`
* Use a different version of GNAF & Admin Bdy data and a custom output table/file name: `python create_concordance_file.py --output-path=~/tmp --output-table="old_bdy_concordance" --admin-schema="admin_bdys_202111" --gnaf-schema="gnaf_202111"`

### STEP 3 - Use the Concordance File

After loading the file into your database/reporting tool of choice - you use it by creating a 3 (or more) table join between the datasets you want to merge & the concordance file/table.

#### Example Script

A Postgres SQL script (below) for the above-mentioned Covid 19 postcode to LGA example is included.

To run the example:
1. Edit the file path, schema name & table owner in `01_import_nsw_covid_data.sql` in the [postgres-scripts/example-usage](/postgres-scripts/example-usage) folder as required
2. Run the script to import the NSW Covid 19 data (supplied in the [data](/data) folder)
3. Edit the schema name & table owner in `02_join_pc_and_lga_data.sql` as required
4. Run the script

```sql
WITH pc_data AS (
   SELECT con.to_id AS lga_id,
          con.to_name AS lga_name,
          sum(pc.cases::float * con.address_percent / 100.0)::integer AS cases
   FROM testing.nsw_covid_cases_20220503_postcode AS pc
           INNER JOIN gnaf_202208.boundary_concordance AS con ON pc.postcode = con.from_id
   WHERE con.from_source = 'geoscape 202208'
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
       (pc_data.cases::float / lga.tests::float * 100.0)::numeric(4,1) AS infection_rate_percent
FROM testing.nsw_covid_tests_20220503_lga AS lga
        INNER JOIN pc_data on pc_data.lga_id = lga.lga_code19;
```
<sup>Example concordance file join to convert postcode level data to LGA and then join with LGA level data</sup>

## Data Licenses

Incorporates or developed using G-NAF © [Geoscape Australia](https://geoscape.com.au/legalhttps://minus34.com/opendata/geoscape-202208/boundary_concordance.csv-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under the [Open Geo-coded National Address File (G-NAF) End User Licence Agreement](https:/https://minus34.com/opendata/geoscape-202208/boundary_concordance.csv.gov.auhttps://minus34.com/opendata/geoscape-202208/boundary_concordance.csvset/ds-dga-19432f89-dc3a-4ef3-b943-5326ef1dbecc/distribution/dist-dga-09f74802-08b1-4214-a6ea-3591b2753d30/details?q=).

Incorporates or developed using Administrative Boundaries © [Geoscape Australia](https://geoscape.com.au/legalhttps://minus34.com/opendata/geoscape-202208/boundary_concordance.csv-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

Based on [Australian Bureau of Statistics](https://www.abs.gov.au/websitedbs/d3310114.nsf/Home/Attributing+ABS+Material) data, licensed by the Commonwealth of Australia under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

Covid 19 data © NSW Ministry of Health, licensed by the NSW Government under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
