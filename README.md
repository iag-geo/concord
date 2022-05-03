# Concord

A [CSV file](/data) and supporting scripts for converting data between any 2 Australian boundaries.

It solves the problem of trying to merge 2 or more datasets based on different census or administrative boundaries such as statistical areas or postcodes.

Download & import the [concordance file](/data) into your database or reporting tool to get started. A script for importing into Postgres is also provided.

### Example Use Cases

- You have sales data by postcode & competitor data by local government area (LGA) and need to determine market share. Using the [boundary concordance file](/data), you convert the postcode data to LGA and merge both datasets by LGA ID.
- You have cancer testing rates by ABS Census 2016 SA2 boundaries and cancer case numbers by LGA and need to determine the rate of disease as a % of testing. You use the [file](/data) to convert the SA2 data to LGAs and merge both datasets by LGA ID.

### Limitations

Read this entire README file to understand the methodology used & it's accuracy.

Using this file comes with the following caveats:
- Only works with data related to residents, citizens & consumers. In other words - industrial, commercial & special use data isn't suited to conversion using the concordance file provided;
- The % overlaps between boundaries are a best estimate of how data should be apportioned between 2 boundary sets based on residential address counts. Your data may have biases in it that mean this approach doesn't return the best result. e.g. looking at the image below - if your postcode 3127 customers were mostly in the Boroondara Council side - the boundary concordance file would incorrectly put 54% of customers in Whitehorse Council.

![pc_vs_lga.png](pc_vs_lga.png "ABS 2016 Postcodes (red) vs LGAs (blue)")

<sup>Postcode 3127 split 46%-54% by the Boroondara & Whitehorse council boundary in blue (Credits: boundary data by Australian Bureau of Statistics, under CC BY 4.0; Map tiles by Carto, under CC BY 3.0. Data by OpenStreetMap, under ODbL)</sup>

****

## Methodology

The concordance file is generated using this approach:

1. Tag all GNAF addresses with 2016 & 2021 ABS Census boundaries and Geoscape 202202 Administrative boundaries 
2. Remove all addresses in ABS Census 2021 meshblocks that are non-residential
3. Aggregate all residential address by the _**from**_ boundary and the _**to**_ boundary
4. Determine the % overlap of residential addresses between both boundary types for all boundary combinations

### Accuracy

Below are the average concordances between 2 boundary types, weighted by residential address counts

Concordance describes what % of residential addresses in the "from" boundary fit within a "to" boundary.

_e.g. 100% of postcode 3126 fits within the Boroondara LGA. However, only ~46% of postcode 3127 fits within that LGA (see image above)._

A high average concordance indicates your data can be converted to the new boundary reliability. The lower the concordance the more inaccurate the data conversion may be.

Concordances only work when going from a smaller boundary to a similar sized or larger one. This is evidenced in the table below.

| from                     | to                       | concordance | avg error       |
|:-------------------------| :----------------------- | ----------: |----------------:|
| abs 2016 lga             | abs 2016 sa3             |         74% |      4.7% |
| abs 2016 poa             | abs 2016 lga             |         93% |      1.4% |
| abs 2016 sa2             | abs 2016 lga             |         97% |      0.5% |
| abs 2016 sa2             | abs 2016 poa             |         79% |      4.2% |
| abs 2016 sa3             | abs 2016 lga             |         83% |      2.9% |
| geoscape 202202 lga      | abs 2016 lga             |        100% |       N/A |
| geoscape 202202 locality | abs 2016 lga             |         98% |       N/A |
| geoscape 202202 locality | abs 2016 sa2             |         94% |       N/A |
| geoscape 202202 locality | abs 2016 sa3             |         99% |       N/A |
| geoscape 202202 locality | geoscape 202202 lga      |         98% |       N/A |
| geoscape 202202 postcode | abs 2016 lga             |         93% |       N/A |
| geoscape 202202 postcode | abs 2016 sa3             |         92% |       N/A |
| geoscape 202202 postcode | geoscape 202202 lga      |         93% |       N/A |

****

## Get started

### 1. Get the Concordance File

There are 2 options to get it:
1. Download and import the file 
2. Run `create_concordance_file.py` to create it yourself

#### Download and Import

1. Download the [concordance file](/data)
2. Import it into your database/reporting tool of choice. If using Postgres:
    1. Edit the file path, schema name & table owner in `00_import_concordance_file.sql` in the [postgres-scripts](/postgres-scripts) folder
    2. Run the SQL script to import the file  

#### Run Python Script

This requires a knowledge of Python, Postgres & pg_restore.

It only needs to be done for 3 reasons:
1. The boundary from/to combination you need isn't in the standard [concordances file](/data)
2. We've been too lazy to update the concordances file with the latest boundary data from the ABS and/or Geoscape
3. You have a license of [Geoscape Buildings](https://geoscape.com.au/data/buildings/) or [Geoscape Land Parcels](https://geoscape.com.au/data/land-parcels/) and want to use the _planning zone_ data in those product to:
    1. Use a more accurate list of residential addresses to determine the data apportionment percentages (see note below); or
    2. Use a different set of addresses to apportion your data; e.g. industrial or commercial addresses

Running the script requires the following open data, available as Postgres dump files, as well as the optional licensed Geoscape data mentioned above:
1. ABS Census 2016 boundaries ([download](https://minus34.com/opendata/census-2016/census_2016_bdys.dmp))
2. ABS Census 2021 boundaries ([download](https://minus34.com/opendata/census-2021/census_2021_bdys.dmp))
3. GNAF from gnaf-loader ([download](https://minus34.com/opendata/geoscape-202202/gnaf-202202.dmp))
4. Geoscape Administrative Boundaries from gnaf-loader ([download](https://minus34.com/opendata/geoscape-202202/admin-bdys-202202.dmp))
5. ABS Census 2016 data - for QA only ([download](https://minus34.com/opendata/census-2016/census_2016_data.dmp))

###### Process

1. Download the above dump files and import them using `pg_restore`
2. Prep the Census boundary tagged address tables by running `01a_create_gnaf_2016_census_bdy_table.sql` & `01b_create_gnaf_2021_census_bdy_table.sql` in the [postgres-scripts](/postgres-scripts) folder
3. OPTIONAL: If you have access to Geoscape Buildings or Land Parcels data:
    1. import it into Postgres
    2. Edit the `02_create_residential_address_table.sql` in the [postgres-scripts](/postgres-scripts) folder to suit your dataset and schema name
    3. Run the above SQL script
4. Review the Python script as required - makes any required changes in the sections marked for editing
5. Add `psycopg2` to your Python 3.x environment
6. Run the script

##### Note
 - The benefit of using Geoscape planning zone data over the default residential address filter (ABS Census 2021 meshblock categories) is reduced by ~2.3m addresses not having a planning zone, The code as-is fills this missing data with 2021 meshblock categories.
 
### 2. Use the Concordance File

After loading the file into your database/reporting tool of choice - you use it by creating a 3 (or more) table join between the datsets you want to merge and the concordance file table.

Below is a sample script for merging postcode data with LGA data in Postgres:




## Data Licenses

Incorporates or developed using G-NAF © [Geoscape Australia](https://geoscape.com.au/legal/data-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under the [Open Geo-coded National Address File (G-NAF) End User Licence Agreement](https://data.gov.au/dataset/ds-dga-19432f89-dc3a-4ef3-b943-5326ef1dbecc/distribution/dist-dga-09f74802-08b1-4214-a6ea-3591b2753d30/details?q=).

Incorporates or developed using Administrative Boundaries © [Geoscape Australia](https://geoscape.com.au/legal/data-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

Based on [Australian Bureau of Statistics](https://www.abs.gov.au/websitedbs/d3310114.nsf/Home/Attributing+ABS+Material) data, licensed by the Commonwealth of Australia under the [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
