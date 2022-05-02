# Concord

A CSV file for converting data between Australian census & administrative boundaries.

It allows you to merge data based on different boundaries into a single dataset or report.

The concordance file is a CSV file for importing into your database or reporting tool. A script for importing into Postgres is also provided.

### Examples

- You have sales data by postcode & competitor data by local government area (LGA) and need to determine market share. Using the boundary concordances CSV file, you convert the postcode data to LGA and merge both datasets by LGA ID.
- You have cancer testing rates by ABS Census 2016 SA2 boundaries and cancer case numbers by LGA and need to determine the rate of disease as a % of tests. You use the Concord file to convert the SA2 data to LGA and merge both datasets by LGA ID.

## Methodology

The concordance file is generated using this approach:

1. Tag all GNAF addresses with 2016 & 2021 ABS Census boundaries and Geoscape 202202 Administrative boundaries 
2. Remove all addresses in ABS Census 2021 meshblocks that are non-residential
3. Aggregate all residential address by the _**from**_ boundary and the _**to**_ boundary
4. Determine the % overlap of addresses between both boundary types for all boundary combinations

![pc_vs_lga.png](https://github.com/iag-geo/concord/blob/main/pc_vs_lga.png "ABS 2016 Postcodes (pink) vs LGAs (blue)")

Postcode 3127 split 46%-54% by the Boroondara & Whitehorse council boundaries

### Important
- Only works with data related to residents, citizens & consumers. In other words - industrial, commercial & special use data isn't suited.
- The % overlaps between boundaries are a best estimate of how data should be apportioned between 2 boundary sets. Your data may have biases in it that mean this approach doesn't return the best result. e.g. looking at the above image - if your postcode 3127 customers were mostly in the Boroondara Council side - the boundary concordance file would incorrectly put 54% of customers in the neighbouring council.

## Concordances

Below are the average concordances between 2 boundary types, weighted by residential address counts

Concordance describes what % of residential addresses in the "from" boundary fit within a "to" boundary.

_e.g. 100% of postcode 3126 fits within the Boroondara LGA. However, only ~46% of postcode 3127 fits within that LGA (see image above)._

A high average concordance indicates your data can be converted to the new boundary reliability. The lower the concordance the more inaccurate the data conversion may be. 

| from                     | to                  | avg. concordance |
|:-------------------------|:--------------------|-----------------:|
| abs 2016 lga             | abs 2016 sa3             |         74% |
| abs 2016 poa             | abs 2016 lga             |         93% |
| abs 2016 sa2             | abs 2016 lga             |         97% |
| abs 2016 sa2             | abs 2016 poa             |         79% |
| abs 2016 sa2             | abs 2016 sa3             |        100% |
| abs 2016 sa3             | abs 2016 lga             |         83% |
| geoscape 202202 lga      | abs 2016 lga             |        100% |
| geoscape 202202 locality | abs 2016 lga             |         98% |
| geoscape 202202 locality | geoscape 202202 lga      |         98% |
| geoscape 202202 postcode | abs 2016 lga             |         93% |
| geoscape 202202 postcode | geoscape 202202 lga      |         93% |


notes:
 - the benefits of using Geoscape Buildings is minimal due to ~2.3m addresses not having a planning zone
 - you need to run 02_create_residential_address_table.sql on your GNAF and Geoscape Buildings data to create the
   address_principals_buildings table below




