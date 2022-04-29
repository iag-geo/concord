# Concord

An easy-to-use dataset for converting data between Australian census and administrative boundaries. It allows you to merge data that's based on different boundaries into a single dataset or report.

**Important:** only works with data related to residents, citizens & consumers. Industrial, commercial & special use areas are deliberately ignored in the analysis.

### Example usage

You have sales volumes by postcode & competitor data by local government area (LGA) and need to determine market share. Using Concord, you convert the postcode data to LGA and then merge both datasets by LGA ID to determine market penetration.

## Methodology

The concordance file is generated using the following approach:

1. Tag all GNAF addresses with 2016 & 2021 ABS Census boundaries and Geoscape 202202 Administrative boundaries 
2. Remove all addresses in ABS Census meshblocks that are non-residential
3. TODO: etc..

## Concordances

Below are the average concordances between 2 boundary types, weighted by residential address counts

Concordance describes what % of residential addresses in the "from" boundary fit within a "to" boundary.

_e.g. 100% of postcode 2040 fits within the Inner West Council LGA. However, only ~40% of postcode 2042 fits within the City of Sydney LGA._

A high average concordance indicates your data can be converted to the new boundary reliability. THe lower the concordance the more biases will appear in your converted data. 

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





TODO: _**Insert useful image here**_

