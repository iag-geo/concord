# Concord

An easy-to-use dataset for converting data between Australian census and administrative boundaries. It allows you to merge data that's based on different boundaries into a single dataset or report.

**Important:** only works with data related to residents, citizens & consumers. Industrial, commercial & special use areas are deliberately ignored in the analysis.

### Example usage

You have sales volumes by postcode & competitor data by local government area (LGA) and need to determine market share. Using Concord, you convert the postcode data to LGA and then merge both datasets by LGA ID to determine market penetration.

With a known concordance of 94% between postcodes & LGAs, this results in a maximum error of 6%.

## Methodology

The concordance file is generated using the following approach:

1. Tag all GNAF addresses with 2016 & 2021 ABS Census boundaries and Geoscape 202202 Administrative boundaries 
2. Remove all addresses in ABS Census meshblocks that are non-residential
3. TODO: etc..

## Concordances

Below are the average concordances between 2 boundary types, weighted by residential address counts

| from                     | to                  | concordance |
|:-------------------------|:--------------------|------------:|
| abs 2016 lga             | abs 2016 sa3        |         73% |
| abs 2016 poa             | abs 2016 lga        |         94% |
| abs 2016 sa2             | abs 2016 lga        |         98% |
| abs 2016 sa2             | abs 2016 poa        |         82% |
| abs 2016 sa2             | abs 2016 sa3        |        100% |
| abs 2016 sa3             | abs 2016 lga        |         85% |
| geoscape 202202 lga      | abs 2016 lga        |        100% |
| geoscape 202202 locality | abs 2016 lga        |         98% |
| geoscape 202202 locality | geoscape 202202 lga |         98% |
| geoscape 202202 postcode | abs 2016 lga        |         94% |
| geoscape 202202 postcode | geoscape 202202 lga |         93% |





TODO: _**Insert useful image here**_

