# Concord

An easy-to-use dataset for converting data between Australian census and administrative boundaries.

Its purpose is to allow you to merge data that's based on different boundaries, into a single dataset or report.

**Important:** only works with data related to residents, citizens & consumers. Industrial, commercial & special use areas are deliberately ignored in the analysis.

### Example

You have sales volumes by postcode & competitor data by local government area (LGA) and need to determine market share. By using Concord, you can convert the postcode data to LGA and then merge by LGA ID to determine market penetration.

With a known concordance of 94% between postcodes & LGAs, you can do this with a maximum error of 6%.

## Methodology

The concordance file is generated using the following approach:

1. Tag all GNAF addresses with 2016 & 2021 ABS Census boundaries and Geoscape 202202 Administrative boundaries 
2. Remove all addresses in ABS Census meshblocks that are non-residential
3. TODO: etc..








TODO: _**Insert useful image here**_

