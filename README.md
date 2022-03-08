# concord

A series of Postgres scripts for determining the % of address, population & dwelling counts (i.e. the concordance) between the following Australian boundaries:
1. ABS Census SA2's and Postcodes
2. ABS Census SA3's and Local Government Areas (i.e. Councils)

Purpose is to allow you to apportion your own residential statistical data between the above boundaries.

For example, you have 2 sets of data; one is collected at the SA2 level the other is by postcode. Q: how do you combine the 2 datasets? A: by apportionaing the data, as a percentage of the total data, by the overlaps between the SA2s and Postcodes.

**Important:** only works with data related to residents, citizens & consumers. Industrial, commercial and special use areas are deliberately removed in this process.

TODO: _**Insert useful image here**_

## (Proposed) Methodology

### Step 1 - Get Population/Dwelling Estimates by Boundary

1. Using ABS Census meshblock & Open Street Map boundaries remove non-residential areas from the bdys (industrial/commercial areas, shops, parks, etc...)
2. Remove the non-residential GNAF points in the removed areas. Do this for the current GNAF and the February 2017 release of GNAF (Feb 2017 is used as it most likely represents the closest number of addresses surveyed on Census night in August 2016)
3. Determine the change in residential GNAF address counts since the last ABS Census (2016) to estimate population & dwelling growth in developing suburbs
4. Using population & dwelling density (e.g. pop. per square km) - create current pop. and dwelling estimates for each boundary
5. Validate the above using annual ABS pop.estimates and the SA2 and LGA level

### Step 2 - Apportion Pop. & Dwelling Counts Between Boundaries
