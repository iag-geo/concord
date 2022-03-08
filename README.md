# concord

A series of Postgres scripts for determining the % of address, population & dwelling counts (i.e. the concordance) between the following Australian boundaries:
1. ABS Census SA2's and Postcodes
2. ABS Census SA3's and Local Government Areas (i.e. Councils)

Purpose is to allow you to apportion your own data between the above boundaries.

For example, you have 2 sets of data; one is colected at the SA2 level the other is by postcode. Q: how do you combine the 2 datasets? A: by apportionaing the data, as a percentage of the total data, by the overlaps between the SA2s and Postcodes.

TODO: _**Insert useful image here**_

## (Proposed) Methodology

### 1. Get Population/Dwelling Estimates

  a. Determine the change in GNAF address counts since the last ABS Census (2016) to estimate population & dwelling growth in developing suburbs
  b. Using population population & dwelling density (e.g. pop. per square km) - create current pop. and dwelling estimates for each boundary
  c. Validate the above using annual ABS pop.estimates and the SA2 and LGA level
  
  
