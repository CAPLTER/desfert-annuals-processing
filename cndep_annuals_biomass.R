
# README ------------------------------------------------------------------

# Script for processing and uploading Des Fert annuals biomass data to the
# database. Based on an Excel sheet supplied by the technicians.


# libraries ---------------------------------------------------------------

library(RPostgreSQL)
library(tidyverse)
library(readxl)


# connections -------------------------------------------------------------

# postgres
source('~/Documents/localSettings/pg_prod.R')
source('~/Documents/localSettings/pg_local.R')
  
pg <- pg_prod
pg <- pg_local


# process data ------------------------------------------------------------

# get the data and format as appropriate and to match database table structure ----
biomass2019 <- read_excel('~/Desktop/DesFert_Biomass_SPR2019.xlsx')
# biomass2018 <- read_excel('~/Desktop/CNDep_SpringAnnualBiomassSampling_2018_wdata.xlsx')

biomass_temp <- biomass2019 %>%
  rename(mass = `Aboveground Dry Mass (g) - brown bag (g)`) %>%  # that field name is painful
  mutate(
    date = as.Date(`Collection Date`),
    year = as.numeric(format(date, format = "%Y")),
    notes = "Some of the brown bags differed in type due to location where the bags were purchased. The two types of bag used were lunch bags and giant lunch bags. The lunch bags were smaller and the giant lunch bags were larger. Average weight of small bag (n = 10) = 7.297; average weight of large bag (n = 10) = 10.241."
  ) %>%
  select(
    plot_id = `Trmt Plots`,
    location_within_plot = `Sub-plots`,
    replicate = replicates,
    subquad_orientation = `Subquadrat Orientation (N, S, E, W)`,
    date,
    year,
    mass,
    notes)


# data to postgres --------------------------------------------------------

# write the data to a temp table
if (dbExistsTable(pg, c('urbancndep', 'biomass_temp'))) dbRemoveTable(pg, c('urbancndep', 'biomass_temp')) # make sure tbl does not exist
dbWriteTable(pg, c('urbancndep', 'biomass_temp'), value = biomass_temp, row.names = F)

# insert data from the temp table into annuals_biomass
insert_biomass_query <-
  'INSERT INTO urbancndep.annuals_biomass
  (
    plot_id,
    location_within_plot,
    replicate,
    subquad_orientation,
    date,
    year,
    mass,
    notes
  )
  (
    SELECT
      plot_id,
      location_within_plot,
      replicate,
      subquad_orientation,
      date,
      year,
      mass,
      notes
    FROM
    urbancndep.biomass_temp
  );'

executeinsert <- dbSendStatement(pg, insert_biomass_query)

# clean up
if (dbHasCompleted(executeinsert)) { dbRemoveTable(pg, c('urbancndep', 'biomass_temp')) } else
{ stop('something wrong biomass_temp not deleted') }

