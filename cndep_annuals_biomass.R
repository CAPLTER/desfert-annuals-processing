
# README ------------------------------------------------------------------

# Script for processing and uploading Des Fert annuals biomass data to the
# database. Based on an Excel sheet supplied by the technicians.

# the initial build of the annuals_biomass table did not have a foreign key to
# plot id. Added here but this is a one-time op.
# dbSendStatement(pg, '
#             ALTER TABLE urbancndep.annuals_biomass
#             ADD CONSTRAINT annuals_biomass_fk_plot_id
#                 FOREIGN KEY (plot_id)
#                 REFERENCES urbancndep.plots(id);')


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
biomass2018 <- read_excel('~/Desktop/CNDep_SpringAnnualBiomassSampling_2018_wdata.xlsx')

biomass_temp <- biomass2018 %>%
  rename(mass = `Aboveground Dry Mass (g) - envelope (g)`) %>%  # that field name is painful
  # mutate(mass = replace(mass, grepl("no sample", NOTES), NA)) %>% # uncollected samples
  # mutate(mass = round(mass, digits = 3)) %>% # whoa, lots of sig figs
  mutate(
    date = as.Date(`Collection Date`),
    year = as.numeric(format(date, format = "%Y")),
    NOTES = paste0(ifelse(is.na(NOTES), "", paste0(NOTES, "; ")), "average bag wt (n=10) = 1.437")
  ) %>%
  select(plot_id = `Trmt Plots`,
         location_within_plot = `Sub-plots`,
         replicate = replicates,
         subquad_orientation = `Subquadrat Orientation (N, S, E, W)`,
         date,
         year,
         mass,
         notes = NOTES)


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

