# README

# Script for processing and uploading Des Fert annuals biomass data to the
# database. Based on an Excel sheet supplied by the technicians. The coding here
# is specific to 2017 and should be generally applicable across years, but with
# likely some modification required owing to these data being collected in a
# spreadsheet.

# libraries ----
library(RPostgreSQL)
library(tidyverse)
library(readxl)

# database connections ----
pg <- dbConnect(dbDriver("PostgreSQL"),
                user="srearl",
                dbname="srearl",
                host="localhost",
                password=.rs.askForPassword("Enter password:"))

pg <- dbConnect(dbDriver("PostgreSQL"),
                user="srearl",
                dbname="caplter",
                host="postgresql.research.gios.asu.edu",
                password=.rs.askForPassword("Enter password:"))

# the initial build of the annuals_biomass table did not have a foreign key to
# plot id. Added here but this is a one-time op.
# dbSendStatement(pg, '
#             ALTER TABLE urbancndep.annuals_biomass
#             ADD CONSTRAINT annuals_biomass_fk_plot_id
#                 FOREIGN KEY (plot_id)
#                 REFERENCES urbancndep.plots(id);')

# DesFert_Biomass_spr2017.xlsx is the name of the original file supplied by
# Quincy, but the ultimate storage name and path is: 
# /Research/UrbanCNdep/cndepAnnuals&CommunityComp/Biomass/SamplingSheets_withData/CNDep_SpringAnnualBiomassSampling_2017_wdata.xlsx

# get the data and format as appropriate and to match database table structure ----
biomass2017 <- read_excel('~/Desktop/DesFert_Biomass_spr2017.xlsx')

biomass_temp <- biomass2017 %>%
  rename(mass = `Aboveground Dry Mass (g) - Bag Mass (g)`) %>%  # that field name is painful
  mutate(mass = replace(mass, grepl("no sample", NOTES), NA)) %>% # uncollected samples
  mutate(mass = round(mass, digits = 3)) %>% # whoa, lots of sig figs
  mutate(year = as.numeric(format(collection_date, format = "%Y"))) %>% # might as well get the year while we are at it
  mutate(NOTES = paste0(ifelse(is.na(NOTES), "", paste0(NOTES, "; ")), "average bag wt (n=10) = 6.733")) %>% 
  select(plot_id,
         location_within_plot,
         replicate,
         subquad_orientation,
         date = collection_date,
         year,
         mass,
         notes = NOTES)

# database operations ----

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

