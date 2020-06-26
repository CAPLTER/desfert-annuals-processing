
# README ------------------------------------------------------------------

# Workflow to add new annuals composition data. Initial database creation and
# earlier data ingestions were detailed in knb-lter-cap.632.R and/or
# knb-lter-cap.cndep_initial_publish_and_db_creation.R.

# This workflow was modified for the 2020 assessment owing to complications
# from Covid, which resulted in an abruptly shortened season where only two
# sites were visited: the extra steps of filtering data where sites were not
# visited (i.e., lacking a sample_date) and addressing database functionality
# are added. The latter can be ignored in the future, and the former should not
# affect future efforts but careful to confirm this change in future workflows.

# libraries ---------------------------------------------------------------

library(readxl)
library(tidyverse)
library(RPostgreSQL)


# connections -------------------------------------------------------------

source('~/Documents/localSettings/pg_prod.R')
source('~/Documents/localSettings/pg_local.R')
  
pg <- pg_prod
pg <- pg_local


# fn() write_temp_table ---------------------------------------------------

write_temp_table <- function(databaseName, tempTableName) {
  
  if (dbExistsTable(pg, c(databaseName, tempTableName))) {
    
    dbRemoveTable(pg, c(databaseName, tempTableName))
    
  }
  
  dbWriteTable(pg,
               c(databaseName, tempTableName),
               value = get(tempTableName),
               row.names = F)
  
}


# new data ----------------------------------------------------------------

# import data
annuals_cover <- read_excel('~/Desktop/FinalAnnualsData_2020.xlsx')
# annuals_cover <- read_excel('~/Desktop/FinalAnnualsData_2019.xlsx')
# annuals_cover <- read_excel('~/Desktop/FinalAnnualsData_2018.xlsx')
# annuals_cover <- read_excel('~/Desktop/FinalAnnualsData_2017.xlsx')

# clean up names
colnames(annuals_cover) <- str_trim(colnames(annuals_cover), "both")
colnames(annuals_cover) <- gsub("\\r\\n", " ", colnames(annuals_cover))

# colnames(annuals_cover) <- gsub("%", "", colnames(annuals_cover))
# colnames(annuals_cover) <- gsub("/", "_", colnames(annuals_cover))
# colnames(annuals_cover) <- gsub(" - ", "_", colnames(annuals_cover), fixed = T)
# colnames(annuals_cover) <- gsub("-", "_", colnames(annuals_cover))

# add note regarding when personnel data are not included (likely not to be an
# issue now that Sally is managing)

# annuals_cover <- annuals_cover %>% mutate(Collector = replace(Collector,
# is.na(Collector), 'not reported'))

# add new cover events ----------------------------------------------------

# filter to include only sites that were sampled (see README ~ Covid and 2020
# sampling)
annuals_cover <- annuals_cover %>%
  filter(!is.na(`Date surveyed`))

# pare down to relevant cols for events
temp_cover_event <- annuals_cover %>%
  mutate(sample_date = as.Date(`Date surveyed`)) %>%
  select(sample_date,
         year = Year,
         plot = `Plot num`,
         patch_type = `Subplot type`,
         subplot = `Subplot num`,
         collector = Collector)

# check for uniquness - should not be any duplicates
temp_cover_event %>%
  count(year, plot, patch_type, subplot) %>%
  ungroup() %>%
  arrange(desc(n)) %>%
  filter(n > 1)

# cover_events to pg
write_temp_table(databaseName = 'urbancndep',
                 tempTableName = 'temp_cover_event')

# if (dbExistsTable(pg, c('urbancndep', 'temp_cover_events_old'))) dbRemoveTable(pg, c('urbancndep', 'temp_cover_events_old'))
# dbWriteTable(pg, c('urbancndep', 'temp_cover_events_old'), value = cover_event, row.names = F)

# IF sample_date is not of type date - may not be necessary, check first
# not needed in 2019 at least
# dbExecute(pg,'
# ALTER TABLE urbancndep.temp_cover_event
#   ALTER COLUMN sample_date TYPE DATE;')

insert_events_query <-
'INSERT INTO urbancndep.cover_events
(
  sample_date,
  year,
  plot,
  patch_type,
  subplot,
  collector
)
(
  SELECT
    sample_date,
    year,
    plot,
    patch_type,
    subplot,
    collector
  FROM
  urbancndep.temp_cover_event
);'

dbExecute(pg,
          insert_events_query)

# remove temp table
if (dbExistsTable(pg, c('urbancndep', 'temp_cover_event'))) dbRemoveTable(pg, c('urbancndep', 'temp_cover_event'))

# alternate PSQL workflow {
# write_csv(temp_cover_event, "~/Desktop/temp_cover_event.csv")
# after scp TO server:
# \COPY cover_events (sample_date, year, plot, patch_type, subplot, collector) FROM '~/Desktop/temp_cover_event.csv' DELIMITER ',' CSV HEADER ;
# }

# cover data --------------------------------------------------------------

# identify causes if converting to numeric throws some error messages
# helpful snooping tools:
  # apply(annuals_cover, 2, function(x) unique(x))
  # justchars <- annuals_cover[lapply(annuals_cover, class) == "character"]
  # apply(justchars, 2, function(x) unique(x))
  # str(annuals_cover, list.len = ncol(annuals_cover))
  # annuals_cover[!is.na(annuals_cover$`Chorizanthe rigida`),]$`Chorizanthe rigida`

str(annuals_cover, list.len = ncol(annuals_cover))

# beginning after Date (to apply to all columns where something is measured),
# convert cols (there are some of type char) to num
# annuals_cover[,c(14:ncol(annuals_cover))] <- apply(annuals_cover[,c(14:ncol(annuals_cover))], 2, function(x) as.numeric(as.character(x)))

# annuals_cover <- annuals_cover %>% 
#   mutate_at(.vars = 14:ncol(annuals_cover),
#             .funs = as.numeric)

# get event data that was previously added
event_id <- dbGetQuery(pg,'
SELECT
  cover_event_id,
  year,
  plot,
  patch_type,
  subplot
FROM urbancndep.cover_events;')

# alternate PSQL workflow {
# \COPY (SELECT cover_event_id, year, plot, patch_type, subplot FROM cover_events WHERE year = 2020) TO '~/Desktop/events_from_db.csv' DELIMITER ',' CSV HEADER ;
# after scp FROM server:
# event_id <- read_csv("~/Desktop/events_from_db.csv")
# }


# join annuals_cover on event_id to add event_id
annuals_cover <- annuals_cover %>% 
  left_join(event_id, by = c("Plot num" = "plot", "Subplot type" = "patch_type", "Subplot num" = "subplot", "Year" = "year"))

# remove empty columns - new to 2020
annuals_cover <- annuals_cover %>% 
  select_if(function(x){!all(is.na(x))})


# align column names with pg::cover_types ---------------------------------

# 1. get all columns employed in that year's data
dataNames <- annuals_cover %>% 
  select(`Amsinckia menziesii`:ncol(annuals_cover)) %>%
  gather(cover_type, cover_amt) %>%
  filter(cover_amt != 0) %>%
  filter(!is.na(cover_amt)) %>%
  distinct(cover_type) %>%
  mutate(
    cover_type = tolower(cover_type),
    cover_type = gsub(" ", "_", cover_type),
    dataNames = TRUE
  )

# 2. get the cover types that are already in the database
cover_types <- dbGetQuery(pg, '
SELECT
  cover_type_id,
  cover_category,
  cover_type
FROM urbancndep.cover_types;') %>%
  mutate(
    cover_type = tolower(cover_type),
    inDB = TRUE
  )

# alternate PSQL workflow {
# \COPY (SELECT cover_type_id, cover_category, cover_type FROM urbancndep.cover_types) TO '~/Desktop/taxa_from_db.csv' DELIMITER ',' CSV HEADER ;
# after scp FROM server:
# cover_types <- read_csv("~/Desktop/taxa_from_db.csv")
# cover_types <- cover_types %>%
#   mutate(
#     cover_type = tolower(cover_type),
#     inDB = TRUE
#   )
# }


# 3. join & anti-join to identify mismatches between data names and those in the DB

full_join(dataNames, cover_types, by = c("cover_type" = "cover_type")) %>%
  arrange(inDB, cover_type) %>%
  print(n = Inf)

anti_join(dataNames, cover_types, by = c("cover_type" = "cover_type")) %>% print(n = Inf)

cover_types %>% filter(grepl("litter", cover_type, ignore.case = T)) # helper line


# 4. add new taxa to DB

# addressed in database change log for 2020
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'Eucrypta_micrantha', 2020);")
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'Poaceae', 2020);")

# alternate PSQL workflow {
# INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'Eucrypta_micrantha', 2020);
# INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'Poaceae', 2020);
# }

# addressed in database change log for 2019
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'Amsinckia', 2018);")
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'Eucrypta', 2018);")
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'unidentified_1_2018', 2018);")
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'unidentified_2_2018', 2018);")
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('annual', 'unidentified_3_2018', 2018);")
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('plot characteristic', 'litter', 2018);")
# dbExecute(pg, "INSERT INTO urbancndep.cover_types(cover_category, cover_type, year_added) VALUES ('plot characteristic', 'green', 2017);")

# FIXES

# steps 1-4 are iterative processes to get to the following statements that will
# adjust column names of the new data such that they can be matched to existing
# (or newly added) entities in pg::cover_types

colnames(annuals_cover) <- gsub("latr", "larrea", colnames(annuals_cover), ignore.case = T)
colnames(annuals_cover) <- gsub(" sp\\.", "", colnames(annuals_cover), ignore.case = T)

annuals_cover <- annuals_cover %>%
  rename(
    `Amsinckia tessellata` = `Amsinckia tesselata`,
    `Eucrypta chrysanthemifolia` = `Eucrypta chrysamthemifolia`,
    #     `Herniaria hirsuta` = `Herniaria hirusta`,
    #     `Lotus strigosus` = `Lotus strigosus (L. tomentellus)`,
    #     `Physaria gordonii` = `Physaria (Lesquerella) gordonii`,
    #     `Amsinckia` = `Unknown Amsinckia`,
    #     `Pectocarya` = `Unknown Pectocarya`,
    `green` = `green (observed)`,
    #     `unidentified 1 2019` = `2019 Unknown #1 (rosette)`,
    #     `Bebbia juncea cover` = `Bebbia juncea (shrub) Cover`
    poaceae = `Unknown Poaceae`,
    `Castilleja exserta` = `Castilleja exserta (Orthocarpus purpurascens)`
  )


# edit composition data and load to pg ------------------------------------
  
# stack, filter, and join (to get cover_type id)

# get the cover type ids from database
cover_types <- dbGetQuery(pg, '
SELECT
  cover_type_id,
  cover_category,
  cover_type
FROM urbancndep.cover_types;') %>%
  mutate(cover_type = tolower(cover_type)) # to lower so we can ignore case

# note: sampled = TRUE is a dummy variable that will facilitate creating a
# record when pulled from the database even when no plants or plot
# characteristics were present. This is really, really, important.

temp_annuals_composition <- annuals_cover %>%
  select(`Amsinckia menziesii`:cover_event_id) %>%
  mutate(sampled = TRUE) %>% # see note above
  gather(cover_type, cover_amt, -cover_event_id) %>%
  filter(
    cover_amt != 0,
    !is.na(cover_amt)
  ) %>% 
  mutate(
    cover_type = tolower(cover_type),
    cover_type = gsub(" ", "_", cover_type)
  ) %>% 
  left_join(cover_types, by = c('cover_type')) %>%
  select(cover_event_id, cover_type_id, cover_amt)


# annuals_cover to pg
write_temp_table(databaseName = 'urbancndep',
                 tempTableName = 'temp_annuals_composition')

insert_composition_query <-
'INSERT INTO urbancndep.cover_composition
(
  cover_event_id,
  cover_type_id,
  cover_amt
)
(
  SELECT
    cover_event_id,
    cover_type_id,
    cover_amt
  FROM
  urbancndep.temp_annuals_composition
);'

dbExecute(pg,
          insert_composition_query)

# remove the temp table
if (dbExistsTable(pg, c('urbancndep', 'temp_annuals_composition'))) dbRemoveTable(pg, c('urbancndep', 'temp_annuals_composition'))


# alternate PSQL workflow {
# write_csv(temp_annuals_composition, "~/Desktop/temp_annuals_composition.csv")
# after scp TO server:
# \COPY cover_composition (cover_event_id, cover_type_id, cover_amt) FROM '~/Desktop/temp_annuals_composition.csv' DELIMITER ',' CSV HEADER ;
# }
