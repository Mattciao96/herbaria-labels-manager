# LIBS
library(dplyr)
library(stringr)
library(tidyr)

# VARIABLES
regex_code_id <- 'FI-HCI-[0-9]+'
regex_cover_id <- 'Folder-FI-HCI-[0-9]+'
regex_old_id <- 'FI[0-9]+'

batch_colnames <-
  c(
    'TYPE',
    'CUBBY.NUMBER',
    'COVER.BARCODE',
    'SHEET.TYPE',
    'COLOR',
    'SHEET.BARCODE',
    'CONNECTIONS',
    'DATE.CREATED',
    'PATH.JPG',
    'BOH'
  )


# iniziamo con il primo file hardcoded
batch_file <- 'data/Connection/CP1_20240603_BATCH_0001.csv'
cover_file <- 'data/Cover/COVER_COVER_CP1_20240603_BATCH_0001.csv'
sheet_file <- 'data/Sheets/SHEET_SHEET_CP1_20240603_BATCH_0001.csv'

batch_data <- read.csv(batch_file, sep = ';' , header = FALSE)
names(batch_data) <- batch_colnames
sheet_data <- read.csv(sheet_file)
cover_data <- read.csv(cover_file)


################################################################################
# PROCESSING
################################################################################


# 1: extract herbarium id from image path and notes
# find duplicates in herbarium id
sheet_data <- sheet_data %>%
  mutate(ORIGINAL.HERBARIUM.ID = str_extract(PATH.JPG, regex_code_id)) %>% 
  mutate(HERBARIUM.ID =
           ifelse(
             !is.na(str_extract(NOTES, regex_code_id)),
             str_extract(NOTES, regex_code_id),
             str_extract(PATH.JPG, regex_code_id)
           ))

sheet_data <- sheet_data %>%
  group_by(HERBARIUM.ID) %>%
  mutate(ID.DUPLICATE = n() > 1) %>%
  ungroup()

# 1.1: extract cover id from image path and notes
# find duplicates in cover id (i don't think it is a problem here)
cover_data <- cover_data %>%
  mutate (COVER.ID = str_extract(PATH.JPG, regex_cover_id))

cover_data <- cover_data %>% 
  group_by(COVER.ID) %>% 
  mutate(ID.DUPLICATE = n() > 1) %>% 
  ungroup()

# cover_data %>% filter(ID.DUPLICATE == TRUE) # to check for duplicates

# 1.2 add the dataset type (BATCH, SHEET, COVER) to the columns do avoid duplicate names 
batch_data <- batch_data %>%
  rename_with(.fn = function(column_name) {
    paste0("BATCH.", column_name)
  })

sheet_data <- sheet_data %>%
  rename_with(.fn = function(column_name) {
    paste0("SHEET.", column_name)
  })

cover_data <- cover_data %>%
  rename_with(.fn = function(column_name) {
    paste0("COVER.", column_name)
  })

# 1.3 for batch data, extract the old id in a column OLD.ID
# and the  original barcode
batch_data <- batch_data %>%
  mutate(BATCH.SHEET.ORIGINAL.BARCODE = BATCH.SHEET.BARCODE) %>%
  mutate(BATCH.OLD.ID = str_extract(BATCH.CONNECTIONS, regex_old_id))



################################################################################
# 2 merge all data
################################################################################

# 2: create a new row for each sheet in batch written in CONNECTIONS with the SHEET.BARCODE taken from CONNECTIONS
# iterate each row in cp_data with Type == 'Sheet'
# check if CONNECTIONS is not empty
# extract all different herbarium id using pattern FI-HCI-[0-9]+
# for each one copy the row and change the herbarium id in Sheet.Barcode
# but i need to consider even the old id type (FI[0-9]+)

#test_rem <- sheet_data %>% filter(SHEET.ID.DUPLICATE == TRUE)

new_rows_to_add <- batch_data %>%
  filter(BATCH.TYPE == 'Sheet',
         !is.na(BATCH.CONNECTIONS),
         BATCH.CONNECTIONS != '') %>%
  mutate(EXTRACTED.ID = str_extract_all(BATCH.CONNECTIONS, regex_code_id)) %>% # there can be multiple numbers
  filter(lengths(EXTRACTED.ID) > 0) %>%
  mutate(UNIQUE.ID = lapply(EXTRACTED.ID, unique)) %>%

  unnest(cols = c(UNIQUE.ID)) %>%   # expand rows based on unique IDs !bellissimo
  mutate(BATCH.SHEET.BARCODE = UNIQUE.ID) %>%
  select(-EXTRACTED.ID, -UNIQUE.ID)


batch_data <- bind_rows(batch_data, new_rows_to_add)

# 2.1 remove  all others with Type != 'Sheet'
batch_data <- batch_data %>%
  filter(BATCH.TYPE == 'Sheet')

# 2.2: merge sheet and batch
# errors

not_in_batch_errors <-  sheet_data %>%
  left_join(batch_data,
            by = c('SHEET.HERBARIUM.ID' = 'BATCH.SHEET.BARCODE')) %>%
  filter(is.na(BATCH.TYPE))

# even the opposite
not_in_sheet_errors <-  batch_data %>%
  left_join(sheet_data,
            by = c('BATCH.SHEET.BARCODE' = 'SHEET.HERBARIUM.ID')) %>%
  filter(is.na(SHEET.APPLICATION.ID))

# correct sheet and batch
#sheet_batch_data <-  sheet_data %>%
#  left_join(batch_data,
#  by = c('SHEET.ORIGINAL.HERBARIUM.ID' = 'BATCH.SHEET.BARCODE'))
#
batch_sheet_data <-  batch_data %>%
  left_join(sheet_data,
            by = c('BATCH.SHEET.BARCODE' = 'SHEET.HERBARIUM.ID'))

# check duplicates
batch_sheet_data %>% group_by(BATCH.SHEET.BARCODE, SHEET.ID.DUPLICATE) %>%
  summarise(n = n()) %>%
  filter(n > 1)

# change name for duplicates, the first keep it as it is, from the second duplicate  -> from  BATCH.SHEET.BARCODE to BATCH.SHEET.BARCODE + -DUP-<number>
# super
batch_sheet_data <- batch_sheet_data %>%
  group_by(BATCH.SHEET.BARCODE) %>%
  mutate(
    FINAL.ID = if_else(
      row_number() > 1,
      paste0(BATCH.SHEET.BARCODE, "-DUP-", row_number() - 1),
      BATCH.SHEET.BARCODE
    )
  ) %>%
  ungroup()

batch_sheet_data %>% group_by(FINAL.ID) %>%
  summarise(n = n()) %>%
  filter(n > 1)


# 2.3: merge sheet_cp and cover
sheet_batch_cover_data <- batch_sheet_data %>%
  left_join(cover_data, by = c('BATCH.COVER.BARCODE' = 'COVER.COVER.ID'))


# ! NOTE: STEP 2 WAS A NIGTHMARE DUE TO MANY UNEXPECTED DATA

################################################################################
# 3: manage multisheet
################################################################################

sheet_batch_cover_data %>% arrange(BATCH.)
# i already ordered the table correctly
# create a column is_multisheet_first
# iterate through the sheet_batch_cover_data and check if there are 'Multisheet' in batch.Sheet.Type
# the first row with multisheet will have is_multisheet_first = TRUE
# in anoter table write the relation with sheet.herbarium_id of the various record (one to many)
# if batch.Cover.Barcode changes or barch.Color changes the chain of correlation of current multisheets is broken so go on
multisheet_data <- sheet_batch_cover_data %>%
  filter(batch.Sheet.Type == 'Multisheet') %>%
  arrange(batch.Cover.Barcode, batch.Date.Created)

# now i want to create a new table that contains the many to many relation
# first i want to create a new column that contains data




# --- 2. Process the Data ---

# Identify consecutive runs and find the first multisheet within each run
sheet_batch_cover_data_processed <- sheet_batch_cover_data %>%
  # Create an ID for consecutive runs of Barcode and Color
  mutate(group_run_id = consecutive_id(batch.Cover.Barcode, batch.Color)) %>%
  # Group by these consecutive runs
  group_by(group_run_id) %>%
  # Within each group, find the index (row number relative to group start)
  # and the ID of the *first* 'Multisheet'
  mutate(
    first_multisheet_index_in_group = which(batch.Sheet.Type == 'Multisheet')[1],
    # Get index of first TRUE
    # Get the herbarium ID corresponding to that index within the group
    # Use NA if no 'Multisheet' is found in the group
    multisheet_first_id_in_group = if_else(
      !is.na(first_multisheet_index_in_group),
      sheet.herbarium_id[first_multisheet_index_in_group],
      NA_character_ # Use NA of the correct type (character)
    )
  ) %>%
  # Ungroup to calculate row_number across the whole dataframe if needed later,
  # but keep group info for the next step
  # ungroup() # Actually, let's keep it grouped for the next mutate
  
  # Now create the 'is_multisheet_first' column
  mutate(
    # Check if a first multisheet was found AND if the current row *is* that first one
    is_multisheet_first = !is.na(first_multisheet_index_in_group) &
      (row_number() == first_multisheet_index_in_group)
  ) %>%
  ungroup() # Now we can safely ungroup

# Display the result with the new column
print("Processed Data with 'is_multisheet_first':")
print(
  sheet_batch_cover_data_processed %>% select(-group_run_id, -first_multisheet_index_in_group)
) # Hide intermediate columns for clarity

# --- 3. Create the Relationship Table ---

multisheet_relations <- sheet_batch_cover_data_processed %>%
  # Keep only rows that belong to a group where a multisheet was found
  filter(!is.na(multisheet_first_id_in_group)) %>%
  # Select the ID of the first multisheet in the group and the ID of the current row
  select(multisheet_first_herbarium_id = multisheet_first_id_in_group,
         related_herbarium_id = sheet.herbarium_id) %>%
  # Optional: remove self-references if needed (where first ID = related ID)
  # filter(multisheet_first_herbarium_id != related_herbarium_id) %>%
  # Ensure distinct relationships if duplicates somehow occurred (unlikely with this method)
  distinct()



# --- 4. Final Original Table (Optional Cleanup) ---
# You might want the original table just with the is_multisheet_first column

sheet_batch_cover_data_processed <-
  sheet_batch_cover_data_processed %>%
  select(-group_run_id,
         -first_multisheet_index_in_group,
         -multisheet_first_id_in_group)


multisheet_relations <- multisheet_relations %>%
  rename(herbarium_id_with_label = multisheet_first_herbarium_id,
         related_herbarium_id = related_herbarium_id)





# tabella raw (fatta)
# tabella multisheet_relations (fatta)
# tabella dwc
# tabella locality (generata dal web)
# tabella name (generata dal web)




















# at the eend of everythig i can add a column with the path to the label transcription