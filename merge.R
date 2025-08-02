# load files in env
library(dplyr)
library(stringr)
library(tidyr)


year <- '2024'
month <- '01'
day <- '01'
cp <- 'CP1'
batch <- '0001'
batch_name <- 'BATCH'
sheet_name <- 'SHEET_SHEET'
cover_name <- 'COVER_COVER'
LABEL_PATH <- 'test/'

regex_code_id <- 'FI-HCI-[0-9]+' # regex to extract herbarium id from notes or image path
regex_cover_id <- 'Folder-FI-HCI-[0-9]+'
batch_file <- paste0(LABEL_PATH, cp, '_', year, month, day, '_', batch_name, '_', batch, '.csv')
sheet_file <- paste0(LABEL_PATH, cp, '_', year, month, day, '_', sheet_name, '_', batch, '.csv')
cover_file <- paste0(LABEL_PATH, cp, '_', year, month, day, '_', cover_name, '_', batch, '.csv')

batch_data <- read.csv(batch_file, sep = ';')
sheet_data <- read.csv(sheet_file)
cover_data <- read.csv(cover_file)


# 1: extract herbarium id and cover id from image path and notes
sheet_data <- sheet_data %>% 
  mutate(herbarium_id = 
           ifelse(!is.na(str_extract(Notes, regex_code_id)),
                  str_extract(Notes, regex_code_id),
                  str_extract(Path.JPG, regex_code_id)
                  )
         )

cover_data <- cover_data %>% 
  mutate (
    cover_id = str_extract(Path.JPG, regex_cover_id)
  )

# 1.1 add the dataset name to the columns
batch_data <- batch_data %>%
  rename_with(.fn = ~ paste0("batch.", .x))

sheet_data <- sheet_data %>%
  rename_with(.fn = ~ paste0("sheet.", .x))

cover_data <- cover_data %>%
  rename_with(.fn = ~ paste0("cover.", .x))

# 1.2 CHECK ultra important there are duplicate IDs in sheet_data
duplicates <- sheet_data %>% 
  group_by(sheet.herbarium_id) %>% 
  filter(n() > 1) %>%
  ungroup()

# 2: create a new row for each sheet in batch
# iterate each row in cp_data with Type == 'Sheet'
# check if Garbage is not empty
# extract all different herbarium id using pattern FI-HCI-[0-9]+
# for each one copy the row and change the herbarium id in Sheet.Barcode

new_rows_to_add <- batch_data %>%
  filter(
    batch.Type == 'Sheet',
    !is.na(batch.Garbage),
    nzchar(batch.Garbage)
  ) %>%
  # Extract all IDs
  mutate(extracted_ids = str_extract_all(batch.Garbage, regex_code_id)) %>%
  # Keep only rows where IDs were found
  filter(lengths(extracted_ids) > 0) %>%
  # Get unique IDs per row
  mutate(unique_ids = lapply(extracted_ids, unique)) %>%
  # Expand rows based on unique IDs
  unnest(cols = c(unique_ids)) %>%
  # *** CRITICAL STEP: Update Barcode for these NEW rows ***
  mutate(batch.Sheet.Barcode = unique_ids) %>%
  # Clean up temporary columns
  select(-extracted_ids, -unique_ids)

processed_batch_data <- bind_rows(batch_data, new_rows_to_add)

# 2.1 remove remove (not transcribe red) and all others with Type != 'Sheet'
processed_batch_data <- processed_batch_data %>% 
  filter(batch.Type == 'Sheet')

# 2: merge sheet and cp
sheet_batch_data <-  sheet_data %>% 
  left_join(processed_batch_data, by = c('sheet.herbarium_id' = 'batch.Sheet.Barcode'))

missing_batch <- sheet_batch_data %>% 
  filter(is.na(batch.Type))

# 2.1: merge sheet_cp and cover
sheet_batch_cover_data <- sheet_batch_data %>% 
  left_join(cover_data, by = c('batch.Cover.Barcode' = 'cover.cover_id'))

# NOTE: i lost the batch data for the cover but i think it is not useful at all 

# 3: manage multisheet

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
    first_multisheet_index_in_group = which(batch.Sheet.Type == 'Multisheet')[1], # Get index of first TRUE
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
print(sheet_batch_cover_data_processed %>% select(-group_run_id, -first_multisheet_index_in_group)) # Hide intermediate columns for clarity

# --- 3. Create the Relationship Table ---

multisheet_relations <- sheet_batch_cover_data_processed %>%
  # Keep only rows that belong to a group where a multisheet was found
  filter(!is.na(multisheet_first_id_in_group)) %>%
  # Select the ID of the first multisheet in the group and the ID of the current row
  select(
    multisheet_first_herbarium_id = multisheet_first_id_in_group,
    related_herbarium_id = sheet.herbarium_id
  ) %>%
  # Optional: remove self-references if needed (where first ID = related ID)
  # filter(multisheet_first_herbarium_id != related_herbarium_id) %>%
  # Ensure distinct relationships if duplicates somehow occurred (unlikely with this method)
  distinct()



# --- 4. Final Original Table (Optional Cleanup) ---
# You might want the original table just with the is_multisheet_first column

sheet_batch_cover_data_processed <- sheet_batch_cover_data_processed %>%
  select(-group_run_id, -first_multisheet_index_in_group, -multisheet_first_id_in_group)


multisheet_relations <- multisheet_relations %>% 
  rename(
    herbarium_id_with_label = multisheet_first_herbarium_id,
    related_herbarium_id = related_herbarium_id
  )





# tabella raw (fatta)
# tabella multisheet_relations (fatta)
# tabella dwc
# tabella locality (generata dal web)
# tabella name (generata dal web)




















# at the eend of everythig i can add a column with the path to the label transcription