# ricordati di mettere anche i missing in sheet dopo multisheet

# LIBS
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)

# VARIABLES
regex_code_id <- 'FI-HCI-[0-9]+'
regex_cover_id <- 'Folder-FI-HCI-[0-9]+'
regex_old_id <- 'FI[0-9]+'
regex_fiaf_id <- 'FIAF-[0-9]+'

batch_colnames <- c(
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
# batch_name = 'CP2_20240920_BATCH_0001.csv';
# Function to process a single batch
process_batch <- function(batch_name) {
  cat("Processing:", batch_name, "\n")
  
  # Extract the base name (without .csv extension)
  base_name <- gsub("\\.csv$", "", batch_name)
  
  # Construct file paths
  save_file <- paste0('processed_', batch_name)
  batch_file <- paste0('data/Connection/', batch_name)
  cover_file <- paste0('data/Cover/COVER_COVER_', batch_name)
  sheet_file <- paste0('data/Sheets/SHEET_SHEET_', batch_name)
  
  # Check if all required files exist
  if (!file.exists(batch_file)) {
    cat("ERROR: Batch file not found:", batch_file, "\n")
    return(FALSE)
  }
  if (!file.exists(sheet_file)) {
    cat("ERROR: Sheet file not found:", sheet_file, "\n")
    return(FALSE)
  }
  if (!file.exists(cover_file)) {
    cat("ERROR: Cover file not found:", cover_file, "\n")
    return(FALSE)
  }
  
  # Read data
  tryCatch({
    batch_data <- read.csv(batch_file, sep = ';', header = FALSE)
    names(batch_data) <- batch_colnames
    
    sheet_data <- read.csv(sheet_file)
    cover_data <- read.csv(cover_file)
  }, error = function(e) {
    cat("ERROR reading files for", batch_name, ":", e$message, "\n")
    return(FALSE)
  })
  
  # replace all column names of the 3 files to uppercase
  names(batch_data) <- toupper(names(batch_data))
  names(sheet_data) <- toupper(names(sheet_data))
  names(cover_data) <- toupper(names(cover_data))
  
  ############################################################################
  # PROCESSING
  ############################################################################
  
  # 1: extract herbarium id from image path and notes
  # find duplicates in herbarium id
  # sheet_data <- sheet_data %>%
  #   mutate(ORIGINAL.HERBARIUM.ID = str_extract(PATH.JPG, regex_code_id)) %>%
  #   mutate(HERBARIUM.ID =
  #            ifelse(
  #              !is.na(str_extract(NOTES, regex_code_id)),
  #              str_extract(NOTES, regex_code_id),
  #              str_extract(PATH.JPG, regex_code_id)
  #            ))
  sheet_data <- sheet_data %>%
    mutate(ORIGINAL.HERBARIUM.ID = 
             coalesce(
               str_extract(PATH.JPG, regex_code_id),
               str_extract(PATH.JPG, regex_fiaf_id)
             )) %>%
    mutate(HERBARIUM.ID =
             coalesce(
               str_extract(NOTES, regex_code_id),
               str_extract(PATH.JPG, regex_code_id),
               str_extract(NOTES, regex_fiaf_id),
               str_extract(PATH.JPG, regex_fiaf_id)
             ))
  
  sheet_data <- sheet_data %>%
    group_by(HERBARIUM.ID) %>%
    mutate(ID.DUPLICATE = n() > 1) %>%
    ungroup()
  
  # 1.1: extract cover id from image path and notes
  # find duplicates in cover id (i don't think it is a problem here)
  cover_data <- cover_data %>%
    mutate(COVER.ID = str_extract(PATH.JPG, regex_cover_id))
  
  cover_data <- cover_data %>%
    group_by(COVER.ID) %>%
    mutate(ID.DUPLICATE = n() > 1) %>%
    ungroup()
  
  # 1.2 add the dataset type (BATCH, SHEET, COVER) to the columns to avoid duplicate names
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
  # and the original barcode
  batch_data <- batch_data %>%
    mutate(BATCH.SHEET.ORIGINAL.BARCODE = BATCH.SHEET.BARCODE) %>%
    mutate(BATCH.OLD.ID = str_extract(BATCH.CONNECTIONS, regex_old_id))
  
  ############################################################################
  # 2 merge all data
  ############################################################################
  # nota in un mondo ideale senza problemi funzionerebbe ma connections è un bordello terribile
  # devo quindi partire da sheet
  # 2: create a new row for each sheet in batch written in CONNECTIONS with the SHEET.BARCODE taken from CONNECTIONS
  # new_rows_to_add <- batch_data %>%
  #   filter(BATCH.TYPE == 'Sheet',
  #          !is.na(BATCH.CONNECTIONS),
  #          BATCH.CONNECTIONS != '') %>%
  #   mutate(EXTRACTED.ID = str_extract_all(BATCH.CONNECTIONS, regex_code_id)) %>%
  #   filter(lengths(EXTRACTED.ID) > 0) %>%
  #   mutate(UNIQUE.ID = lapply(EXTRACTED.ID, unique)) %>%
  #   unnest(cols = c(UNIQUE.ID)) %>%
  #   mutate(BATCH.SHEET.BARCODE = UNIQUE.ID) %>%
  #   select(-EXTRACTED.ID, -UNIQUE.ID)
  
  # batch_data <- bind_rows(batch_data, new_rows_to_add)
  
  # 2.1 remove all others with Type != 'Sheet'
  batch_data <- batch_data %>%
    filter(BATCH.TYPE == 'Sheet')
  
  # 2.2: merge sheet and batch
  batch_sheet_data <- sheet_data %>%
    left_join(batch_data,
              by = c('SHEET.ORIGINAL.HERBARIUM.ID' = 'BATCH.SHEET.BARCODE'))
  
  batch_sheet_data <- batch_sheet_data %>% 
    mutate(BATCH.SHEET.BARCODE = SHEET.ORIGINAL.HERBARIUM.ID)
  # change name for duplicates
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
  
  # 2.3: merge sheet_cp and cover
  sheet_batch_cover_data <- batch_sheet_data %>%
    left_join(cover_data, by = c('BATCH.COVER.BARCODE' = 'COVER.COVER.ID'))
  
  ############################################################################
  # SAVE DUPS LOGS
  ############################################################################
  
  # 2.4 save the duplicates_in_sheet.csv
  duplicates_file <- if(file.exists('results/duplicates_in_sheet.csv')) {
    read.csv('results/duplicates_in_sheet.csv') %>%
      mutate(across(everything(), as.character))
  } else {
    data.frame(FILE = character(), SHEET.HERBARIUM.ID = character(), 
               SHEET.ORIGINAL.HERBARIUM.ID = character(), stringsAsFactors = FALSE)
  }
  
  dups <- sheet_data %>%
    filter(SHEET.ID.DUPLICATE == TRUE) %>%
    select(SHEET.HERBARIUM.ID, SHEET.ORIGINAL.HERBARIUM.ID) %>%
    distinct() %>%
    mutate(FILE = batch_file) %>%
    select(FILE, SHEET.HERBARIUM.ID, SHEET.ORIGINAL.HERBARIUM.ID)
  
  # if dups has rows, append them to the duplicates_file if they are not already present
  if (nrow(dups) > 0) {
    new_dups_to_add <- anti_join(dups, duplicates_file, by = c("FILE", "SHEET.HERBARIUM.ID", "SHEET.ORIGINAL.HERBARIUM.ID"))
    if (nrow(new_dups_to_add) > 0) {
      updated_duplicates_log <- bind_rows(duplicates_file, new_dups_to_add)
      write.csv(updated_duplicates_log, 'results/duplicates_in_sheet.csv', row.names = FALSE)
      cat(nrow(new_dups_to_add), "new duplicate records were found and appended\n")
    } else {
      cat("duplicates were found, but they were already present in the log file. No new entries added.\n")
    }
  } else {
    cat("No duplicates found in", batch_file, "\n")
  }
  
  ############################################################################
  # 3: manage multisheet
  ############################################################################
  # not considered problem, for multisheet, only the image with the label appears
  # in the sheet , so, when duplicating, you have to insert the correct image
  # 3.1 create a multisheet
  # - keep only multisheet and group run id
  # - for first add new column first_multisheet (1) and multisheet_reference (itself) otherwise warning if not exist
  # - for not first add a new row with the data of 
  
  # 3.1 tengo solo i multisheet
  batch_data_multisheet <- batch_data %>%
    mutate(group_run_id = consecutive_id(BATCH.COVER.BARCODE, BATCH.COLOR)) %>%
    group_by(group_run_id) %>%
    mutate(
      first_multisheet_index_in_group = which(BATCH.SHEET.TYPE == 'Multisheet')[1],
      multisheet_first_id_in_group = if_else(
        !is.na(first_multisheet_index_in_group),
        BATCH.SHEET.BARCODE[first_multisheet_index_in_group],
        NA_character_
      )
    ) %>%
    mutate(
      is_multisheet_first = !is.na(first_multisheet_index_in_group) &
        (row_number() == first_multisheet_index_in_group)
    ) %>%
    ungroup() %>% 
    filter(!is.na(first_multisheet_index_in_group))
  
  # 3.2 gestisco quella merdosa situazione in cui è sia multisheet che piu campioni sullo stesso foglio
   multisheet_rows_to_add <- batch_data_multisheet %>%
    filter(!is.na(BATCH.CONNECTIONS),
           BATCH.CONNECTIONS != '') %>%
     mutate(
       EXTRACTED.ID = map2(
         str_extract_all(BATCH.CONNECTIONS, regex_code_id),
         str_extract_all(BATCH.CONNECTIONS, regex_fiaf_id),
         ~c(.x, .y)
       )
     ) %>%
     filter(lengths(EXTRACTED.ID) > 0) %>%
     mutate(UNIQUE.ID = lapply(EXTRACTED.ID, unique)) %>%
     unnest(cols = c(UNIQUE.ID)) %>%
     mutate(BATCH.SHEET.BARCODE = UNIQUE.ID) %>%
     select(-EXTRACTED.ID, -UNIQUE.ID) %>% 
     mutate(is_multisheet_first = FALSE)
  
  batch_data_multisheet_all <- bind_rows(batch_data_multisheet, multisheet_rows_to_add)
  
  # ora l'ultima parte rognosa, aggiungerli sheet_batch_cover_data
  # 1: controlla se l'id di BATCH.SHEET.BARCODE (first of group) esiste alrimenti skippa il gruppo
  # 2: per altri duplica il primo e cambia id in 
  # 3: genera id
  batch_data_multisheet_all <- batch_data_multisheet_all %>% 
    arrange(group_run_id, desc(is_multisheet_first))
    
  sheet_batch_cover_data_with_labels <-  process_multisheet_data(
    batch_data_multisheet_all, 
    sheet_batch_cover_data
  )
  
  # Step 3.5: Fix namepath - replace FINAL.ID with multisheet_first_id_in_group in sheet.path.jpg
  sheet_batch_cover_data_final <- sheet_batch_cover_data_with_labels %>% 
  mutate(
    SHEET.PATH.JPG = ifelse(
      !is.na(multisheet_first_id_in_group) & !is_multisheet_first,
      str_replace(SHEET.PATH.JPG, as.character(multisheet_first_id_in_group), as.character(FINAL.ID)),
      SHEET.PATH.JPG
    )
  ) %>%
    ungroup()
  
  ############################################################################
  # 4 add missing sheet data to the log
  ############################################################################
  
  missing_sheets_file <- if(file.exists('results/missing_sheet_in_batch.csv')) {
    read.csv('results/missing_sheet_in_batch.csv') %>%
      mutate(across(everything(), as.character))
  } else {
    data.frame(FILE = character(), BATCH.SHEET.TYPE = character(),
               BATCH.COLOR = character(), BATCH.SHEET.BARCODE = character(),
               BATCH.CONNECTIONS = character(), BATCH.SHEET.ORIGINAL.BARCODE = character(),
               stringsAsFactors = FALSE)
  }
  
  missing_sheets <- sheet_batch_cover_data_final %>%
    filter(is.na(SHEET.APPLICATION.ID)) %>%
    mutate(FILE = batch_file) %>%
    select(FILE, BATCH.SHEET.TYPE, BATCH.COLOR, BATCH.SHEET.BARCODE, BATCH.CONNECTIONS, BATCH.SHEET.ORIGINAL.BARCODE)
  
  # if missing_sheets has rows, append them to the missing_sheets_file if they are not already present
  if (nrow(missing_sheets) > 0) {
    new_missing_sheets_to_add <- anti_join(missing_sheets, missing_sheets_file, by = c("FILE", "BATCH.SHEET.TYPE", "BATCH.COLOR", "BATCH.SHEET.BARCODE", "BATCH.CONNECTIONS", "BATCH.SHEET.ORIGINAL.BARCODE"))
    if (nrow(new_missing_sheets_to_add) > 0) {
      updated_missing_log <- bind_rows(missing_sheets_file, new_missing_sheets_to_add)
      write.csv(updated_missing_log, 'results/missing_sheet_in_batch.csv', row.names = FALSE)
      cat(nrow(new_missing_sheets_to_add), "new missing sheet records were found and appended\n")
    } else {
      cat("missing sheets were found, but they were already present in the log file. No new entries added.\n")
    }
  } else {
    cat("No missing sheets found in", batch_file, "\n")
  }
  
  ############################################################################
  # 5 save processed file in results folder
  ############################################################################
  
  write.csv(sheet_batch_cover_data_final,
            file = paste0("results/", save_file),
            row.names = FALSE,
            na = "")
  
  cat("Successfully processed:", batch_name, "\n")
  return(TRUE)
}

# Main processing loop
main_processing <- function() {
  # Read the processed.csv file
  if (!file.exists("results/processed.csv")) {
    stop("results/processed.csv file not found!")
  }
  
  processed_df <- read.csv("results/processed.csv", stringsAsFactors = FALSE)
  
  # Filter files that have no missing files and are not yet processed
  files_to_process <- processed_df %>%
    filter(missing_files == "" | is.na(missing_files)) %>%
    filter(processed == "FALSE") %>%
    pull(name)
  
  if (length(files_to_process) == 0) {
    cat("No files to process. All eligible files have already been processed.\n")
    return()
  }
  
  cat("Found", length(files_to_process), "files to process:\n")
  print(files_to_process)
  
  # Create results directory if it doesn't exist
  if (!dir.exists("results")) {
    dir.create("results")
  }
  
  # Process each file
  successful_files <- character()
  
  for (file_name in files_to_process) {
    cat("\n" , rep("=", 60), "\n")
    
    success <- tryCatch({
      process_batch(file_name)
    }, error = function(e) {
      cat("ERROR processing", file_name, ":", e$message, "\n")
      FALSE
    })
    
    if (success) {
      successful_files <- c(successful_files, file_name)
    }
  }
  
  # Update the processed.csv file for successful files
  if (length(successful_files) > 0) {
    processed_df <- processed_df %>%
      mutate(processed = ifelse(name %in% successful_files, "TRUE", processed))
    
    write.csv(processed_df, "results/processed.csv", row.names = FALSE)
    
    cat("\n", rep("=", 60), "\n")
    cat("SUMMARY:\n")
    cat("Successfully processed", length(successful_files), "files:\n")
    print(successful_files)
    cat("Updated processed.csv with success status.\n")
  } else {
    cat("\nNo files were successfully processed.\n")
  }
}

# Run the main processing
main_processing()
