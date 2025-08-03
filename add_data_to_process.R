# LIBS
library(dplyr)

# VARIABLES
data_dir <- "data"
results_dir <- "results"

# UTILS FUNCTIONS


# CHECK FILES

# 1: check if processed.csv exists
if (file.exists(paste0(results_dir,'processed.csv'))) stop('processed.csv already exist')
if (!file.exists("processed.template.csv")) stop('processed.template.csv is missing')

# 2: check folders exists
if (!dir.exists(data_dir)) stop(paste0(data_dir, "does not exist"))
if (!dir.exists(paste0(data_dir, '/Connection'))) stop("Connection does not exist")
if (!dir.exists(paste0(data_dir, '/Cover'))) stop("Cover does not exist")
if (!dir.exists(paste0(data_dir, '/Sheets'))) stop("Sheets does not exist")

# 3: process with missing files notes
processed_data_index = read.csv('processed.template.csv')
connection_files <- connection_files %>% sort()

for (i in 1: length(connection_files)) {
  has_sheet <- FALSE
  has_cover <- FALSE
  
  if (file.exists(paste0(data_dir, '/Sheets/', 'SHEET_SHEET_', connection_files[i]))) {
    has_sheet <- TRUE
  }
  if (file.exists(paste0(data_dir, '/Cover/', 'COVER_COVER_', connection_files[i]))) {
    has_cover <- TRUE
  }
  
  missing_files_data <- ''
  if (!has_sheet & !has_cover) {
    missing_files_data <- 'SHEET | COVER'
  } else if (!has_sheet) {
    missing_files_data <- 'SHEET'
  } else if (!has_cover) {
    missing_files_data <- 'COVER'
  }
  
  
  processed_data_index <- rbind(processed_data_index, 
                            data.frame(
                              name = connection_files[i],
                              missing_files = missing_files_data,
                              processed = 'FALSE'
                            ))
}


write.csv(processed_data_index, paste0(results_dir, '/', 'processed.csv'), row.names = FALSE)


# 4: generate other files from templates
missing_sheet_in_batch <- read.csv('missing_sheet_in_batch.template.csv')
duplicates_in_sheet <- read.csv('duplicates_in_sheet.template.csv')

write.csv(missing_sheet_in_batch, paste0(results_dir, '/', 'missing_sheet_in_batch.csv'), row.names = FALSE)
write.csv(duplicates_in_sheet, paste0(results_dir, '/', 'duplicates_in_sheet.csv'), row.names = FALSE)
