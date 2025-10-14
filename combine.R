# COMBINE PROCESSED FILES SCRIPT
# This script combines all processed files that start with "processed_CP" in the results folder

# LIBRARIES
library(dplyr)

# Function to combine all processed files
combine_processed_files <- function() {
  cat("Starting to combine processed files...\n")
  
  # Check if results directory exists
  if (!dir.exists("results")) {
    stop("Results directory not found!")
  }
  
  # Get list of all files that start with "processed_CP" and end with ".csv"
  processed_files <- list.files(
    path = "results", 
    pattern = "^processed_CP.*\\.csv$", 
    full.names = TRUE
  )
  
  if (length(processed_files) == 0) {
    cat("No processed files found that start with 'processed_CP'\n")
    return()
  }
  
  cat("Found", length(processed_files), "files to combine:\n")
  print(basename(processed_files))
  
  # Initialize empty list to store dataframes
  all_data <- list()
  
  # Read each file and store in list
  for (i in seq_along(processed_files)) {
    file_path <- processed_files[i]
    file_name <- basename(file_path)
    
    cat("Reading file", i, "of", length(processed_files), ":", file_name, "\n")
    
    tryCatch({
      # Read the CSV file
      df <- read.csv(file_path, stringsAsFactors = FALSE)
      
      # Convert all columns to character to avoid type conflicts
      df <- df %>% mutate(across(everything(), as.character))
      
      # Add a column to track source file
      df$SOURCE_FILE <- file_name
      
      # Store in list
      all_data[[i]] <- df
      
      cat("Successfully read", nrow(df), "rows from", file_name, "\n")
      
    }, error = function(e) {
      cat("ERROR reading", file_name, ":", e$message, "\n")
    })
  }
  
  # Remove NULL elements (failed reads)
  all_data <- all_data[!sapply(all_data, is.null)]
  
  if (length(all_data) == 0) {
    cat("No files were successfully read\n")
    return()
  }
  
  # Combine all dataframes
  cat("Combining all dataframes...\n")
  
  tryCatch({
    combined_data <- bind_rows(all_data)
    
    # Create output filename with timestamp
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    output_file <- "results/combined.csv"
    
    # Write combined data to CSV
    write.csv(combined_data, 
              file = output_file, 
              row.names = FALSE, 
              na = "")
    
    cat("Successfully combined", length(all_data), "files\n")
    cat("Total rows in combined dataset:", nrow(combined_data), "\n")
    cat("Total columns in combined dataset:", ncol(combined_data), "\n")
    cat("Combined file saved as:", output_file, "\n")
    
    # Print summary by source file
    cat("\nSummary by source file:\n")
    summary_by_file <- combined_data %>%
      group_by(SOURCE_FILE) %>%
      summarise(
        row_count = n(),
        .groups = "drop"
      ) %>%
      arrange(SOURCE_FILE)
    
    print(summary_by_file)
    
    return(combined_data)
    
  }, error = function(e) {
    cat("ERROR combining files:", e$message, "\n")
  })
}

# Run the combination function
combined_result <- combine_processed_files()

# Optional: Display basic info about the combined dataset
if (exists("combined_result") && !is.null(combined_result)) {
  cat("\n", rep("=", 60), "\n")
  cat("FINAL SUMMARY:\n")
  cat("Combined dataset contains", nrow(combined_result), "rows and", ncol(combined_result), "columns\n")
  
  # Show column names
  cat("\nColumn names:\n")
  print(colnames(combined_result))
}

rdf <- combined_result %>%
  filter(!startsWith(FINAL.ID, "FI-HCI") & 
           !startsWith(FINAL.ID, "FIAF"))
