# Multisheet Batch Data Processing Script
# =========================================

# Load required libraries
library(dplyr)
library(tidyr)

# Function to add new columns to sheet_batch_cover_data
initialize_new_columns <- function(sheet_data) {
  sheet_data <- sheet_data %>%
    mutate(
      group_run_id = NA,
      multisheet_first_id_in_group = NA,
      is_multisheet_first = NA
    )
  return(sheet_data)
}

# Function to get matching columns between two dataframes
get_matching_columns <- function(source_df, target_df) {
  # Find columns that exist in both dataframes
  matching_cols <- intersect(names(source_df), names(target_df))
  return(matching_cols)
}

# Function to process a single group
process_multisheet_group <- function(group_data, sheet_data, matching_cols) {
  # Initialize result dataframe
  result_sheet_data <- sheet_data
  
  # Sort to ensure the first row has is_multisheet_first = TRUE
  group_data <- group_data %>%
    arrange(desc(is_multisheet_first))
  
  # Get the first row (should have is_multisheet_first = TRUE)
  first_row <- group_data[1, ]
  
  # Validate first row
  if (!first_row$is_multisheet_first) {
    warning(paste("Group", first_row$group_run_id, 
                  "doesn't have a row with is_multisheet_first = TRUE as first"))
    return(result_sheet_data)
  }
  
  # Process first row (is_multisheet_first = TRUE)
  first_barcode <- first_row$BATCH.SHEET.BARCODE
  
  # Check if first barcode exists in FINAL.ID
  matching_idx <- which(result_sheet_data$FINAL.ID == first_barcode)
  
  if (length(matching_idx) == 0) {
    # Skip to next group if not found
    return(result_sheet_data)
  }
  
  # Update the matching row with group information
  result_sheet_data[matching_idx, "group_run_id"] <- first_row$group_run_id
  result_sheet_data[matching_idx, "multisheet_first_id_in_group"] <- first_row$multisheet_first_id_in_group
  result_sheet_data[matching_idx, "is_multisheet_first"] <- first_row$is_multisheet_first
  
  # Process remaining rows (is_multisheet_first = FALSE)
  if (nrow(group_data) > 1) {
    for (i in 2:nrow(group_data)) {
      current_row <- group_data[i, ]
      
      # Get the multisheet_first_id_in_group value
      first_id <- current_row$multisheet_first_id_in_group
      
      # Check if this ID exists in FINAL.ID
      template_idx <- which(result_sheet_data$FINAL.ID == first_id)
      
      if (length(template_idx) == 0) {
        # Skip to next row if not found
        next
      }
      
      # Create new row based on template
      new_row <- result_sheet_data[template_idx[1], ]
      
      # Replace FINAL.ID with current BATCH.SHEET.BARCODE
      new_row$FINAL.ID <- current_row$BATCH.SHEET.BARCODE
      
      # Update matching columns from batch_data
      for (col in matching_cols) {
        if (col %in% names(current_row) && col %in% names(new_row)) {
          new_row[[col]] <- current_row[[col]]
        }
      }
      
      # Add group information columns
      new_row$group_run_id <- current_row$group_run_id
      new_row$multisheet_first_id_in_group <- current_row$multisheet_first_id_in_group
      new_row$is_multisheet_first <- current_row$is_multisheet_first
      
      # Append new row to result
      result_sheet_data <- rbind(result_sheet_data, new_row)
    }
  }
  
  return(result_sheet_data)
}

# Main processing function
process_multisheet_data <- function(batch_data_multisheet_all, sheet_batch_cover_data) {
  
  # Step 1: Initialize new columns in sheet_batch_cover_data
  cat("Step 1: Adding new columns to sheet_batch_cover_data...\n")
  sheet_batch_cover_data <- initialize_new_columns(sheet_batch_cover_data)
  
  # Step 2: Get matching columns between datasets
  cat("Step 2: Identifying matching columns...\n")
  matching_cols <- get_matching_columns(batch_data_multisheet_all, sheet_batch_cover_data)
  cat(paste("Found", length(matching_cols), "matching columns\n"))
  
  # Step 3: Get unique group IDs
  group_ids <- unique(batch_data_multisheet_all$group_run_id)
  cat(paste("Step 3: Processing", length(group_ids), "groups...\n"))
  
  # Step 4: Process each group
  result_data <- sheet_batch_cover_data
  
  for (gid in group_ids) {
    # Get data for current group
    group_data <- batch_data_multisheet_all %>%
      filter(group_run_id == gid)
    
    # Process the group
    result_data <- process_multisheet_group(group_data, result_data, matching_cols)
    
    # Progress indicator
    cat(paste("  Processed group:", gid, "\n"))
  }
  
  cat("Processing complete!\n")
  
  # Summary statistics
  cat("\nSummary:\n")
  cat(paste("  Original sheet_batch_cover_data rows:", nrow(sheet_batch_cover_data), "\n"))
  cat(paste("  Final sheet_batch_cover_data rows:", nrow(result_data), "\n"))
  cat(paste("  New rows added:", nrow(result_data) - nrow(sheet_batch_cover_data), "\n"))
  
  rows_with_group <- sum(!is.na(result_data$group_run_id))
  cat(paste("  Rows with group information:", rows_with_group, "\n"))
  
  return(result_data)
}