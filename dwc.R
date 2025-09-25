# da fare
# rimuovi tutto il garbagio
# nome nelle cover in scientificName e nome nelle sheet in verbatimScientificName
# localita ok forse un id
# date da sistemare ma facile
# immagini le sposti in simple multimedia o audiovisual
# ho legit
# ho catalogNumber = occurrenceId
# altitude un casino
# - devi considerare unita di misura
# - devi vedere come hanno messo il range
# - devi tenere verbatimAltitude

# in italy can be determined by the cover type

# carica combined 
# 1: rimuovi invalidi
# 2: fai date
library(dplyr)
combined <- read.csv("~/Desktop/post-doc/firenze/results/combined.csv")
combined_valid <- combined %>% 
  filter(SHEET.TAXONOMY != '') %>% 
  select(SHEET.PATH.JPG,
         SHEET.LOCATION,
         SHEET.COUNTRY,
         SHEET.ALTITUDE,
         SHEET.COLLECTOR,
         SHEET.TAXONOMY,
         SHEET.DATE.OF.COLLECTION,
         SHEET.NAME.OF.AUTHORS.OF.THE.FIRST.IDENTIFICATION,
         FINAL.ID)

combined_good <- combined_valid %>% 
  rename(
    associatedMedia = SHEET.PATH.JPG,
    locality = SHEET.LOCATION,
    country = SHEET.COUNTRY,
    verbatimElevation = SHEET.ALTITUDE,
    recordedBy = SHEET.COLLECTOR,
    scientificName = SHEET.TAXONOMY,
    verbatimEventDate = SHEET.DATE.OF.COLLECTION,
    identifiedBy = SHEET.NAME.OF.AUTHORS.OF.THE.FIRST.IDENTIFICATION,
    catalogNumber = FINAL.ID
  )


library(stringr)
library(tidyr)

combined_good <- combined_good %>%
  # Clean verbatimEventDate: keep only numbers and /
  mutate(
    verbatimEventDate_clean = str_replace_all(verbatimEventDate, "[^0-9/]", "")
  ) %>%
  
  # Split by / and extract day, month, year
  separate(verbatimEventDate_clean, 
           into = c("day", "month", "year"), 
           sep = "/", 
           fill = "right", 
           remove = FALSE) %>%
  
  # Convert to numeric and handle empty strings as NA
  mutate(
    day = ifelse(day == "" | is.na(day), NA, as.numeric(day)),
    month = ifelse(month == "" | is.na(month), NA, as.numeric(month)),
    year = ifelse(year == "" | is.na(year), NA, as.numeric(year))
  ) %>%
  
  # Create eventDate in ISO format (yyyy-mm-dd) only when all components are present
  mutate(
    eventDate = case_when(
      !is.na(year) & !is.na(month) & !is.na(day) & 
        year >= 1000 & year <= 9999 & 
        month >= 1 & month <= 12 & 
        day >= 1 & day <= 31 ~ sprintf("%04d-%02d-%02d", year, month, day),
      TRUE ~ NA_character_
    )
  ) %>%
  
  # Remove the temporary cleaning column
  select(-verbatimEventDate_clean)

names(combined_good)

# Reorder columns in a logical sequence
combined_good <- combined_good %>%
  select(
    catalogNumber,          # 1. Specimen identifier (primary key)
    scientificName,         # 2. What it is (taxonomy)
         # 3. Who collected it
    verbatimEventDate,      # 4. Original date as recorded
    day, month, year,       # 5. Parsed date components  
    eventDate,              # 6. Standardized ISO date
    country,                # 7. Where: Country
    locality,               # 8. Where: Specific location
    verbatimElevation,  
    recordedBy,     # 9. Where: Elevation
    identifiedBy,           # 10. Who identified it
    associatedMedia         # 11. Associated files/images
  )

# Load required libraries
library(digest)
library(base64enc)
library(dplyr)

# Thumbor configuration
THUMBOR_URL <- "https://137.204.119.230/img"
THUMBOR_KEY <- "Luc_&&vjDV7Jj2j"

# Function to parse image path and build true path (following PHP logic)
build_true_path <- function(image_string) {
  if (is.na(image_string) || image_string == "") {
    return(NA_character_)
  }
  
  # Pattern to match /imagePath/imageName (like PHP IMAGE_PATH_PATTERN)
  image_match <- regmatches(image_string, regexpr("^/(.*)/(.*?)$", image_string, perl = TRUE))
  if (length(image_match) == 0) {
    stop(paste("Invalid image string format:", image_string))
  }
  
  # Extract imagePath and imageName
  parts <- strsplit(gsub("^/", "", image_match), "/")[[1]]
  if (length(parts) < 2) {
    stop(paste("Invalid image string format:", image_string))
  }
  
  image_path <- parts[1]
  image_name <- parts[2]
  
  # Pattern to extract CP, year, month, day from imagePath (like PHP PATH_PARTS_PATTERN)
  path_match <- regmatches(image_path, regexpr("^(CP\\d+)_(\\d{4})(\\d{2})(\\d{2})", image_path, perl = TRUE))
  if (length(path_match) == 0) {
    stop(paste("Invalid image path format:", image_path))
  }
  
  # Extract components using substring (since R regmatches doesn't capture groups easily)
  cp <- substr(path_match, 1, regexpr("_", path_match) - 1)
  date_part <- substr(path_match, regexpr("_", path_match) + 1, nchar(path_match))
  
  year <- substr(date_part, 1, 4)
  month <- substr(date_part, 5, 6)
  day <- substr(date_part, 7, 8)
  
  # Build true path: year/month/day/cp/imagePath/JPG/imageName
  true_path <- paste(year, month, day, cp, image_path, "JPG", image_name, sep = "/")
  
  return(true_path)
}

# Function to generate safe Thumbor URL
generate_thumbor_url <- function(image_string) {
  if (is.na(image_string) || image_string == "") {
    return(NA_character_)
  }
  
  # First build the true path
  true_path <- build_true_path(image_string)
  if (is.na(true_path)) {
    return(NA_character_)
  }
  
  # Create path to sign with Erbari prefix
  path_to_sign <- paste0("Erbari/", true_path)
  
  # Generate HMAC-SHA1 signature
  raw_hash <- hmac(THUMBOR_KEY, path_to_sign, algo = "sha1", raw = TRUE)
  
  # Convert to URL-safe base64 (replace +/- with -_, remove padding)
  signature <- gsub("+$", "", gsub("/", "_", gsub("\\+", "-", base64encode(raw_hash))))
  
  # Build final URL
  paste0(THUMBOR_URL, "/", signature, "/", path_to_sign)
}

# Add url column to your dataframe
df <- combined_good %>%
  mutate(url = sapply(associatedMedia, function(x) {
    tryCatch(
      generate_thumbor_url(x),
      error = function(e) {
        message("Error processing image: ", x, " - ", e$message)
        return(NA_character_)
      }
    )
  }))
write.csv(df, 'results/dwc.csv', na="")
