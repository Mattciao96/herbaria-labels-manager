# VARIABLES
data_dir <- "data"
results_dir <- "results"

# CHECK FILES

# 1: check if processed.csv exists
if (file.exists(paste0(results_dir,'processed.csv'))) stop('processed.csv already exist')
if (!file.exists("processed.template.csv")) stop('processed.template.csv is missing')

# 2: check folders exists
if (!dir.exists(data_dir)) stop(paste0(data_dir, "does not exist"))
if (!dir.exists(results_dir)) stop(paste0(results_dir, "does not exist"))


