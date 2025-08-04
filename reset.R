# remove all files in results folder



files <- list.files('results', full.names = TRUE)
if (length(files) > 0) {
  file.remove(files)
}
