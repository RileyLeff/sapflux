dir_of_interest <- "data/.cache"
unzipped_dir <- "data/sap_flux_dont_touch"
file_of_interest <- list.files(dir_of_interest, full.names = TRUE)

unzip(
    zipfile = path_of_interest,
    exdir = "data"
)

fs::file_move(list.files(unzipped_dir, full.names = TRUE), "data")
fs::file_delete(unzipped_dir)
