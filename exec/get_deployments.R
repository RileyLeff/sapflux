source("exec/get_constants.R")

#' Downloads deployments from google drive.
#' 
#' @param path A character (string) that represents a valid path. This function doesn't perform any of its own validation on the input path, make sure that it's valid and is a folder.
#' @param forbidden_words A list of characters (strings) where, if they appear in a file path, we know we don't want to read in that file.
#' @return A vector of acceptable file paths.
#' @examples
#' # e.g. if the folder "data/raw" contains file1.csv, file2.csv, and file3yowza.csv
#' get_valid_file_paths("data/raw", forbidden_words = c("yowza")) # returns something like c("data/raw/file1.csv", "data/raw/file2.csv")
download_deployments <- function(
    id = constants$resource_ids$deployment_resource_id,
    save_to = "data/deployments.csv"
) {
    googledrive::drive_deauth()
    googledrive::drive_download(
        googledrive::as_id(id),
        path = save_to
    )
}

readin_deployments <- function(path = "data/deployments.csv"){
    dply <- read.csv(path)
    dply$start_ts_utc <- as.POSIXct(dply$start_ts_utc, tz = "UTC")
    return(dply)
}

get_deployments <- function(
    id = constants$resource_ids$deployment_resource_id, 
    path = "data/deployments.csv"
    save_to = "data/deployments.csv"
) {
    download_deployments(id, save_to)
    x <- readin_deployments(path)
    return(x)
}