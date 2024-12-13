get_deployments <- function(path = "data/deployments.csv"){
    dply <- read.csv(path)
    dply$start_ts_utc <- as.POSIXct(dply$start_ts_utc, tz = "UTC")
    return(dply)
}