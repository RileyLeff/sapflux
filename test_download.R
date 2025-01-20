source("R/get_constants.R")
source("R/external_file_util.R")

library(RSelenium)

container <- start_selenium_container(
    port = 4445,
    image = constants$selenium$container_name,
    container_engine = "podman",
    max_attempts = 5, 
    delay_per_container_attempt = 3
)

remDr <- get_remote_driver(port = 4445L, download_dir = "/downloads")
fullurl <- paste0(constants$google_drive$url_prefix, constants$externals$raw$id)
remDr$navigate(fullurl)
remDr$getTitle()

button <- remDr$findElement(using = "xpath", constants$selenium$selectors$elements[[1]]$value)
button$clickElement()
