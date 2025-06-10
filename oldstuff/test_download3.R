source("R/get_constants.R")
source("R/external_file_util_3.R")
sync_external_data(
    entries = constants$externals,
    key_path = constants$google_drive$path_to_key,
    scopes = constants$google_drive$scopes,
    selenium_port = 4445,
    max_start_time = 120,
    max_completion_time = 120,
    dns_servers = constants$network$default$dns_servers,
    network_mode = constants$network$default$network_mode,
    container_download_dir = "Downloads",
    config = NA
)

# First, start a container and get a driver manually:
container <- start_selenium_container(
    dns_servers = constants$network$gw$dns_servers,
    network_mode = constants$network$gw$network_mode,
)

remDr <- get_remote_driver(port = 4445)

# Now you can try commands interactively:
# Test basic navigation
remDr$navigate("https://www.google.com")  # Test with simple site first

# Try the Drive URL
fullurl <- "https://drive.google.com/drive/folders/147L5_KrDVFfFyIScO1jkWNXgJeGPqDbz?usp=drive_link"
remDr$navigate(fullurl)

# If it works, try finding the button
download_all <- wait_and_find_download_button(remDr)
download_all$clickElement()

# Check what's actually on the page
remDr$getPageSource()[[1]]  # View page HTML
remDr$getCurrentUrl()       # See if we got redirected

# Clean up when done
remDr$close()
container$kill()