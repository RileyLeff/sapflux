library(processx)
library(RSelenium)

get_last_modification_on_gdrive <- function(drive_id) {
    if(!googledrive::drive_has_token()) stop("authenticate gdrive, you heathen!")
    file_info <- googledrive::drive_get(googledrive::as_id(drive_id[[1]]))
    mod_time <- file_info$drive_resource[[1]]$modifiedTime
    mod_time <- as.POSIXct(mod_time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    return(mod_time)
}

do_we_need_to_download_new_copy_of_file <- function(drive_id, local_path) {
    if(!file.exists(local_path)){
        return(TRUE)
    }

    local_mt <- file.info(local_path)$mtime
    remote_mt <- get_last_modification_on_gdrive(drive_id)
    if(remote_mt > local_mt){
        return(TRUE)
    } else {
        return(FALSE)
    }
}

auth_interactive_or_from_path_to_key <- function(
    path_to_key,
    scopes,
    interactive_auth
) {
    if(interactive_auth){
        googledrive::drive_auth()
    } else {
        credentials <- gargle::credentials_service_account(
            path = path_to_key,
            scopes = scopes
        )
        googledrive::drive_auth(
            token = credentials,
            path = path_to_key,
            cache = FALSE
        )
    }
}

authenticate_to_gdrive_if_necessary <- function(
    path_to_key,
    scopes,
    interactive_auth
) {
    if(!googledrive::drive_has_token()){
        auth_interactive_or_from_path_to_key(
            path_to_key,
            scopes,
            interactive_auth
        )
    }
}

start_selenium_container <- function(
    port,
    image,
    container_engine,
    max_attempts,
    delay_per_container_attempt,
    local_download_dir  # Add this parameter
) {
    # Ensure the local directory exists
    dir.create(local_download_dir, recursive = TRUE, showWarnings = FALSE)
    
    # Get absolute path for the download directory
    abs_download_dir <- normalizePath(local_download_dir)
    
    args <- c(
        "run",
        "--rm",
        "-d",
        "-p", sprintf("%d:4444", port),
        "--shm-size", "2g",
        "--dns", "8.8.8.8",
        "--dns", "8.8.4.4",
        "--network", "bridge",
        "-v", sprintf("%s:/downloads", abs_download_dir)  # Mount local dir to /downloads in container
    )

    args <- c(args, image)

    container_process <- process$new(
        command = container_engine,
        args = args,
        stdout = "|",
        stderr = "|",
        cleanup = TRUE,
        supervise = TRUE
    )

    attempt <- 1
    container_ready <- FALSE
    
    while (attempt <= max_attempts && !container_ready) {
        Sys.sleep(delay_per_container_attempt)
        tryCatch({
            resp <- httr::GET(sprintf("http://localhost:%d/wd/hub/status", port))
            if (httr::status_code(resp) == 200) {
                container_ready <- TRUE
                break
            }
        }, error = function(e) {
            message("Waiting for container... (attempt ", attempt, "/", max_attempts, ")")
        })
        attempt <- attempt + 1
    }
    
    if (!container_ready) {
        container_process$kill()
        error_out <- container_process$read_error()
        stdout_out <- container_process$read_output()
        stop(sprintf("Failed to start Selenium container after %d attempts:\nSTDERR: %s\nSTDOUT: %s", 
                    max_attempts, error_out, stdout_out))
    }
    
    message("Selenium container started successfully")
    return(container_process)
}

get_remote_driver <- function(
    remote_server_addr = "localhost",
    browser_name = "chrome",
    port,
    extra_capabilities
){
    remDr <- RSelenium::remoteDriver(
        remoteServerAddr = remote_server_addr,
        port = port,
        browserName = browser_name,
        extraCapabilities = extra_capabilities
    )
    
    tryCatch({
        remDr$open(silent = TRUE)
        return(remDr)
    }, error = function(e) {
        message("Failed to connect to Selenium server. Error: ", e$message)
        stop(e)
    })
}


download_via_selenium <- function(
    lut_entry,
    remote_server_addr = "localhost",
    browser_name = "chrome", 
    port, 
    extra_capabilities,
    wait_time_sec, 
    url_prefix,
) {
    container <- NULL
    remDr <- NULL
    
    tryCatch({
        container <- start_selenium_container(port = port)
        remDr <- get_remote_driver(port = port)

        fullurl <- paste0(url_prefix, lut_entry$id)
        message("Navigating to: ", fullurl)
        remDr$navigate(fullurl)
        
        Sys.sleep(5)  # Wait for page load
        
        message("Finding download button...")
        download_all <- wait_and_find_download_button(remDr)
        
        message("Clicking download button...")
        download_all$clickElement()
        
        Sys.sleep(wait_time_sec)  # Wait for download
        message("Download initiated")
        
    }, error = function(e) {
        if (!is.null(remDr)) {
            tryCatch({
                remDr$close()
            }, error = function(e) {
                message("Failed to close browser: ", e$message)
            })
        }
        if (!is.null(container)) {
            container$kill()
        }
        stop(e)
    }, finally = {
        if (!is.null(remDr)) {
            tryCatch({
                remDr$close()
            }, error = function(e) {
                message("Failed to close browser: ", e$message)
            })
        }
        if (!is.null(container)) {
            container$kill()
        }
    })
}

download_via_api <- function(id, local_path){
    print("in download via api")
    authenticate_to_gdrive_if_necessary()
    if(do_we_need_to_download_new_copy_of_file(id, local_path)){
        googledrive::drive_download(
            file = googledrive::as_id(id),
            path = local_path
        )
    }
}

sync_external_data <- function(lut_entry, port, wait_time_sec){
    this_id <- lut_entry$id
    if(lut_entry$download_via == "api"){
        download_via_api(
            this_id, 
            lut_entry$local_path
        )
    } else if(lut_entry$download_via == "selenium"){
        download_via_selenium(
            lut_entry,
            port,
            wait_time_sec
        )
    }
}

sync_all_external_data <- function(luts, port, wait_time_sec){
    for(this_lut in luts){
        message("Syncing: ", this_lut$local_path)
        sync_external_data(
            this_lut,
            port,
            wait_time_sec
        )
        Sys.sleep(wait_time_sec)
    }
}