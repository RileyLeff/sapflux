library(processx)
library(RSelenium)
library(httr)

get_id_from_multi_id_case_or_throw_error <- function(lut_entry){
    if(!("id_determiner" %in% names(lut_entry))) {
        stop("Missing an id_determiner function for an external dataset with multiple possible IDs.")
    } else if(!existsFunction(lut_entry$id_determiner)) {
        stop("id_determiner doesn't point to a valid function in current namespace.")
    }
    id_determiner_fn <- eval(parse(text = lut_entry$id_determiner))
    which_id_to_grab <- id_determiner_fn()
    
    if(!(which_id_to_grab %in% names(lut_entry$id))) {
        stop("id_determiner function produced an invalid possible_id name.")
    }

    correct_id <- lut_entry$id[which_id_to_grab]
    return(correct_id)
}

get_id_or_throw_error <- function(lut_entry){
    if(length(lut_entry$id) == 1){
        this_id <- lut_entry$id
    } else {
        this_id <- get_id_from_multi_id_case_or_throw_error(lut_entry)
    }
    return(this_id)
}

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
    path_to_key = constants$path_to_key,
    env_key = constants$env_key
) {
    if(Sys.getenv(env_key) == "interactive"){
        googledrive::drive_auth()
    } else {
        googledrive::drive_auth(
            path = path_to_key,
            token = gargle::credentials_service_account(path = path_to_key),
            scopes = "https://www.googleapis.com/auth/drive.readonly",
            cache = FALSE
        )
    }
}

authenticate_to_gdrive_if_necessary <- function(
    path_to_key = constants$path_to_key,
    env_key = constants$env_key
) {
    if(!googledrive::drive_has_token()){
        auth_interactive_or_from_path_to_key(
            path_to_key,
            env_key
        )
    }
}

start_selenium_container <- function(
    port = constants$selenium_port,
    image = "selenium/standalone-chrome:3.141.59",
    container_engine = Sys.getenv("CONTAINER_ENGINE", "docker")
) {
    message("Starting Selenium container...")
    
    # Check if we're running in GitHub Actions
    is_github_actions <- Sys.getenv("GITHUB_ACTIONS") == "true"
    
    # Adjust arguments based on environment
    args <- c(
        "run",
        "--rm",
        "-d",  # Run in detached mode
        "-p", sprintf("%d:4444", port),
        "--shm-size", "2g"
    )
    
    # Add VNC ports only if not in CI
    if (!is_github_actions) {
        args <- c(args,
            "-p", "5900:5900",
            "-p", "7900:7900"
        )
    }
    
    args <- c(args, image)
    
    message("Starting container with command: ", container_engine, " ", paste(args, collapse = " "))
    
    container_process <- process$new(
        command = container_engine,
        args = args,
        stdout = "|",
        stderr = "|",
        cleanup = TRUE,
        supervise = TRUE
    )
    
    # Wait for container to be ready
    max_attempts <- 10
    attempt <- 1
    container_ready <- FALSE
    
    while (attempt <= max_attempts && !container_ready) {
        Sys.sleep(3)
        tryCatch({
            resp <- httr::GET(sprintf("http://localhost:%d/wd/hub/status", port))
            if (httr::status_code(resp) == 200) {
                container_ready <- TRUE
                break
            }
        }, error = function(e) {
            message("Waiting for container to be ready... (attempt ", attempt, "/", max_attempts, ")")
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
    port = constants$selenium_port
){
    remDr <- RSelenium::remoteDriver(
        remoteServerAddr = "localhost",
        port = port,
        browserName = "chrome"
    )
    
    tryCatch({
        remDr$open(silent = TRUE)
        return(remDr)
    }, error = function(e) {
        message("Failed to connect to Selenium server. Error: ", e$message)
        stop(e)
    })
}

download_via_api <- function(id, local_path){
    authenticate_to_gdrive_if_necessary()
    if(do_we_need_to_download_new_copy_of_file(id, local_path)){
        googledrive::drive_download(
            file = googledrive::as_id(id),
            path = local_path
        )
    }
}

download_via_selenium <- function(
    lut_entry,
    port = constants$selenium_port,
    wait_time_sec = constants$selenium_server_startup_wait_time_sec,
    url_prefix = constants$drive_url_prefix
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
        download_all <- remDr$findElement(
            using = "xpath",
            value = constants$download_button_finder
        )
        
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

sync_external_data <- function(
    lut_entry,
    port = constants$selenium_port,
    wait_time_sec = constants$selenium_server_startup_wait_time_sec
){
    this_id <- get_id_or_throw_error(lut_entry)
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

sync_all_external_data <- function(
    luts = constants$externals,
    port = constants$selenium_port,
    wait_time_sec = constants$selenium_server_startup_wait_time_sec
){
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