library(processx)
library(RSelenium)

#' Get Last Modification Time of Google Drive Resource
#' 
#' Retrieves the last modification time of a Google Drive resource.
#' Requires pre-existing Google Drive authentication.
#' 
#' @param drive_id Character string. Google Drive file ID.
#' @return POSIXct timestamp in UTC timezone representing last modification time.
#' @throws Error if not authenticated to Google Drive.
#' @note Requires Google Drive authentication.
#' @examples
#' \dontrun{
#' # Must authenticate first
#' googledrive::drive_auth()
#' mod_time <- get_last_modification_on_gdrive("file_id_here")
#' }
get_last_modification_on_gdrive <- function(drive_id) {
    if(!googledrive::drive_has_token()) stop("must authenticate to gdrive for get_last_modification_on_gdrive() to work!")
    file_info <- googledrive::drive_get(googledrive::as_id(drive_id[[1]]))
    mod_time <- file_info$drive_resource[[1]]$modifiedTime
    mod_time <- as.POSIXct(mod_time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    return(mod_time)
}

#' Check if Local File Needs Updating
#' 
#' Compares local and remote file modification times to determine if 
#' a new download is needed.
#' 
#' @param drive_id Character string. Google Drive file ID.
#' @param local_path Character string. Path to local file.
#' @return Logical. TRUE if file needs updating or doesn't exist locally.
#' @note Requires Google Drive authentication.
file_is_stale <- function(drive_id, local_path) {
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

#' Authenticate to Google Drive
#' 
#' Handles Google Drive authentication either interactively or using service account credentials.
#' 
#' @param path_to_key Character string. Required only for non-interactive authentication. Path to service account key file.
#' @param scopes Character vector. Required only for non-interactive authentication. OAuth scopes for Google Drive access.
#' @param interactive_auth Logical. Whether to use interactive authentication.
#' @note This function has side effects (modifies global authentication state).
#' @return Nothing.
auth_interactive_or_from_path_to_key <- function(
    path_to_key = NULL,
    scopes = NULL,
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

#' Conditionally Authenticate to Google Drive
#' 
#' Checks if there's an existing Google Drive token and authenticates only if necessary.
#' This function serves as a guard to prevent redundant authentication attempts.
#' 
#' @param path_to_key Character string. Path to the service account key JSON file.
#'        Only used if interactive_auth is FALSE.
#' @param scopes Character vector. OAuth scopes required for Google Drive access.
#'        See \url{https://developers.google.com/identity/protocols/oauth2/scopes#drive}
#'        for available scopes.
#' @param interactive_auth Logical. If TRUE, prompts for interactive authentication.
#'        If FALSE, uses service account authentication with provided key file.
#' 
#' @return Nothing.
#' 
#' @note This function has side effects: it may modify the global authentication state
#'       and may prompt for user interaction if interactive_auth is TRUE.
#' 
#' @examples
#' \dontrun{
#' # Interactive authentication
#' authenticate_to_gdrive_if_necessary(
#'     path_to_key = "path/to/key.json",
#'     scopes = c("https://www.googleapis.com/auth/drive.readonly"),
#'     interactive_auth = TRUE
#' )
#' 
#' # Service account authentication
#' authenticate_to_gdrive_if_necessary(
#'     path_to_key = ".env/service-account-key.json",
#'     scopes = c(
#'         "https://www.googleapis.com/auth/drive.readonly",
#'         "https://www.googleapis.com/auth/drive.file"
#'     ),
#'     interactive_auth = FALSE
#' )
#' }
#' 
#' @seealso 
#' \code{\link[googledrive]{drive_auth}}, 
#' \code{\link[gargle]{credentials_service_account}}
#' 
#' @export
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

#' Start Selenium Container
#' 
#' Starts a Selenium container with specified configuration and waits for it to be ready.
#' 
#' @param port Integer. Port to expose Selenium on host.
#' @param image Character string. Docker/Podman image name.
#' @param container_engine Character string. Container runtime to use (e.g., "podman").
#' @param max_attempts Integer. Maximum number of connection attempts.
#' @param delay_per_container_attempt Numeric. Seconds to wait between attempts.
#' @param local_download_dir Character string. Local directory to mount for downloads.
#' @return processx object representing the container process.
#' @note Creates directories and starts container process (side effects).
start_selenium_container <- function(
    port,
    image,
    container_engine,
    max_attempts,
    delay_per_container_attempt,
    local_download_dir
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

#' Create Remote WebDriver Connection
#' 
#' Creates and initializes a Selenium WebDriver connection.
#' 
#' @param remote_server_addr Character string. Selenium server address.
#' @param browser_name Character string. Browser to use.
#' @param port Integer. Port number for Selenium server.
#' @param extra_capabilities List. Browser-specific capabilities.
#' @return RSelenium remote driver object.
get_remote_driver <- function(
    remote_server_addr = "localhost",
    browser_name = "chrome",
    port,
    extra_capabilities = NULL
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
    url_prefix
) {
    container <- NULL
    remDr <- NULL
    
    tryCatch({
        container <- start_selenium_container(port = port)
        remDr <- get_remote_driver(port = port)

        fullurl <- paste0(url_prefix, lut_entry$id)
        message("Navigating to: ", fullurl)
        remDr$navigate(fullurl)
        
        Sys.sleep(1)  # Wait for page load
        
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

#' Split LUT Entries by Download Method
#' 
#' Takes a list of LUT entries and groups them by their download method.
#' 
#' @param luts List of LUT entries, each containing at least download_via field
#' @param valid_methods Character vector of allowed download methods. 
#'        Default c("selenium", "api")
#' @return List of lists, grouped by download_via value
#' @examples
#' split_luts_by_download_method(constants$externals)
#' 
#' # With custom valid methods
#' split_luts_by_download_method(
#'   constants$externals, 
#'   valid_methods = c("google_drive_api", "selenium")
#' )
split_luts_by_download_method <- function(
    luts,
    valid_methods = c("selenium", "api")
) {
    # Handle single entry case
    if (!is.null(luts$download_via)) luts <- list(luts)
    
    # Validate input structure
    if (!all(sapply(luts, function(x) "download_via" %in% names(x)))) {
        stop("All entries must contain 'download_via' field")
    }
    
    # Get actual methods present in data
    methods_present <- unique(sapply(luts, `[[`, "download_via"))
    
    # Validate against allowed methods
    invalid_methods <- setdiff(methods_present, valid_methods)
    if (length(invalid_methods) > 0) {
        stop(
            "Invalid download methods found: ", 
            paste(invalid_methods, collapse = ", "), 
            "\nValid methods are: ", 
            paste(valid_methods, collapse = ", ")
        )
    }
    
    # Split into list by download method
    result <- split(luts, sapply(luts, `[[`, "download_via"))
    
    # Ensure all valid methods have an entry (possibly empty)
    result <- structure(
        lapply(setNames(valid_methods, valid_methods), function(method) {
            result[[method]] %||% list()
        }),
        class = "download_method_split"
    )
    
    return(result)
}

# Optional: Add print method for nicer output
#' @export
print.download_method_split <- function(x, ...) {
    cat("Download methods split:\n")
    for (method in names(x)) {
        n_entries <- length(x[[method]])
        cat(sprintf("  %s: %d entries\n", method, n_entries))
        if (n_entries > 0) {
            paths <- sapply(x[[method]], `[[`, "local_path")
            cat("    Files:", paste(basename(paths), collapse = ", "), "\n")
        }
    }
    invisible(x)
}

sync_external_data(luts){
    with_authentication(
        luts <- luts[[sapply(luts, file_is_stale)]]
    )
    
}

