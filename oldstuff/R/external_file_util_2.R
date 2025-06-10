#' Authenticate to Google Drive
#' 
#' Handles Google Drive authentication either interactively or using service account credentials.
#' 
#' @param path_to_key Character string. Required only for non-interactive authentication. Path to service account key file.
#' @param scopes Character vector. Required only for non-interactive authentication. OAuth scopes for Google Drive access.
#' @param interactive Logical. Whether to use interactive authentication.
#' @note This function has side effects (modifies global authentication state).
#' @return Nothing.
auth_interactive_or_from_path_to_key <- function(
    path_to_key = NULL,
    scopes = NULL,
    interactive
) {
    if(interactive){
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
    interactive
) {
    if(!googledrive::drive_has_token()){
        auth_interactive_or_from_path_to_key(
            path_to_key,
            scopes,
            interactive
        )
    }
}

#' Execute Code with Google Drive Authentication
#' 
#' @param expr Expression to evaluate with active authentication
#' @param auth_config List with components:
#'   \itemize{
#'     \item interactive Logical. Whether to use interactive auth
#'     \item path_to_key Character. Path to service account key (non-interactive only)
#'     \item scopes Character vector. OAuth scopes (non-interactive only)
#'   }
#' @param cleanup Logical. Whether to deauth after execution
#' @return Result of expr evaluation
with_gdrive_auth <- function(
    expr, 
    path_to_key,
    scopes,
    interactive 
    cleanup = FALSE
) {
    needs_auth <- !googledrive::drive_has_token()
    
    if (needs_auth) {
        auth_interactive_or_from_path_to_key(
            path_to_key = path_to_key,
            scopes = scopes,
            interactive_auth = interactive
        )
    }
    
    result <- tryCatch({
        eval(substitute(expr))
    }, finally = {
        if (cleanup) googledrive::drive_deauth()
    })
    
    result
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

#' Execute Code with Selenium Session
#' 
#' @param expr Expression to evaluate with active Selenium session
#' @param selenium_config List of Selenium configuration options
#' @param cleanup Logical. Whether to cleanup container after execution
#' @return Result of expr evaluation
with_selenium <- function(expr, selenium_config, cleanup = TRUE) {
    container <- NULL
    remDr <- NULL
    
    tryCatch({
        container <- start_selenium_container(
            port = selenium_config$port,
            image = selenium_config$container_name,
            container_engine = "podman",
            max_attempts = 5,
            delay_per_container_attempt = 3,
            local_download_dir = selenium_config$local_download_dir
        )
        
        remDr <- get_remote_driver(
            port = selenium_config$port,
            extra_capabilities = selenium_config$chrome_capabilities
        )
        
        eval(substitute(expr), environment())
        
    }, finally = {
        if (cleanup) {
            if (!is.null(remDr)) remDr$close()
            if (!is.null(container)) container$kill()
        }
    })
}

#' @keywords internal
new_external_entry <- function(entry) {
    structure(entry, class = c(paste0(entry$download_via, "_entry"), "external_entry"))
}

#' Sync External Data Entry
#' 
#' @param entry External data entry from configuration
#' @param config Global configuration options
#' @return Invisibly returns entry
sync_entry <- function(entry, config) {
    UseMethod("sync_entry")
}

#' @export
sync_entry.google_drive_api_entry <- function(entry, config) {
    with_gdrive_auth({
        if (file_is_stale(entry$id, entry$local_path)) {
            googledrive::drive_download(
                file = googledrive::as_id(entry$id),
                path = entry$local_path,
                overwrite = TRUE
            )
        }
    }, config$google_drive)
    
    invisible(entry)
}

#' @export
sync_entry.selenium_entry <- function(entry, config) {
    with_selenium({
        fullurl <- paste0(config$google_drive$url_prefix, entry$id)
        message("Navigating to: ", fullurl)
        remDr$navigate(fullurl)
        
        Sys.sleep(1)
        
        download_all <- wait_and_find_download_button(remDr)
        download_all$clickElement()
        
        Sys.sleep(config$selenium$startup_wait_time_sec)
    }, config$selenium)
    
    invisible(entry)
}

#' Sync External Data
#' 
#' @param entries List of external data entries from configuration
#' @param config Configuration list
#' @return Invisibly returns entries
#' @export
sync_external_data <- function(entries, config) {
    # Handle single entry
    if (!is.null(entries$id)) entries <- list(entries)
    
    # Convert to internal objects
    entries <- lapply(entries, new_external_entry)
    
    # Check which need updating
    to_update <- with_gdrive_auth({
        Filter(
            function(entry) file_is_stale(entry$id, entry$local_path),
            entries
        )
    }, config$google_drive)
    
    if (length(to_update) == 0) {
        message("All files up to date")
        return(invisible(entries))
    }
    
    # Split by method
    by_method <- split_luts_by_download_method(to_update)
    
    # Process each method
    for (method in names(by_method)) {
        if (length(by_method[[method]]) > 0) {
            message("Processing ", method, " downloads...")
            lapply(by_method[[method]], sync_entry, config = config)
        }
    }
    
    invisible(entries)
}