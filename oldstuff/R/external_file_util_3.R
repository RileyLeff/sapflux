#' External File Utility Functions
#' 
#' This file contains functions for syncing external data files from various sources.
#' Primary functionality includes Google Drive API downloads, Selenium-based downloads,
#' and authentication management.
#' 
#' @dependencies processx, RSelenium, googledrive, gargle, httr
#'
#' @examples
#' # Load configuration
#' library(RcppTOML)
#' config <- parseTOML("data/constants.toml")
#' 
#' # Sync all external data using config
#' sync_external_data(config$externals, config = config)
#' 
#' # Sync single entry using config
#' sync_external_data(config$externals$deployments, config = config)
#' 
#' # Sync with explicit parameters
#' sync_external_data(
#'     config$externals,
#'     key_path = ".env/key.json",
#'     scopes = c("https://www.googleapis.com/auth/drive.readonly"),
#'     selenium_port = 4445,
#'     wait_time_sec = 5
#' )
#' 
#' # Use with_gdrive_auth directly
#' with_gdrive_auth({
#'     # Your code here
#' }, config = config)
#' 
#' # Use with_selenium directly
#' with_selenium({
#'     # Your code here
#' }, config = config)

library(processx)
library(RSelenium)

# Authentication Functions ----------------------------------------------------

#' Get Last Modification Time of Google Drive Resource
#' 
#' @param drive_id Character string. Google Drive file ID.
#' @return POSIXct timestamp in UTC timezone representing last modification time.
#' @throws Error if not authenticated to Google Drive.
get_last_modification_on_gdrive <- function(drive_id) {
    if(!googledrive::drive_has_token()) {
        stop("must authenticate to gdrive for get_last_modification_on_gdrive() to work!")
    }
    file_info <- googledrive::drive_get(googledrive::as_id(drive_id[[1]]))
    mod_time <- file_info$drive_resource[[1]]$modifiedTime
    mod_time <- as.POSIXct(mod_time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    return(mod_time)
}

#' Check if Local File Needs Updating
#' 
#' @param drive_id Character string. Google Drive file ID.
#' @param local_path Character string. Path to local file.
#' @return Logical. TRUE if file needs updating or doesn't exist locally.
file_is_stale <- function(drive_id, local_path) {
    if(!file.exists(local_path)){
        return(TRUE)
    }
    
    local_mt <- file.info(local_path)$mtime
    remote_mt <- get_last_modification_on_gdrive(drive_id)
    return(remote_mt > local_mt)
}
#' Execute Code with Google Drive Authentication
#' 
#' @param expr Expression to evaluate with active authentication
#' @param interactive_auth Logical. Whether to use interactive authentication.
#' @param key_path Character. Path to service account key file.
#' @param scopes Character vector. OAuth scopes.
#' @param cleanup Logical. Whether to deauth after execution.
#' @param config List. Optional configuration structure.
#' @return Result of expr evaluation
with_gdrive_auth <- function(
    expr,
    interactive_auth = TRUE,
    key_path = NA,
    scopes = NA,
    cleanup = FALSE,
    config = NA
) {
    needs_auth <- !googledrive::drive_has_token()
    
    if (needs_auth) {
        if (interactive_auth) {
            googledrive::drive_auth()
        } else {
            credentials <- gargle::credentials_service_account(
                path = key_path,
                scopes = scopes
            )
            googledrive::drive_auth(
                token = credentials,
                path = key_path,
                cache = FALSE
            )
        }
    }
    
    # Capture the parent environment
    parent_env <- parent.frame()
    
    result <- tryCatch({
        eval(substitute(expr), envir = parent_env)  # Evaluate in parent environment
    }, finally = {
        if (cleanup) googledrive::drive_deauth()
    })
    
    result
}

# Selenium Management Functions ---------------------------------------------

#' Find Download Button in Google Drive Interface
#' 
#' @param remDr RSelenium remote driver object
#' @param using Character. Type of selector (e.g., "xpath", "css selector").
#' @param value Character. The selector value.
#' @param max_attempts Integer. Maximum number of attempts to find button.
#' @param delay_sec Numeric. Seconds to wait between attempts.
#' @param config List. Optional configuration structure.
#' @return webElement object representing the download button
#' @keywords internal
wait_and_find_download_button <- function(
    remDr,
    using = "xpath",
    value = "//div[@role='button'][contains(text(), 'Download all')]",
    max_attempts = 10,
    delay_sec = 0.5,
    config = NA
) {
    if (!is.na(config)) {
        using <- config$selenium$selector$using
        value <- config$selenium$selector$value
    }
    
    for (attempt in 1:max_attempts) {
        tryCatch({
            elem <- remDr$findElement(using = using, value = value)
            if (!is.null(elem)) return(elem)
        }, error = function(e) {
            message("Download button not found, attempt ", attempt, "/", max_attempts)
            Sys.sleep(delay_sec)
        })
    }
    
    stop("Failed to find download button after ", max_attempts, " attempts")
}

#' Start Selenium Container
#'
#' @param port Integer. Port to expose Selenium on host.
#' @param image Character. Docker/Podman image name.
#' @param container_engine Character. Container runtime to use.
#' @param max_attempts Integer. Maximum number of connection attempts.
#' @param delay_sec Numeric. Seconds to wait between attempts.
#' @param download_dir Character. Local directory to mount for downloads.
#' @param dns_servers Character vector. DNS servers to use.
#' @param network_mode Character. Network mode ("bridge" or "host").
#' @param config List. Optional configuration structure.
#' @return processx object representing the container process.
start_selenium_container <- function(
    port = 4445,
    image = "rileyleff/riley-selenium-3.x",
    container_engine = "podman",
    max_attempts = 5,
    delay_sec = 3,
    download_dir = "data",
    container_download_dir = "Downloads",
    dns_servers = c("8.8.8.8", "8.8.4.4"),
    network_mode = "bridge",
    config = NA
) {
    # Handle configuration
    if (!is.na(config)) {
        port <- config$selenium$port
        image <- config$selenium$container_name
        download_dir <- config$selenium$local_download_dir
        delay_sec <- config$selenium$startup_wait_time_sec
        
        if (!is.null(config$network[[config$network$current]])) {
            network_config <- config$network[[config$network$current]]
            dns_servers <- network_config$dns_servers
            network_mode <- network_config$network_mode
        }
    }
    
    # Prepare download directory
    dir.create(download_dir, recursive = TRUE, showWarnings = FALSE)
    abs_download_dir <- normalizePath(download_dir)
    
    # Build container arguments
    args <- c(
        "run",
        "--rm",
        "-d",
        "-p", sprintf("%d:4444", port),
        "--shm-size", "2g"
    )
    
    # Add DNS servers
    for (dns in dns_servers) {
        args <- c(args, "--dns", dns)
    }
    
    # Add remaining arguments
    args <- c(
        args,
        "--network", network_mode,
        "-v", sprintf("%s:/%s", abs_download_dir, container_download_dir),
        image
    )
    
    # Rest of function remains the same...
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
        Sys.sleep(delay_sec)
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
        stop(sprintf(
            "Failed to start Selenium container after %d attempts:\nSTDERR: %s\nSTDOUT: %s",
            max_attempts, error_out, stdout_out
        ))
    }
    
    message("Selenium container started successfully")
    return(container_process)
}

#' Create Remote WebDriver Connection
#' 
#' @param port Integer. Port number for Selenium server.
#' @param server_addr Character. Selenium server address.
#' @param browser Character. Browser to use.
#' @param capabilities List. Browser-specific capabilities.
#' @param container_download_dir Character. Where the container downloads stuff to.
#' @param config List. Optional configuration structure.
#' @return RSelenium remote driver object.
get_remote_driver <- function(
    port = 4445,
    server_addr = "localhost",
    browser = "chrome",
    capabilities = NULL,
    container_download_dir = "Downloads",  # Added this parameter
    config = NA
) {
    if (!is.na(config)) {
        port <- config$selenium$port
        capabilities <- config$selenium$chrome_capabilities
    }
    
    # Always set download preferences
    base_capabilities <- list(
        chromeOptions = list(
            prefs = list(
                "download.default_directory" = sprintf("/%s", container_download_dir),
                "download.prompt_for_download" = FALSE,
                "download.directory_upgrade" = TRUE,
                "safebrowsing.enabled" = TRUE
            )
        )
    )
    
    # Merge with provided capabilities if any
    if (!is.null(capabilities)) {
        capabilities$chromeOptions$prefs <- modifyList(
            base_capabilities$chromeOptions$prefs,
            capabilities$chromeOptions$prefs
        )
    } else {
        capabilities <- base_capabilities
    }
    
    remDr <- RSelenium::remoteDriver(
        remoteServerAddr = server_addr,
        port = port,
        browserName = browser,
        extraCapabilities = capabilities
    )
    
    tryCatch({
        remDr$open(silent = TRUE)
        return(remDr)
    }, error = function(e) {
        message("Failed to connect to Selenium server: ", e$message)
        stop(e)
    })
}

#' Execute Code with Selenium Session
#' 
#' @param expr Expression to evaluate with active Selenium session.
#' @param port Integer. Port for Selenium server.
#' @param image Character. Selenium container image.
#' @param download_dir Character. Directory for downloads.
#' @param capabilities List. Browser capabilities.
#' @param cleanup Logical. Whether to cleanup container after execution.
#' @param config List. Optional configuration structure.
#' @return Result of expr evaluation.
with_selenium <- function(
    expr,
    port = 4445,
    image = "rileyleff/riley-selenium-3.x",
    download_dir = "data",
    capabilities = NULL,
    cleanup = TRUE,
    dns_servers = c("8.8.8.8", "8.8.4.4"),
    network_mode = "bridge",
    container_download_dir = "Downloads",
    config = NA
) {
    if (!is.na(config)) {
        port <- config$selenium$port
        image <- config$selenium$container_name
        download_dir <- config$selenium$local_download_dir
        capabilities <- config$selenium$chrome_capabilities
    }
    
    container <- NULL
    remDr <- NULL
    
    tryCatch({
        container <- start_selenium_container(
            port = port,
            image = image,
            download_dir = download_dir,
            dns_servers = dns_servers,
            network_mode = network_mode,
            container_download_dir = container_download_dir,
            config = config
        )
        
        remDr <- get_remote_driver(
            port = port,
            capabilities = capabilities
        )
        
        # Create new environment with access to both parent vars and our remDr
        parent_env <- parent.frame()
        eval_env <- new.env(parent = parent_env)
        eval_env$remDr <- remDr
        
        eval(substitute(expr), envir = eval_env)
        
    }, finally = {
        if (cleanup) {
            if (!is.null(remDr)) remDr$close()
            if (!is.null(container)) container$kill()
        }
    })
}

# Core Sync Functions -----------------------------------------------------

#' Internal constructor for external entries
#' @keywords internal
new_external_entry <- function(entry) {
    structure(entry, class = c(paste0(entry$download_via, "_entry"), "external_entry"))
}

#' Split Entries by Download Method
#' 
#' @param entries List of entries, each containing download_via field
#' @param valid_methods Character vector of allowed download methods
#' @return List of lists, grouped by download_via value
#' @keywords internal
split_by_method <- function(
    entries,
    valid_methods = c("selenium", "google_drive_api")
) {
    # Handle single entry case
    if (!is.null(entries$download_via)) entries <- list(entries)
    
    # Validate input structure
    if (!all(sapply(entries, function(x) "download_via" %in% names(x)))) {
        stop("All entries must contain 'download_via' field")
    }
    
    # Get actual methods present in data
    methods_present <- unique(sapply(entries, `[[`, "download_via"))
    
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
    result <- split(entries, sapply(entries, `[[`, "download_via"))
    
    # Ensure all valid methods have an entry (possibly empty)
    result <- structure(
        lapply(setNames(valid_methods, valid_methods), function(method) {
            result[[method]] %||% list()
        }),
        class = "download_method_split"
    )
    
    return(result)
}

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

#' Download Entry Using Google Drive API
#' 
#' @param entry External entry object
#' @param key_path Character. Path to service account key.
#' @param scopes Character vector. OAuth scopes.
#' @param config List. Optional configuration structure.
#' @return Invisibly returns entry
download_via_api <- function(
    entry,
    key_path = NA,
    scopes = NA,
    config = NA
) {
    with_gdrive_auth({
        if (file_is_stale(entry$id, entry$local_path)) {
            googledrive::drive_download(
                file = googledrive::as_id(entry$id),
                path = entry$local_path,
                overwrite = TRUE
            )
        }
    }, 
    interactive_auth = FALSE,
    key_path = key_path,
    scopes = scopes,
    config = config)
    
    invisible(entry)
}

#' Wait for Download to Start and Complete
#' 
#' @param download_dir Character. Directory to watch for downloads.
#' @param max_start_time Integer. Maximum seconds to wait for download to start.
#' @param max_completion_time Integer. Maximum seconds to wait for download to complete.
#' @param check_interval Numeric. Seconds between checks.
#' @return Character. Path to downloaded file or NULL if failed.
wait_for_download <- function(
    download_dir,
    max_start_time = 30,
    max_completion_time = 300,
    check_interval = 0.5
) {
    start_time <- Sys.time()
    
    # Wait for download to start
    message("Waiting for download to start...")
    while (difftime(Sys.time(), start_time, units="secs") < max_start_time) {
        # Look for temporary files
        temp_files <- list.files(
            download_dir, 
            pattern = "\\.(crdownload|tmp|part)$",
            full.names = TRUE
        )
        
        if (length(temp_files) > 0) {
            message("Download started")
            break
        }
        
        Sys.sleep(check_interval)
    }
    
    if (difftime(Sys.time(), start_time, units="secs") >= max_start_time) {
        stop("Download did not start within ", max_start_time, " seconds")
    }
    
    # Wait for download to complete
    message("Waiting for download to complete...")
    completion_start <- Sys.time()
    
    while (difftime(Sys.time(), completion_start, units="secs") < max_completion_time) {
        # Check for temporary files
        temp_files <- list.files(
            download_dir, 
            pattern = "\\.(crdownload|tmp|part)$",
            full.names = TRUE
        )
        
        # Check for completed files (might need to adjust pattern)
        completed_files <- list.files(
            download_dir, 
            pattern = "\\.zip$",  # or whatever extension you expect
            full.names = TRUE
        )
        
        if (length(temp_files) == 0 && length(completed_files) > 0) {
            message("Download completed")
            return(completed_files[1])  # Return path to downloaded file
        }
        
        Sys.sleep(check_interval)
    }
    
    stop("Download did not complete within ", max_completion_time, " seconds")
}

#' Download Entry Using Selenium
#' 
#' @param entry External entry object
#' @param port Integer. Selenium port.
#' @param url_prefix Character. URL prefix for Google Drive.
#' @param wait_time_sec Integer. Seconds to wait for download.
#' @param capabilities List. Browser capabilities.
#' @param config List. Optional configuration structure.
#' @return Invisibly returns entry
download_via_selenium <- function(
    entry,
    port = 4445,
    url_prefix = "https://drive.google.com/drive/folders/",
    max_start_time = 30,
    max_completion_time = 300,
    capabilities = NULL,
    network_mode = "bridge",
    dns_servers = c("8.8.8.8", "8.8.4.4"),
    container_download_dir = "Downloads",
    config = NA
) {
    if (!is.na(config)) {
        port <- config$selenium$port
        url_prefix <- config$google_drive$url_prefix
        capabilities <- config$selenium$chrome_capabilities
    }
    
    with_selenium({
        fullurl <- paste0(url_prefix, entry$id)
        message("Navigating to: ", fullurl)
        remDr$navigate(fullurl)
        
        Sys.sleep(1)  # Brief pause for page load
        
        download_all <- wait_and_find_download_button(remDr, config = config)
        message("Found download button, clicking...")
        download_all$clickElement()
        
        # Wait for download with timeout
        downloaded_file <- wait_for_download(
            download_dir = entry$local_path,
            max_start_time = max_start_time,
            max_completion_time = max_completion_time
        )
        
        message("Download successful: ", downloaded_file)
    }, 
    port = port,
    capabilities = capabilities,
    network_mode = network_mode,
    dns_servers = dns_servers,
    container_download_dir = container_download_dir,
    config = config
    )
    
    invisible(entry)
}

#' Sync External Data Entries
#' 
#' @param entries List of external data entries
#' @param key_path Character. Path to service account key.
#' @param scopes Character vector. OAuth scopes.
#' @param selenium_port Integer. Port for Selenium server.
#' @param wait_time_sec Integer. Seconds to wait for downloads.
#' @param capabilities List. Browser capabilities.
#' @param config List. Optional configuration structure.
#' @return Invisibly returns entries
#' @export
sync_external_data <- function(
    entries,
    key_path = NA,
    scopes = NA,
    selenium_port = 4445,
    max_start_time = 30,
    max_completion_time = 300,
    capabilities = NULL,
    dns_servers = c("8.8.8.8", "8.8.4.4"),
    network_mode = "bridge",
    container_download_dir = "Downloads",
    config = NA
) {
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
    }, 
    interactive_auth = FALSE,
    key_path = key_path,
    scopes = scopes,
    config = config)
    
    if (length(to_update) == 0) {
        message("All files up to date")
        return(invisible(entries))
    }
    
    # Split by method
    by_method <- split_by_method(to_update)
    
    # Process API downloads
    if (length(by_method$google_drive_api) > 0) {
        message("Processing API downloads...")
        lapply(
            by_method$google_drive_api,
            download_via_api,
            key_path = key_path,
            scopes = scopes,
            config = config
        )
    }
    
    # Process Selenium downloads
    if (length(by_method$selenium) > 0) {
        message("Processing Selenium downloads...")
        lapply(
            by_method$selenium,
            download_via_selenium,
            port = selenium_port,
            max_start_time = max_start_time,
            max_completion_time = max_completion_time,
            capabilities = capabilities,
            network_mode = network_mode,
            dns_servers = dns_servers,
            container_download_dir = container_download_dir,
            config = config
        )
    }
    
    invisible(entries)
}