get_id_from_multi_id_case_or_throw_error <- function(lut_entry){
    # case where we don't have any function name to determine which ID to use
    if(!("id_determiner" %in% names(lut_entry))) {
        stop("Missing an id_determiner function for an external dataset with multiple possible IDs.")
    # case where we have a name but it doesn't point to an actual function
    } else if(!existsFunction(lut_entry$id_determiner)) {
        stop("id_determiner doesn't point to a valid function in current namespace.")
    }
    # run the id_determiner function to get the name
    id_determiner_fn <- eval(parse(text = lut_entry$id_determiner))
    which_id_to_grab <- id_determiner_fn()
    
    # case where the id_determiner doesn't produce a valid name
    if(!(which_id_to_grab %in% names(lut_entry$possible_ids))) {
        stop("id_determiner function produced an invalid possible_id name.")
    }

    correct_id <- lut_entry$possible_ids[which_id_to_grab]
    return(correct_id)
}

get_id_or_throw_error <- function(lut_entry){
    if(length(lut_entry$possible_ids) == 1){
        this_id <- lut_entry$possible_ids
    } else {
        this_id <- get_id_from_multi_id_case_or_throw_error(lut_entry)
    }
    return(this_id)
}

get_last_modification_on_gdrive <- function(drive_id) {
    if(!googledrive::drive_has_token()) stop("authenticate gdrive, you heathen!")
    file_info <- googledrive::drive_get(googledrive::as_id(drive_id))
    mod_time <- file_info$drive_resource[[1]]$modifiedTime
    mod_time <- as.POSIXct(mod_time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    return(mod_time)
}

do_we_need_to_download_new_copy_of_file <- function(drive_id, local_path) {
    if(!file.exists(local_path)){
        # case where we don't have a local copy
        return(TRUE)
    }

    local_mt <- file.info(local_path)$mtime
    remote_mt <- get_last_modification_on_gdrive(drive_id)
    if(remote_mt > local_mt){
        # case where remote file is more updated than local
        return(TRUE)
    } else {
        # case where local file is at least as recent as remote
        return(FALSE)
    }
}


auth_interactive_or_from_path_to_key <- function(
    path_to_key = constants$path_to_key,
    env_key = constants$env_key
) {
    if(Sys.getenv(env_key) == "interactive"){
        googledrive::drive_auth()
        print("hi")
    } else {
                print("2")
        googledrive::drive_auth(

            path = path_to_key,
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

get_command_to_start_chromedriver <- function(
    path_to_selenium_webserver = constants$path_to_selenium_server,
    port = constants$selenium_port,
    chrome_path = constants$externals$chromedriver$local_path 
){
    args <- c(
        paste0("-Dwebdriver.chrome.driver=", chrome_path),
        "-jar",
        path_to_selenium_webserver,
        "-port",
        as.character(port)
    )
    
    return(
        list(
            command = "java",
            args = args
        )
    )
}

start_selenium_webserver <- function(
    path_to_selenium_webserver = constants$path_to_selenium_server,
    port = constants$selenium_port,
    chrome_path = constants$externals$chromedriver$local_path,
    wait_time_sec = constants$selenium_server_startup_wait_time_sec
) {
    cmd <- get_command_to_start_chromedriver(path_to_selenium_webserver, port, chrome_path)
    selenium_process <- process$new(
        command = cmd$command,
        args = cmd$args,
        stdout = "|",
        stderr = "|",
        cleanup = TRUE
    )
    
    Sys.sleep(wait_time_sec)
    
    if (!selenium_process$is_alive()) {
        stop("Failed to start Selenium server: ", 
             selenium_process$read_error(), 
             selenium_process$read_output())
    }
    
    return(selenium_process)
}


# dir_of_interest <- "data/.cache"
# unzipped_dir <- "data/sap_flux_dont_touch"
# file_of_interest <- list.files(dir_of_interest, full.names = TRUE)

# unzip(
#     zipfile = path_of_interest,
#     exdir = "data"
# )

# fs::file_move(list.files(unzipped_dir, full.names = TRUE), "data")
# fs::file_delete(unzipped_dir)

download_via_api <- function(id, local_path){
    authenticate_to_gdrive_if_necessary()
    if(do_we_need_to_download_new_copy_of_file(id, local_path)){
        googledrive::drive_download(
            file = googledrive::as_id(id),
            path = local_path
        )
    }
}

get_remote_driver <- function(
    chrome_options = constants$chrome_options,
    port = constants$selenium_port,
    save_path
){

    chrome_options$chromeOptions$prefs["download.default_directory"] <-  save_path

    RSelenium::remoteDriver(
        remoteServerAddr = "localhost",
        port = port,
        browserName = "chrome",
        extraCapabilities = chrome_options
    )
}

download_via_selenium <- function(
    lut_entry,
    path_to_selenium_server = constants$path_to_selenium_server,
    port = constants$selenium_port,
    path_to_chromedriver = constants$externals$chromedriver$local_path,
    wait_time_sec = constants$selenium_server_startup_wait_time_sec,
    url_prefix = constants$drive_url_prefix
) {
    if(!file.exists(path_to_chromedriver)){
        errmsg <- paste(
            "no chromedriver found at provided path: ",
            path_to_chromedriver,
            " make sure chromedriver is downloaded first."
        )
        stop(errmsg)
    }
    server <- start_selenium_webserver(
            path_to_selenium_webserver, 
            port, 
            path_to_chromedriver, 
            wait_time_sec
    )

    rem_dr <- get_remote_driver(
        chrome_options = constants$chrome_options,
        port = port,
        save_path = dirname(lut_entry$local_path)
    )

    rem_dr$open()
    fullurl <- paste(constants$drive_url_prefix, lut_entry$possible_ids)
    rem_dr$navigate()
    download_all <- rem_dr$findElement(
        using = "xpath",
        constants$download_button_finder
    )
    download_all$clickElement()
    server$kill()
}

sync_external_data <- function(
    lut_entry,
    path_to_selenium_server = constants$path_to_selenium_server,
    port = constants$selenium_port,
    path_to_chromedriver = constants$externals$chromedriver$local_path,
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
            path_to_selenium_server,
            port,
            path_to_chromedriver,
            wait_time_sec
        )
    }
}

sync_all_external_data <- function(
    luts = constants$externals,
    path_to_chromedriver = constants$externals$chromedriver$local_path,
    name_of_chromedriver_entry = "chromedriver"
){
    if(!file.exists(path_to_chromedriver)) print("hi")
}