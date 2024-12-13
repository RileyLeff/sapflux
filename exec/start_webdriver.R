
get_path_to_stupid_chrome <- function() {

    sysinfo <- Sys.info()

    if(
        # mac m-series
        (sysinfo["sysname"] == "Darwin") & 
        (sysinfo["machine"] == "arm64")
    ) {
        return("util/webdrivers/chromedriver-mac-arm64/chromedriver")
    } else if(
        # mac intel
        (sysinfo["sysname"] == "Darwin") &
        (sysinfo["machine"] != "arm64")
    ) {
        return("util/webdrivers/chromedriver-mac-x64/chromedriver")
    } else if(
        (sysinfo["sysname"] == "Windows") &
        (sysinfo["machine"] == "x86-64")
    ) {
        return("util/webdrivers/chromedriver-win64/chromedriver.exe")
    } else if(
        (sysinfo["sysname"] == "Linux") &
        (sysinfo["machine"] == "x86_64")
    ) {
        return("util/webdrivers/chromedriver-linux64/chromedriver")
    }
}


get_command_to_start_chrome <- function(
    wdpath = "util/selenium-server-standalone-3.9.1.jar",
    port = 4445,
    chromepath
){
    command <- paste(
        "java ", 
        "-Dwebdriver.chrome.driver",
        chromepath,
        " -jar ",
        wdpath,
        " -port ",
        port,
        sep = ""
    )
    return(command)
}

start_selenium_webserver_for_this_platform <- function(
    wdpath = "util/selenium-server-standalone-3.9.1.jar", 
    port = 4445, 
    chromepath = get_path_to_stupid_chrome()
){
    system(
        get_command_to_start_chrome(
            wdpath,
            port,
            chromepath
        )
    )
}
# note that this is a non-default port config!
# default port 4444 is often taken on macOS

# note the java dependency
# I have legitimately no idea whether this will work on windows

# command <- 'java -Dwebdriver.chrome.driver=util/webdrivers/chromedriver-mac-arm64/chromedriver -jar util/selenium-server-standalone-3.9.1.jar -port 4445'
# system(command)
