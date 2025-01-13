get_platform_for_chromedriver <- function(){
    this_os <- Sys.info()["sysname"]
    this_arch <- Sys.info()["machine"]

    if(
        # mac m-series
        (this_os == "Darwin") & 
        (this_arch == "arm64")
    ) {
        return("mac_aarch64")
    } else if(
        # mac intel
        (this_os == "Darwin") &
        (this_arch != "arm64")
    ) {
        return("mac_x64")
    } else if(
        (this_os == "Windows") &
        (this_arch == "x86-64")
    ) {
        return("windows_x64")
    } else if(
        (this_os == "Linux") &
        (this_arch == "x86_64")
    ) {
        return("linux_x64s")
    }
}