library(RSelenium)

remDr <- RSelenium::remoteDriver(
    remoteServerAddr = "localhost",
    port = 4445L,
    browserName = "chrome",
)

remDr$open()
remDr$navigate("https://www.google.com")
remDr$navigate("https://drive.google.com/drive/folders/147L5_KrDVFfFyIScO1jkWNXgJeGPqDbz")
remDr$screenshot(file = "herenow.png")
url <- paste0(constants$drive_url_prefix, constants$externals$raw$id)

remDr$navigate(url)

crazier_xpath <- "/html/body/div[25]/div/div[6]/div/span[2]"