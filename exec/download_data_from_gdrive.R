library(RSelenium)

download_dir <- paste(getwd(), "data/.cache", sep = "/")

chrome_options <- list(
  chromeOptions = list(
    prefs = list(
      "download.default_directory" = download_dir,
      "download.prompt_for_download" = FALSE,
      "download.directory_upgrade" = TRUE,
      "safebrowsing.enabled" = TRUE
    )
  )
)

remDr <- remoteDriver(
  remoteServerAddr = "localhost",
  port = 4445L,
  browserName = "chrome",
  extraCapabilities = chrome_options
)
remDr$open()
remDr$navigate("https://drive.google.com/drive/folders/1GujdVa13vjbRU-1yKvIv50JYiSipQP68")

# click download all
download_all <- remDr$findElement(
  using = "xpath",
  "/html/body/div[3]/div/div[3]/div[1]/div/div/div/div[2]/div/div[1]/div/div/div[2]/div/div[1]"
)
download_all$clickElement()

