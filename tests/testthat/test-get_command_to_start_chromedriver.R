get_test_constants <- function() {
  RcppTOML::parseTOML("../../data/constants.toml")
}

test_that("returns correct structure", {
  result <- get_command_to_start_chromedriver()
  
  expect_type(result, "list")
  expect_named(result, c("command", "args"))
  expect_equal(result$command, "java")
  expect_type(result$args, "character")
})

test_that("handles default arguments from constants", {
  skip_if_not_installed("RcppTOML")
  constants <- get_test_constants()
  
  result <- get_command_to_start_chromedriver()
  
  expected_args <- c(
    paste0("-Dwebdriver.chrome.driver=", constants$externals$chromedriver$local_path),
    "-jar",
    constants$path_to_selenium_server,
    "-port",
    as.character(constants$selenium_port)
  )
  
  expect_equal(result$args, expected_args)
})

test_that("handles custom arguments", {
  custom_path <- "path/to/chromedriver"
  custom_server <- "path/to/selenium.jar"
  custom_port <- 9999
  
  result <- get_command_to_start_chromedriver(
    path_to_selenium_webserver = custom_server,
    port = custom_port,
    chrome_path = custom_path
  )
  
  expected_args <- c(
    paste0("-Dwebdriver.chrome.driver=", custom_path),
    "-jar",
    custom_server,
    "-port",
    as.character(custom_port)
  )
  
  expect_equal(result$args, expected_args)
})

test_that("port is converted to character", {
  result <- get_command_to_start_chromedriver(port = 1234)
  
  # Check that port argument is character type
  port_arg_index <- which(result$args == "-port") + 1
  expect_type(result$args[port_arg_index], "character")
})

test_that("chrome driver path is properly concatenated", {
  test_path <- "test/chrome/driver"
  result <- get_command_to_start_chromedriver(chrome_path = test_path)
  
  chrome_arg <- result$args[1]
  expect_equal(
    chrome_arg,
    paste0("-Dwebdriver.chrome.driver=", test_path)
  )
})

test_that("arguments are in correct order", {
  result <- get_command_to_start_chromedriver()
  
  # Check that arguments follow expected pattern
  expect_match(result$args[1], "^-Dwebdriver.chrome.driver=")
  expect_equal(result$args[2], "-jar")
  expect_equal(result$args[4], "-port")
})