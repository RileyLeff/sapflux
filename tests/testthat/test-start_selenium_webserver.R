test_that("starts server with default parameters", {
  skip_if_not_installed("RcppTOML")
  skip_if_not_installed("mockery")
  constants <- RcppTOML::parseTOML("../../data/constants.toml")
  
  mock_process_factory <- function(command, args, stdout, stderr, cleanup) {
    structure(list(
      command = command,
      args = args,
      is_alive = function() TRUE,
      read_error = function() "",
      read_output = function() ""
    ), class = "process")
  }
  
  mockery::stub(
    start_selenium_webserver, 
    'process$new', 
    mock_process_factory
  )
  
  selenium <- start_selenium_webserver()
  expect_true(selenium$is_alive())
})

test_that("uses correct command arguments", {
  skip_if_not_installed("RcppTOML")
  skip_if_not_installed("mockery")
  constants <- RcppTOML::parseTOML("../../data/constants.toml")
  
  mock_process_factory <- function(command, args, stdout, stderr, cleanup) {
    structure(list(
      command = command,
      args = args,
      is_alive = function() TRUE,
      read_error = function() "",
      read_output = function() ""
    ), class = "process")
  }
  
  mockery::stub(
    start_selenium_webserver, 
    'process$new', 
    mock_process_factory
  )
  
  selenium <- start_selenium_webserver()
  
  expected_command <- "java"
  expected_args <- c(
    paste0("-Dwebdriver.chrome.driver=", constants$externals$chromedriver$local_path),
    "-jar",
    constants$path_to_selenium_server,
    "-port",
    as.character(constants$selenium_port)
  )
  
  expect_equal(selenium$command, expected_command)
  expect_equal(selenium$args, expected_args)
})

test_that("throws error when process dies immediately", {
  skip_if_not_installed("RcppTOML")
  skip_if_not_installed("mockery")
  
  mock_dead_process <- function(...) {
    structure(list(
      is_alive = function() FALSE,
      read_error = function() "Failed to start",
      read_output = function() "Some output"
    ), class = "process")
  }
  
  mockery::stub(
    start_selenium_webserver,
    'process$new',
    mock_dead_process
  )
  
  expect_error(
    start_selenium_webserver(),
    "Failed to start Selenium server"
  )
})

test_that("handles custom wait time", {
  skip_if_not_installed("RcppTOML")
  skip_if_not_installed("mockery")
  
  mock_process_factory <- function(...) {
    structure(list(
      command = "java",
      args = character(),
      is_alive = function() TRUE,
      read_error = function() "",
      read_output = function() ""
    ), class = "process")
  }
  
  mockery::stub(
    start_selenium_webserver,
    'process$new',
    mock_process_factory
  )
  
  start_time <- Sys.time()
  custom_wait <- 0.1  # 100ms
  selenium <- start_selenium_webserver(wait_time_sec = custom_wait)
  end_time <- Sys.time()
  
  expect_true(difftime(end_time, start_time, units = "secs") >= custom_wait)
})

test_that("verifies file existence for chromedriver", {
  skip_if_not_installed("RcppTOML")
  skip_if_not_installed("mockery")
  
  mock_process_factory <- function(...) {
    structure(list(
      command = "java",
      args = character(),
      is_alive = function() TRUE,
      read_error = function() "",
      read_output = function() ""
    ), class = "process")
  }
  
  mockery::stub(
    start_selenium_webserver,
    'process$new',
    mock_process_factory
  )
  
  # Test with non-existent path
  expect_error(
    start_selenium_webserver(chrome_path = "nonexistent/path"),
    "no chromedriver found"
  )
})