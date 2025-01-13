# Set up helper for loading constants
get_test_constants <- function() {
  RcppTOML::parseTOML("../../data/constants.toml")
}

test_that("returns single ID directly when only one exists", {
  # Mock entry with single ID (like deployments in constants)
  lut_entry <- list(
    possible_ids = "1mFr-Y5TmiqUqZFoxtfd68nqoNgtceU-xBcRmnrw6hfo",
    download_via = "api",
    local_path = "data/deployments.csv"
  )
  
  expect_equal(
    get_id_or_throw_error(lut_entry),
    "1mFr-Y5TmiqUqZFoxtfd68nqoNgtceU-xBcRmnrw6hfo"
  )
})

test_that("handles real single ID case from constants", {
  skip_if_not_installed("RcppTOML")
  constants <- get_test_constants()
  
  result <- get_id_or_throw_error(constants$externals$deployments)
  expect_true(is.character(result))
  expect_length(result, 1)
})

test_that("handles multiple IDs when determiner function exists", {
  # Create temporary platform function
  test_platform <- "mac_x64"
  assign("get_test_platform_for_chromedriver", function() test_platform, envir = .GlobalEnv)

  
  # Mock entry with multiple IDs (like chromedriver in constants)
  lut_entry <- list(
    possible_ids = list(
      windows_x64 = "win_id",
      mac_x64 = "mac_id",
      linux_x64 = "linux_id"
    ),
    download_via = "api",
    local_path = "util/chromedriver",
    id_determiner = "get_test_platform_for_chromedriver"
  )

  expected <- list("mac_id")
  names(expected)[1] <- test_platform
  
  expect_equal(
    get_id_or_throw_error(lut_entry),
    expected
  )
  
  # Clean up
  rm("get_test_platform_for_chromedriver", envir = .GlobalEnv)
})

test_that("handles real multiple ID case from constants", {
  skip_if_not_installed("RcppTOML")
  test_platform <- "mac_x64"
  constants <- get_test_constants()
  
  # Create temporary platform function that matches constants
  assign("get_test_platform_for_chromedriver", function() test_platform, envir = .GlobalEnv)

  constants$externals$chromedriver$id_determiner <- "get_test_platform_for_chromedriver"
  
  result <- get_id_or_throw_error(constants$externals$chromedriver)

  expected <- list(constants$externals$chromedriver$possible_ids$mac_x64)
  names(expected)[1] <- test_platform
  expect_length(result, 1)
  expect_equal(
    result, 
    expected
)
  
  # Clean up
  rm("get_test_platform_for_chromedriver", envir = .GlobalEnv)
})

test_that("propagates errors from get_id_from_multi_id_case_or_throw_error", {
  # Mock entry with multiple IDs but missing determiner
  lut_entry <- list(
    possible_ids = list(
      windows_x64 = "win_id",
      mac_x64 = "mac_id"
    ),
    download_via = "api",
    local_path = "util/chromedriver"
  )
  
  expect_error(
    get_id_or_throw_error(lut_entry),
    "Missing an id_determiner function"
  )
})

test_that("handles all entry types from constants", {
  skip_if_not_installed("RcppTOML")
  constants <- get_test_constants()
  
  # Test single ID entries
  expect_no_error(get_id_or_throw_error(constants$externals$deployments))
  expect_no_error(get_id_or_throw_error(constants$externals$sensors))
  expect_no_error(get_id_or_throw_error(constants$externals$dst_transitions))
  expect_no_error(get_id_or_throw_error(constants$externals$raw))
  
  # Test multiple ID entry (needs mock function)
  assign("get_platform_for_chromedriver", function() "mac_x64", envir = .GlobalEnv)
  expect_no_error(get_id_or_throw_error(constants$externals$chromedriver))
  rm("get_platform_for_chromedriver", envir = .GlobalEnv)
})