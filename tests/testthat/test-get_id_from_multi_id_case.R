test_that("errors when missing id_determiner", {
  # Mock entry without id_determiner
  lut_entry <- list(
    possible_ids = list(
      windows_x64 = "id1",
      mac_x64 = "id2"
    ),
    download_via = "api",
    local_path = "path/to/file"
  )
  
  expect_error(
    get_id_from_multi_id_case_or_throw_error(lut_entry),
    "Missing an id_determiner function"
  )
})

test_that("errors when id_determiner function doesn't exist", {
  # Mock entry with non-existent function
  lut_entry <- list(
    possible_ids = list(
      windows_x64 = "id1",
      mac_x64 = "id2"
    ),
    download_via = "api",
    local_path = "path/to/file",
    id_determiner = "not_a_real_function"
  )

  expect_error(
    get_id_from_multi_id_case_or_throw_error(lut_entry),
    "doesn't point to a valid function"
  )
})

test_that("errors when id_determiner returns invalid platform", {
  # Create temporary function that returns invalid platform
  assign("bad_platform_fn", function() "invalid_platform", envir = .GlobalEnv)

  lut_entry <- list(
    possible_ids = list(
      windows_x64 = "id1",
      mac_x64 = "id2"
    ),
    download_via = "api",
    local_path = "path/to/file",
    id_determiner = "bad_platform_fn"
  )

  expect_error(
    get_id_from_multi_id_case_or_throw_error(lut_entry),
    "produced an invalid possible_id name"
  )

  # Clean up
  rm("bad_platform_fn", envir = .GlobalEnv)
})

test_that("returns correct ID for valid input", {
  # Create temporary function that returns valid platform
  assign("good_platform_fn", function() "mac_x64", envir = .GlobalEnv)

  lut_entry <- list(
    possible_ids = list(
      windows_x64 = "id1",
      mac_x64 = "id2"
    ),
    download_via = "api",
    local_path = "path/to/file",
    id_determiner = "good_platform_fn"
  )

  expect_equal(
    get_id_from_multi_id_case_or_throw_error(lut_entry),
    list("mac_x64" = "id2")
  )

  # Clean up
  rm("good_platform_fn", envir = .GlobalEnv)
})

test_that("handles real example from constants", {
  skip_if_not_installed("RcppTOML")
  test_platform = "mac_x64"
  constants <- RcppTOML::parseTOML("../../data/constants.toml")

  # Create temporary platform function that matches one in constants
  assign("get_test_platform_for_chromedriver", function() test_platform, envir = .GlobalEnv)

  constants$externals$chromedriver$id_determiner <- "get_test_platform_for_chromedriver"

  result <- get_id_from_multi_id_case_or_throw_error(constants$externals$chromedriver)

  expect <- list("1W_6EeRWN26gvUCl1lROVrFpEkeBo6xAl")
  names(expect)[1] <- test_platform

  expect_equal(
    result,
    expect
  )
  
  # Clean up
  rm("get_test_platform_for_chromedriver", envir = .GlobalEnv)
})
